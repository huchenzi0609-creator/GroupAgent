"""Test the checked pre-ReAct sources and the rollback bridge against PostgreSQL."""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DeliveryState, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore
from groupagent.scheduling import AtTiming, EveryTiming, PreparedMessage, Scheduler

ROOT = Path(__file__).resolve().parents[2]
BASE = "6dcac4ab3d5cfd9c415bc9b071a4c62b5140eb0d"
HELPER = ROOT / "deploy/prepare-pot-react-rollback.py"


@pytest.fixture
def previous_package(tmp_path):
    # The source of truth is the reviewed local commit, independent of the
    # candidate checkout and using only public package files.
    archived = subprocess.run(
        ["git", "archive", BASE, "src/groupagent"], cwd=ROOT, capture_output=True, check=False
    )
    if archived.returncode:
        pytest.skip("The review base commit is absent from this checkout")
    with tarfile.open(fileobj=io.BytesIO(archived.stdout)) as tar:
        tar.extractall(tmp_path / "base", filter="data")
    return tmp_path / "base/src/groupagent"


def bridge(previous, target):
    return subprocess.run(
        [sys.executable, str(HELPER), str(previous), str(target)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_bridge_refuses_unreviewed_source_and_never_edits_base(previous_package, tmp_path):
    untouched = previous_package / "runtime/app.py"
    original = untouched.read_bytes()
    overlay = tmp_path / "overlay"
    result = bridge(previous_package, overlay)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["base_commit"] == BASE
    assert set(json.loads(result.stdout)["overlay"]) >= {
        "scheduling/service.py",
        "runtime/dispatch.py",
        "persistence/postgres.py",
    }
    assert untouched.read_bytes() == original
    untouched.write_bytes(original + b"\n# hypothetical drift\n")
    rejected = tmp_path / "rejected"
    result = bridge(previous_package, rejected)
    assert result.returncode != 0 and "reviewed Pot rollback base" in result.stderr
    assert not rejected.exists()


@pytest.mark.postgres
def test_reviewed_old_runtime_reads_and_sends_only_legacy_work_on_schema_19(
    previous_package, tmp_path
):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    overlay = tmp_path / "overlay"
    result = bridge(previous_package, overlay)
    assert result.returncode == 0, result.stderr
    shutil.copytree(overlay / "groupagent", previous_package, dirs_exist_ok=True)

    store = PostgresStore(dsn)
    assert store.migrate() >= 19
    group = "rollback-bridge-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    scheduler = Scheduler(store)
    now = datetime.now(UTC)
    at = now + timedelta(hours=1)
    identifiers = {}
    source = GroupEvent(
        id=uuid4().hex,
        source_key="rollback-public-fixture:" + uuid4().hex,
        group_id=group,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="到时提醒我核对合成数据",
        occurred_at=now,
        received_at=now,
        trusted_actor_origin="verified_channel_metadata",
    )
    erased_source = source.model_copy(
        update={"id": uuid4().hex, "source_key": "rollback-erasure:" + uuid4().hex}
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(source, source.id, "1", 0)
        tx.ingest_event(erased_source, erased_source.id, "1", 0)
        legacy_message = tx.enqueue_reply(source.id, "Legacy synthetic reply")
    message = PreparedMessage(text="核对合成数据", purpose="explicit_reminder")
    for kind in ("pending", "unknown", "accepted", "dispatching"):
        rule = scheduler.create(
            actor,
            kind,
            None,
            AtTiming(at=at),
            now=now,
            authorization_event_id=source.id,
            prepared_message=message,
        )
        identifiers[kind] = rule.id
    erasable = scheduler.create(
        actor,
        "Authorized erasure while rolled back",
        None,
        AtTiming(at=at),
        now=now,
        authorization_event_id=erased_source.id,
        prepared_message=message,
    )
    recurring = scheduler.create(
        actor,
        "Converted with direct history",
        None,
        EveryTiming(interval_seconds=3600),
        now=now,
        authorization_event_id=source.id,
        prepared_message=message,
    )
    silent = scheduler.create(
        actor,
        "Conditional future goal",
        "Report only if the synthetic source changed",
        EveryTiming(interval_seconds=3600),
        now=now,
        authorization_event_id=source.id,
        allow_silent_result=True,
    )
    scheduler.wake(actor, at)
    silent_work = scheduler.occurrences(actor, silent.id)[0].work_id
    erased_delivery = scheduler.occurrences(actor, erasable.id)[0].delivery_id
    deliveries = {
        kind: scheduler.occurrences(actor, rule_id)[0].delivery_id
        for kind, rule_id in identifiers.items()
    }
    active = scheduler.get(actor, recurring.id)
    scheduler.update(
        actor,
        active.id,
        expected_revision=active.revision,
        goal="Calculate after reauthorization",
        now=at,
    )
    sealed = scheduler.create(
        actor,
        "Still active prepared rule",
        None,
        AtTiming(at=at + timedelta(hours=1)),
        now=now,
        authorization_event_id=source.id,
        prepared_message=message,
    )
    legacy = scheduler.create(
        actor, "Old goal", "Compute a synthetic sum", AtTiming(at=at + timedelta(hours=1)), now=now
    )
    with store.transaction(actor) as tx:
        observer = tx.enqueue_delivery(
            None, "text:Sealed observation", "f" * 64, "react-observer:synthetic"
        )
        for kind in ("unknown", "accepted", "dispatching"):
            intent = tx.claim_delivery("candidate-sender", intent_id=deliveries[kind])
            tx.conn.execute(
                "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,"
                "payload_hash,state,platform_message_id,platform_job_id,rendered_text) "
                "VALUES (%s,%s,0,%s,%s,%s,%s,%s,%s)",
                (
                    group,
                    intent.id,
                    "candidate-native:" + intent.id,
                    "a" * 64,
                    "accepted" if kind == "accepted" else "submitted",
                    "fixture-native-one" if kind == "accepted" else None,
                    "fixture-native-job:" + intent.id,
                    "Synthetic frozen wire text",
                ),
            )
            if kind != "dispatching":
                tx.finish_delivery(
                    intent.id,
                    intent.claim_token,
                    DeliveryState(kind),
                    platform_message_id="fixture-native-one" if kind == "accepted" else None,
                )
            else:
                tx.conn.execute(
                    "UPDATE ga_deliveries SET claim_expires_at=clock_timestamp()-interval '1 second' "
                    "WHERE group_id=%s AND id=%s",
                    (group, intent.id),
                )
        protected = {
            *deliveries.values(),
            observer.id,
            scheduler.occurrences(actor, recurring.id)[0].delivery_id,
        }
        before = {row.id: row for row in tx.list_delivery() if row.id in protected}
        parts_before = tx.conn.execute(
            "SELECT * FROM ga_delivery_parts WHERE group_id=%s ORDER BY intent_id,position",
            (group,),
        ).fetchall()

    environment = {
        **os.environ,
        "ROLLBACK_TEST_GROUP": group,
        "ROLLBACK_TEST_IDS": json.dumps(
            {
                "protected_pending": deliveries["pending"],
                "legacy_rule": legacy.id,
                "sealed_rule": sealed.id,
                "converted_rule": recurring.id,
                "unknown_rule": identifiers["unknown"],
                "legacy_message": legacy_message.id,
                "erased_source": erased_source.id,
                "silent_rule": silent.id,
                "silent_work": silent_work,
                "now": (at + timedelta(hours=1)).isoformat(),
            }
        ),
    }
    ran = subprocess.run(
        [sys.executable, "-I", "-c", OLD_RUNTIME_TEST, str(previous_package.parent), str(tmp_path)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=25,
        check=False,
    )
    assert ran.returncode == 0, ran.stderr
    with store.transaction(actor) as tx:
        assert {row.id: row for row in tx.list_delivery() if row.id in protected} == before
        assert (
            tx.conn.execute(
                "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=ANY(%s) "
                "ORDER BY intent_id,position",
                (group, list(protected)),
            ).fetchall()
            == parts_before
        )
        assert tx.get_delivery(legacy_message.id).state == DeliveryState.ACCEPTED
        removed = tx.get_delivery(erased_delivery)
        assert removed.state == DeliveryState.REJECTED
        assert (
            removed.payload_ref == "text:[content removed]" and removed.reason == "source_forgotten"
        )
        assert (
            tx.conn.execute("SELECT max(version) AS version FROM ga_schema_migrations").fetchone()[
                "version"
            ]
            >= 19
        )
        assert len(tx.list_work()) == 2
    assert len(scheduler.occurrences(actor, recurring.id)) == 1
    assert len(scheduler.occurrences(actor, silent.id)) == 1
    assert scheduler.occurrences(actor, sealed.id) == []


OLD_RUNTIME_TEST = r"""
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from groupagent.artifacts import ArtifactStore
from groupagent.channels import AgentWechatConfig
from groupagent.channels.local import LocalChannel
from groupagent.config import AppConfig
from groupagent.domain import ActorContext, DeliveryState, DomainError, Role, WorkState
from groupagent.memory import MemoryService
from groupagent.persistence import PostgresStore
from groupagent.runtime.artifacts import ArtifactRegistry
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.notices import NoticeService
from groupagent.runtime.state import RuntimeState
from groupagent.scheduling import Scheduler

root = Path(sys.argv[2])
group = os.environ['ROLLBACK_TEST_GROUP']
ids = json.loads(os.environ['ROLLBACK_TEST_IDS'])
config = AppConfig.model_validate({'group': {
    'enabled': True, 'internal_id': group,
    'external_group_id': 'synthetic-test@chatroom', 'owner_ids': ['synthetic-owner'],
}})
assert AppConfig.model_validate_json(config.model_dump_json()) == config
actor = ActorContext(group_id=group, member_id=config.group.owner_ids[0], role=Role.OWNER)
technical = ActorContext(group_id=group, member_id='service:rollback-fixture', role=Role.MAINTAINER)
store = PostgresStore(os.environ['GROUPAGENT_TEST_DSN'])
assert store.migrate() == 16
scheduler = Scheduler(store)
assert len(scheduler.list(actor)) == 9
assert scheduler.get(actor, ids['sealed_rule']).goal == '核对合成数据'
assert scheduler.occurrences(actor, ids['unknown_rule'])[0].work_id is None
for rule_id in (ids['sealed_rule'], ids['converted_rule'], ids['silent_rule']):
    rule = scheduler.get(actor, rule_id)
    for operation in ('update', 'pause', 'resume', 'cancel'):
        try:
            getattr(scheduler, operation)(actor, rule_id, expected_revision=rule.revision)
        except DomainError as error:
            assert error.code == 'feature_disabled'
        else:
            raise AssertionError('A previous controller edited a sealed schedule')
woken = scheduler.due(actor, datetime.fromisoformat(ids['now']))
assert len(woken) == 1 and woken[0].schedule_id == ids['legacy_rule']
with store.transaction(actor) as tx:
    assert tx.claim_delivery('old-direct', intent_id=ids['protected_pending']) is None
    assert tx.recover_expired_deliveries() == []
try:
    with store.transaction(actor) as tx:
        tx.claim_work(ids['silent_work'], 'old-worker')
except DomainError as error:
    assert error.code == 'feature_disabled'
else:
    raise AssertionError('A previous controller claimed a conditional goal')
with store.transaction(actor) as tx:
    conditional = tx.get_work(ids['silent_work'])
    tx.transition_work(conditional.id, WorkState.PAUSED, expected_revision=conditional.revision,
                       reason='legacy_release_rollback')
assert NoticeService(store, technical, lambda _: actor).tick() == 0
channel = LocalChannel(root / 'previous-channel', AgentWechatConfig(
    base_url='http://127.0.0.1', account_id='local', internal_group_id=group,
    external_group_id=config.group.external_group_id, source_epoch='rollback-fixture',
))
artifacts = ArtifactStore(root / 'previous-artifacts')
try:
    sender = Dispatcher(RuntimeState(store), technical, lambda _: actor, channel, ArtifactRegistry(store, artifacts))
    assert asyncio.run(sender.tick()) == 1
finally:
    artifacts.close()
MemoryService(store).forget(actor, event_ids=[ids['erased_source']])
"""
