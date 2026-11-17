"""Actual host proof SQL against synthetic PostgreSQL; only Docker absence is observed offline."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactScope, DocumentService, Report, Section
from groupagent.config import AppConfig
from groupagent.domain import WorkState
from groupagent.runtime.effect_reconciler import UnpublishedDraftReconciler
from groupagent.runtime.recovery import checkpoint_digest
from groupagent.sandbox import DockerSandbox
from groupagent.tools import EffectState

pytestmark = pytest.mark.postgres


def fixture_module(name):
    """Reuse the execution acceptance fixture without changing test package layout."""
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).parents[1] / "execution" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def host_case(runtime_env, monkeypatch):
    fixture_module("test_supervisor")
    fixtures = fixture_module("test_durable_recovery_acceptance")
    env = SimpleNamespace(**vars(runtime_env), control=runtime_env.service)
    work, checkpoint = fixtures.legacy_unknown_work(env)
    config = AppConfig.model_validate(
        {
            "group": {
                "internal_id": env.actor.group_id,
                "external_group_id": "synthetic-host-proof",
                "owner_ids": [env.actor.member_id],
                "policy_revision": env.actor.policy_revision,
            },
            "runtime": {"worker_image": "sha256:" + "a" * 64},
        }
    )
    absence_checks = []

    async def observe_absence(group_id, work_id):
        absence_checks.append((group_id, work_id))

    monkeypatch.setattr(DockerSandbox, "quiesce_work", staticmethod(observe_absence))
    return SimpleNamespace(
        env=env,
        fixtures=fixtures,
        work=work,
        checkpoint=checkpoint,
        boundary=datetime.now(UTC) - timedelta(seconds=60),
        reconcile=UnpublishedDraftReconciler(config, env.store),
        absence_checks=absence_checks,
    )


def current(case):
    with case.env.store.transaction(case.env.actor) as tx:
        return tx.get_work(case.work.id)


def request_observation(case, *, name=None, key=None, revision=None):
    receipt = case.checkpoint.receipts[0]
    body = {
        "payload": {
            "type": "tool_requested",
            "call_id": receipt.call_id,
            "name": name or receipt.tool_name,
            "revision": revision or case.work.goal_revision,
            "idempotency_key": key or receipt.idempotency_key,
        }
    }
    with case.env.store.transaction(case.env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body,created_at) "
            "VALUES(%s,%s,%s,1,'tool_requested',%s,%s)",
            (
                tx.group_id,
                case.work.id,
                case.checkpoint.request.attempt_id,
                Jsonb(body),
                case.boundary,
            ),
        )


def stopped_observation(case):
    with case.env.store.transaction(case.env.actor) as tx:
        tx._audit(
            "execution_stopped",
            case.work.id,
            attempt_id=case.checkpoint.request.attempt_id,
            worker_id="synthetic-stopped-controller",
            evidence="synthetic_scoped_worker_absence_and_joined_stage",
        )


def artifact_history(case, when, *, available=True):
    env = case.env
    scope = ArtifactScope(env.actor.group_id, case.work.id, env.actor.member_id)
    documents = DocumentService(env.artifacts)
    version = documents.generate(
        scope,
        "earlier-valid.md",
        Report("Synthetic evidence", (Section("Result", ("Verified synthetic prior artifact",)),)),
    )
    validation = documents.validate(
        scope,
        version.file_id,
        version.version_id,
        expected_text=("Verified synthetic prior artifact",),
    )
    assert validation.passed
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,available,created_at) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                tx.group_id,
                version.version_id,
                version.file_id,
                case.work.id,
                env.actor.member_id,
                version.name,
                version.format,
                version.sha256,
                version.size_bytes,
                Jsonb(version.to_dict()),
                Jsonb(validation.to_dict()),
                available,
                when,
            ),
        )
        tx.conn.execute(
            "INSERT INTO ga_work_outputs(group_id,work_id,goal_revision,version_id,validation,purpose) VALUES(%s,%s,%s,%s,%s,'candidate')",
            (
                tx.group_id,
                case.work.id,
                case.work.goal_revision,
                version.version_id,
                Jsonb(validation.to_dict()),
            ),
        )
    return version


def native_send_history(case, *, old_intent=False):
    sent_at = case.boundary + timedelta(seconds=5)
    created_at = case.boundary - timedelta(seconds=5) if old_intent else sent_at
    identifier = "synthetic-host-send:" + uuid4().hex
    with case.env.store.transaction(case.env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_deliveries(id,group_id,work_id,goal_revision,created_by,policy_revision,payload_ref,payload_hash,idempotency_key,state,platform_message_id,accepted_at,created_at,updated_at) "
            "VALUES(%s,%s,%s,%s,%s,%s,'text:Synthetic native receipt',%s,%s,'accepted','synthetic-native-message',%s,%s,%s)",
            (
                identifier,
                tx.group_id,
                case.work.id,
                case.work.goal_revision,
                case.env.actor.member_id,
                case.env.actor.policy_revision,
                hashlib.sha256(identifier.encode()).hexdigest(),
                identifier,
                sent_at,
                created_at,
                sent_at,
            ),
        )


def assert_original_untouched(case):
    assert case.env.state.load_checkpoint(case.env.actor, case.work.id) == case.checkpoint
    with case.env.store.transaction(case.env.actor) as tx:
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='work_effect_reconciled'",
            (tx.group_id, case.work.id),
        ).fetchone()


async def test_real_host_proof_accepts_earlier_valid_artifact_without_mutating_checkpoint(
    host_case,
):
    case = host_case
    earlier = artifact_history(case, case.boundary - timedelta(seconds=10))
    request_observation(case)
    stopped_observation(case)
    before = current(case)
    derived = await case.reconcile(before, case.checkpoint, case.env.actor)
    assert derived is not None and derived.receipts[0].effect_state == EffectState.NO_EFFECT
    assert (
        derived.usage == case.checkpoint.usage and derived.receipts[0].error_code == "worker_failed"
    )
    assert current(case) == before
    assert_original_untouched(case)
    assert case.absence_checks == [(case.env.actor.group_id, case.work.id)]
    with case.env.store.transaction(case.env.actor) as tx:
        assert tx.conn.execute(
            "SELECT available FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (tx.group_id, earlier.version_id),
        ).fetchone()["available"]


@pytest.mark.parametrize("missing", ["request", "stop"])
async def test_real_host_proof_rejects_missing_request_or_controller_stop(host_case, missing):
    case = host_case
    if missing != "request":
        request_observation(case)
    if missing != "stop":
        stopped_observation(case)
    assert await case.reconcile(current(case), case.checkpoint, case.env.actor) is None
    assert case.absence_checks == []
    assert_original_untouched(case)


@pytest.mark.parametrize(
    "publication",
    [
        "artifact",
        "unavailable_artifact",
        "artifact_event",
        "native_send",
        "earlier_intent_sent_later",
    ],
)
async def test_real_host_proof_rejects_post_call_publication_or_native_send(host_case, publication):
    case = host_case
    request_observation(case)
    stopped_observation(case)
    if publication in {"artifact", "unavailable_artifact"}:
        artifact_history(
            case, case.boundary + timedelta(seconds=1), available=publication == "artifact"
        )
    elif publication == "artifact_event":
        with case.env.store.transaction(case.env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body,created_at) VALUES(%s,%s,%s,2,'artifact_candidate','{}',%s)",
                (
                    tx.group_id,
                    case.work.id,
                    case.checkpoint.request.attempt_id,
                    case.boundary + timedelta(seconds=1),
                ),
            )
    else:
        native_send_history(case, old_intent=publication == "earlier_intent_sent_later")
    assert await case.reconcile(current(case), case.checkpoint, case.env.actor) is None
    assert case.absence_checks == []
    assert_original_untouched(case)


@pytest.mark.parametrize("field", ["name", "key", "revision"])
async def test_real_host_request_observation_must_bind_exact_tool_authority(host_case, field):
    case = host_case
    request_observation(
        case,
        **{
            field: {
                "name": "workspace_write_text",
                "key": "another-work:another-call",
                "revision": 2,
            }[field]
        },
    )
    stopped_observation(case)
    assert await case.reconcile(current(case), case.checkpoint, case.env.actor) is None
    assert_original_untouched(case)


async def test_stop_proof_must_follow_latest_persisted_request_observation(host_case):
    case = host_case
    request_observation(case)
    stopped_observation(case)
    with case.env.store.transaction(case.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_audit_events SET created_at=%s WHERE group_id=%s AND resource_ref=%s "
            "AND action='execution_stopped'",
            (case.boundary + timedelta(seconds=5), tx.group_id, case.work.id),
        )
        tx.conn.execute(
            "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body,created_at) "
            "SELECT group_id,work_id,attempt_id,2,kind,body,%s FROM ga_runtime_events "
            "WHERE group_id=%s AND work_id=%s AND attempt_id=%s AND sequence=1",
            (
                case.boundary + timedelta(seconds=10),
                tx.group_id,
                case.work.id,
                case.checkpoint.request.attempt_id,
            ),
        )
    assert await case.reconcile(current(case), case.checkpoint, case.env.actor) is None
    assert case.absence_checks == []
    assert_original_untouched(case)


async def test_host_proposal_is_archived_only_by_supervisor_compare_and_swap(host_case):
    case = host_case
    request_observation(case)
    stopped_observation(case)
    proposal = await case.reconcile(current(case), case.checkpoint, case.env.actor)
    assert proposal is not None
    assert_original_untouched(case)
    supervisor = case.fixtures.supervisor(case.env, reconcile_checkpoint=case.reconcile)
    try:
        assert await supervisor.run_once() is None
        stored = case.env.state.load_checkpoint(case.env.actor, case.work.id)
        assert stored.receipts[0].effect_state == EffectState.NO_EFFECT
        with case.env.store.transaction(case.env.actor) as tx:
            audit = tx.conn.execute(
                "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='work_effect_reconciled'",
                (tx.group_id, case.work.id),
            ).fetchone()["details"]
        assert audit["checkpoint_digest"] == checkpoint_digest(case.checkpoint)
        assert case.env.state._decode_checkpoint(audit["prior_checkpoint"]) == case.checkpoint
        assert current(case).state == WorkState.QUEUED
    finally:
        await supervisor.close()


async def test_cancellation_after_host_proof_wins_before_supervisor_compare_and_swap(host_case):
    case = host_case
    request_observation(case)
    stopped_observation(case)

    async def cancel_after_proof(work, checkpoint, actor):
        proposal = await case.reconcile(work, checkpoint, actor)
        assert proposal is not None
        with case.env.store.transaction(actor) as tx:
            latest = tx.get_work(work.id)
            tx.transition_work(work.id, WorkState.CANCELLED, expected_revision=latest.revision)
        return proposal

    supervisor = case.fixtures.supervisor(case.env, reconcile_checkpoint=cancel_after_proof)
    try:
        assert await supervisor.run_once() is None
        assert current(case).state == WorkState.CANCELLED
        assert_original_untouched(case)
    finally:
        await supervisor.close()
