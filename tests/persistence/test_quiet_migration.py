"""Unpublished 013→014 upgrade on disposable schemas and synthetic messages."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from importlib.resources import files
from uuid import uuid4

import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from groupagent.artifacts import ArtifactStore
from groupagent.channels import AgentWechatConfig
from groupagent.channels.local import LocalChannel
from groupagent.conversation.policy import ingress_admission
from groupagent.domain import ActorContext, DeliveryState, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore
from groupagent.persistence import postgres as postgres_module
from groupagent.runtime.artifacts import ArtifactRegistry
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.state import RuntimeState

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("subject", ["*", "synthetic-member"])
async def test_upgrade_seeds_existing_quiet_and_preserves_new_explicit_invocation(
    tmp_path, monkeypatch, subject
):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    base = PostgresStore(dsn)
    schema = "quiet_upgrade_" + uuid4().hex
    with base.connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    store = PostgresStore(make_conninfo(dsn, options=f"-c search_path={schema}"))
    old = tmp_path / "migrations"
    old.mkdir()
    for path in files("groupagent.persistence").joinpath("migrations").iterdir():
        if path.name.endswith(".sql") and int(path.name.split("_", 1)[0]) <= 13:
            (old / path.name).write_text(path.read_text(), encoding="utf-8")
    try:
        with monkeypatch.context() as patch:
            patch.setattr(postgres_module, "files", lambda _: tmp_path)
            assert store.migrate() == 13
        group = "synthetic-upgrade"
        store.register_group(group)
        actor = ActorContext(group_id=group, member_id="synthetic-member", role=Role.OWNER)

        def ingest(text, *, member_id=actor.member_id):
            identifier = uuid4().hex
            stamp = datetime.now(UTC)
            value = GroupEvent(
                id=identifier,
                source_key=identifier,
                group_id=group,
                actor_id=member_id,
                kind=EventKind.TEXT,
                text=text,
                occurred_at=stamp,
                received_at=stamp,
            )
            with store.transaction(actor) as tx:
                return tx.ingest_event(value, value.id, "first", 0).event

        stale = ingest("Pot，解释一下之前的问题。")
        with store.transaction(actor) as tx:
            old_reply = tx.enqueue_reply(stale.id, "Synthetic old queued answer")
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,%s,true)",
                (group, subject),
            )
        ingest("Synthetic other member discussion", member_id="synthetic-other")
        with store.transaction(actor) as tx:
            expected_cutoff = tx.conn.execute(
                "SELECT max(sequence) AS sequence FROM ga_events WHERE group_id=%s "
                "AND (%s='*' OR actor_id=%s)",
                (group, subject, subject),
            ).fetchone()["sequence"]
        assert store.migrate() == 19 and store.migrate() == 19
        with store.transaction(actor) as tx:
            policy = tx.conn.execute(
                "SELECT quiet_since_sequence FROM ga_conversation_policy WHERE group_id=%s",
                (group,),
            ).fetchone()
            assert policy["quiet_since_sequence"] == expected_cutoff
            assert not ingress_admission(tx, stale).allowed
            assert tx.get_delivery(old_reply.id).payload_ref == "text:Synthetic old queued answer"
        fresh = ingest("Pot，现在解释一下这个新问题。")
        with store.transaction(actor) as tx:
            admission = ingress_admission(tx, fresh)
            assert admission.allowed and admission.invocation_root_event_id == fresh.id
            new_reply = tx.enqueue_reply(fresh.id, "Synthetic fresh answer")
        config = AgentWechatConfig(
            base_url="http://127.0.0.1",
            account_id="synthetic-account",
            external_group_id="synthetic-upgrade@chatroom",
            internal_group_id=group,
            source_epoch="synthetic-test",
        )
        channel = LocalChannel(tmp_path / "channel", config)
        registry = ArtifactRegistry(store, ArtifactStore(tmp_path / "artifacts"))
        sender = Dispatcher(RuntimeState(store), actor, lambda _: actor, channel, registry)
        assert await sender.tick() == 1
        with store.transaction(actor) as tx:
            assert tx.get_delivery(old_reply.id).state == DeliveryState.REJECTED
            assert tx.get_delivery(old_reply.id).reason == "quiet_barrier"
            assert tx.get_delivery(new_reply.id).state == DeliveryState.ACCEPTED
        assert len(list((tmp_path / "channel/outgoing").glob("*.json"))) == 1
    finally:
        with base.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
