"""Metadata consumption keeps aliases and the existing privacy dependency chain.

Real PostgreSQL/controller paths with explicit synthetic decisions. No external
model, execution worker, remote service or paid provider is used.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.control import WorkSelector
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import ActorContext, Role, WorkState
from groupagent.memory import MemberService, MemoryService
from groupagent.runtime.work_facts import WorkFacts
from groupagent.scheduling import Scheduler

from .foreground_model_fixture import candidate_reply, observation_reply
from .test_app import event

pytestmark = [pytest.mark.postgres, pytest.mark.work_awareness]


def emit(env, text, *, actor=None, at=None):
    actor = actor or env.actor
    stamp = at or datetime.now(UTC)
    value = event(env, text, actor_id=actor.member_id, occurred_at=stamp, received_at=stamp)
    with env.store.transaction(actor) as tx:
        return tx.ingest_event(value, "metadata-privacy:" + value.id, "1", 0).event


def harness(env, decide):
    class ForegroundFixture:
        name = "synthetic-metadata-privacy-router"

        def __init__(self):
            self.requests = []

        async def complete(self, request):
            final = observation_reply(request)
            if final is not None:
                return final
            payload = json.loads(request.messages[1].content)
            self.requests.append(payload)
            return candidate_reply(decide(payload))

    model = ForegroundFixture()
    provider = ModelDecisionProvider(model)
    memory = MemoryService(env.store)
    coordinator = ConversationCoordinator(
        AppConfig(
            group=GroupConfig(
                enabled=True,
                internal_id=env.actor.group_id,
                external_group_id=env.config.external_group_id,
                owner_ids=[env.actor.member_id],
            )
        ),
        env.store,
        lambda actor, source: provider,
        memory=memory,
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    return SimpleNamespace(coordinator=coordinator, model=model, memory=memory)


def seed_work(env, goal, *, secondary=None, actor=None, completed=False, at=None):
    """Only seed metadata; completed is a fixture state, not an execution claim."""
    actor = actor or env.actor
    source = emit(env, "请根据指定材料整理这份报告。", actor=actor, at=at)
    with env.store.transaction(actor) as tx:
        sources = [source.id, *([secondary.id] if secondary is not None else [])]
        work = tx.create_work(
            goal,
            source.id,
            scope={
                "source_event_ids": sources,
                "source_fingerprints": {
                    identifier: tx.event_fingerprint(identifier) for identifier in sources
                },
            },
        )
        if completed:
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='completed',created_at=%s,updated_at=%s "
                "WHERE group_id=%s AND id=%s",
                (at, at, actor.group_id, work.id),
            )
        return source, tx.get_work(work.id)


def assert_reply_redacted(tx, source_id, delivery_id, marker):
    decision = tx.get_decision(source_id)
    assert decision["privacy_redacted"] is True
    assert decision["result"]["decision"]["reason_code"] == "source_forgotten"
    assert marker not in json.dumps(decision, ensure_ascii=False)
    delivery = tx.get_delivery(delivery_id)
    assert delivery.state.value == "rejected" and delivery.reason == "source_forgotten"
    assert delivery.payload_ref == "text:[content removed]"


async def test_overview_aliases_historical_requester_without_any_roster_entry(runtime_env):
    env = runtime_env
    stranger_id = "never-rostered-private-member-" + uuid4().hex
    stranger = ActorContext(group_id=env.actor.group_id, member_id=stranger_id, role=Role.MEMBER)
    _, work = seed_work(
        env, "合成历史作者的公开报告", actor=stranger, at=datetime.now(UTC) - timedelta(days=2)
    )
    with env.store.transaction(env.actor) as tx:
        # Current ingestion seeds membership. A legacy/imported work may have
        # survived without that roster row; exercise that independent boundary.
        tx.conn.execute(
            "DELETE FROM ga_members WHERE group_id=%s AND member_id=%s",
            (env.actor.group_id, stranger_id),
        )

    def decide(payload):
        shown = {fact["id"]: fact for fact in payload["context"]["work_facts_overview"]["facts"]}
        assert work.id in shown
        assert shown[work.id]["requester_id"].startswith("member_")
        assert stranger_id not in json.dumps(payload, ensure_ascii=False)
        return Decision(
            kind="reply",
            reason_code="synthetic_normal_chat",
            response="可以，我们聊聊。",
            directness="direct",
            confidence=1,
        )

    h = harness(env, decide)
    with env.store.transaction(env.actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT 1 FROM ga_members WHERE group_id=%s AND member_id=%s",
                (env.actor.group_id, stranger_id),
            ).fetchone()
            is None
        )
    request = emit(env, "Pot，聊两句吧。")
    result = await h.coordinator.process(request)
    assert result.decision.kind == "reply" and result.delivery_id
    assert len(h.model.requests) == 1


async def test_archived_title_lookup_reply_is_revoked_with_its_secondary_source(runtime_env):
    env = runtime_env
    old = datetime.now(UTC) - timedelta(days=4)
    marker = "SYNTHETIC-DERIVED-PRIVATE-" + uuid4().hex
    secondary = emit(env, "合成来源内容：" + marker, at=old)
    root, work = seed_work(env, marker + "季度报告", secondary=secondary, completed=True, at=old)
    for index in range(4):
        seed_work(
            env,
            "合成近期已完成工作" + str(index),
            completed=True,
            at=old + timedelta(days=1, minutes=index),
        )

    def decide(payload):
        context = payload["context"]
        assert work.id not in {fact["id"] for fact in context["work_facts_overview"]["facts"]}
        assert work.id not in {fact["id"] for fact in context["works"]}
        assert secondary.id not in context["reference_catalog"]["source_event_ids"]
        return Decision(
            kind="query_work",
            reason_code="synthetic_archived_lookup",
            work_selector=WorkSelector(basis="query", title_query="季度报告", quantifier="all"),
            directness="direct",
            confidence=1,
        )

    h = harness(env, decide)
    request = emit(env, "Pot，季度报告现在是什么状态？")
    context = h.coordinator._context(env.actor, request)
    assert secondary.id not in context["_snapshot"]["source_fingerprints"]
    result = await h.coordinator.process(request)
    assert result.work_id == work.id and marker in result.decision.response
    with env.store.transaction(env.actor) as tx:
        stored = tx.get_decision(request.id)
        assert {root.id, secondary.id}.issubset(stored["source_event_ids"])
    h.memory.forget(env.actor, event_ids=[secondary.id])
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).scope["privacy_redacted"] is True
        assert_reply_redacted(tx, request.id, result.delivery_id, marker)
    assert len(h.model.requests) == 1


@pytest.mark.parametrize("consumer", ["reply", "child_work"])
async def test_overview_metadata_provenance_revokes_reply_and_derived_work(runtime_env, consumer):
    env = runtime_env
    old = datetime.now(UTC) - timedelta(days=2)
    marker = "SYNTHETIC-OVERVIEW-TOPIC-" + uuid4().hex
    secondary = emit(env, "合成来源内容：" + marker, at=old)
    root, parent = seed_work(env, marker + "关联主题", secondary=secondary, at=old)

    def decide(payload):
        context = payload["context"]
        visible = {fact["id"]: fact for fact in context["work_facts_overview"]["facts"]}
        assert parent.id in visible and marker in visible[parent.id]["goal"]
        assert not context["focused_work_ids"]
        assert secondary.id not in context["reference_catalog"]["source_event_ids"]
        topic = visible[parent.id]["goal"]
        if consumer == "reply":
            return Decision(
                kind="reply",
                reason_code="synthetic_topic_chat",
                response="可以围绕这个主题聊思路：" + topic,
                directness="direct",
                confidence=1,
            )
        return Decision(
            kind="start_work",
            reason_code="synthetic_related_child",
            goal="从新的角度整理文字说明：" + topic,
            outputs=[{"format": "text"}],
            directness="direct",
            confidence=1,
        )

    h = harness(env, decide)
    request = emit(
        env,
        "Pot，说句鼓励的话吧。"
        if consumer == "reply"
        else "Pot，基于概览里的主题单独做一份简短说明。",
    )
    context = h.coordinator._context(env.actor, request)
    assert secondary.id not in context["_snapshot"]["source_fingerprints"]
    result = await h.coordinator.process(request)
    assert result.delivery_id and len(h.model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        stored = tx.get_decision(request.id)
        assert {root.id, secondary.id}.issubset(stored["source_event_ids"])
        if consumer == "child_work":
            child = tx.get_work(result.work_id)
            assert marker in child.goal and child.id != parent.id
            # Metadata dependencies are not a grant to consume the source's text.
            assert secondary.id not in child.scope["source_fingerprints"]
        else:
            assert marker in result.decision.response
    h.memory.forget(env.actor, event_ids=[secondary.id])
    with env.store.transaction(env.actor) as tx:
        assert_reply_redacted(tx, request.id, result.delivery_id, marker)
        if consumer == "child_work":
            changed = tx.get_work(result.work_id)
            assert changed.state == WorkState.CANCELLED and changed.scope["privacy_redacted"]
            assert marker not in changed.goal
    if consumer == "child_work":
        snapshot = WorkFacts(env.store).lookup(env.actor, ids=(result.work_id,))
        assert snapshot["status"] == "unavailable" and snapshot["facts"] == []
