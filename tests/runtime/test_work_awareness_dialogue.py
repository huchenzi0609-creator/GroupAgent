"""Synthetic dialogue through the real router, PG controller and local delivery.

The scripted foreground model checks its actual context and emits typed plans;
it is not evidence of live language understanding. Background tools/models and
image decoding are explicit fixtures. PNG bytes and local acceptance are real;
no public search, paid provider, WeChat delivery or OS isolation is claimed.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from PIL import Image

from groupagent.artifacts import ArtifactScope
from groupagent.channels import MemberRecord
from groupagent.control import WorkSelector
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.coordinator import Decision
from groupagent.domain import ActorContext, EventKind, Role, WorkState
from groupagent.execution import ExecutionRequest, ModelReply, ScopedToolEngine, TokenUsage
from groupagent.execution.models import ModelToolCall
from groupagent.media import MediaService, MediaSource
from groupagent.memory import MemoryService
from groupagent.memory.members import ChannelRosterProvider
from groupagent.runtime.contracts import DeliveryDraft, PreparedExecution, ValidatedCandidate
from groupagent.runtime.work_facts import WorkFacts
from groupagent.tools import (
    IdempotencyMode,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
)

from .conftest import source
from .foreground_model_fixture import candidate_reply, observation_reply, semantic_fixture_reply
from .test_app import app_for, event
from .test_async_delivery_supervision import NativeQueueFixture, prepared_supervisor
from .test_early_ack import intake
from .test_inputs import InputFixtureWorker

pytestmark = [pytest.mark.postgres, pytest.mark.work_awareness]
_FIXTURE = json.loads(
    (
        Path(__file__).resolve().parents[1] / "fixtures" / "work_awareness_dialogue.v1.json"
    ).read_text()
)
_TURNS = {turn["id"]: turn for turn in _FIXTURE["dialogue"]}


class FixtureSearchInput(ToolData):
    query: str


class FixtureSearchOutput(ToolData):
    synthetic_fixture: bool = True
    source_url: str = "https://example.com/synthetic-alan"


def png_bytes(index=0):
    result = io.BytesIO()
    Image.new("RGB", (48 + index, 32), (30 + index * 50, 80, 140)).save(result, "PNG")
    return result.getvalue()


def works(env):
    with env.store.transaction(env.actor) as tx:
        return tx.list_work()


def facts_by_id(snapshot):
    assert snapshot["observed_at"] and snapshot["query_scope"]["visibility"] == "public"
    return {fact["id"]: fact for fact in snapshot["facts"]}


def foreground_model(harness):
    class ForegroundModel:
        name = "synthetic-work-awareness-router"

        async def complete(self, request):
            marker = request.messages[0].content or ""
            if marker.startswith("TURN_OUTCOME_V1"):
                return semantic_fixture_reply(request)
            if marker.startswith("TURN_CONTRACT_V1"):
                payload = json.loads(request.messages[1].content)
                current_intent = harness.scenarios[payload["event"]["id"]]["intent"]
                required = (
                    [{"kind": "start_work"}]
                    if current_intent in {"start_profile", "start_memes"}
                    else ([{"kind": "read_work_facts"}] if current_intent == "query_memes" else [])
                )
                return semantic_fixture_reply(
                    request,
                    required_actions=required,
                    intent="observe" if current_intent == "peer_conversation" else None,
                    work_context_required=bool(required),
                )
            final = observation_reply(request)
            if final is not None:
                return final
            payload = json.loads(request.messages[1].content)
            current = payload["event"]
            scenario = harness.scenarios[current["id"]]
            harness.foreground_requests.append(payload)
            intent = scenario["intent"]
            if intent == "start_profile":
                decision = Decision(
                    kind="start_work",
                    reason_code="synthetic_profile_plan",
                    goal="整理主播阿蓝资料与公开来源",
                    work_capabilities=["public_research"],
                    outputs=[{"format": "text"}],
                    directness="direct",
                    confidence=1,
                )
            elif intent == "start_memes":
                overview = facts_by_id(payload["context"]["work_facts_overview"])
                assert harness.ids["profile"] in overview
                decision = Decision(
                    kind="start_work",
                    reason_code="synthetic_pronoun_resolution",
                    goal="查找主播阿蓝梗图并交付两张PNG图片",
                    work_capabilities=["public_images"],
                    outputs=[{"format": "png"}],
                    public_image_count=2,
                    directness="direct",
                    confidence=1,
                )
            elif intent == "query_memes":
                overview = facts_by_id(payload["context"]["work_facts_overview"])
                assert harness.ids["memes"] in overview
                assert "fixture-query-args-must-stay-private" not in json.dumps(overview)
                decision = Decision(
                    kind="query_work",
                    reason_code="synthetic_semantic_work_followup",
                    work_selector=WorkSelector(
                        basis="query", title_query="阿蓝梗图", requester_scope="self"
                    ),
                    directness="direct",
                    confidence=1,
                )
            elif intent == "ordinary_reply":
                decision = Decision(
                    kind="reply",
                    reason_code="synthetic_unrelated_conversation",
                    response="这是当前普通话题的合成回复。",
                    directness="direct",
                    confidence=1,
                )
            else:
                decision = Decision(kind="observe", reason_code="synthetic_peer_conversation")
            return candidate_reply(decision)

    return ForegroundModel()


def dialogue_harness(env):
    app, _ = app_for(env, {})
    harness = SimpleNamespace(
        app=app,
        env=env,
        ids={},
        scenarios={},
        foreground_requests=[],
        factory_calls=[],
        validation_calls=[],
        search_calls=[],
        image_started=asyncio.Event(),
        image_release=asyncio.Event(),
    )
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(foreground_model(harness))

    async def factory(work, lease, actor, workspace):
        harness.factory_calls.append(work.id)
        image_work = "梗图" in work.goal
        gateway = ToolGateway()

        async def search(arguments, context):
            harness.search_calls.append(context.work_id)
            return FixtureSearchOutput()

        gateway.register(
            ToolDefinition(
                name="web_search",
                description="Offline synthetic source observation only",
                input_model=FixtureSearchInput,
                output_model=FixtureSearchOutput,
                handler=search,
                risk_class="read_only",
                idempotency_mode=IdempotencyMode.READ_ONLY,
            )
        )

        class BackgroundModel:
            name = "synthetic-work-awareness-background"

            def __init__(self):
                self.count = 0

            async def complete(self, request):
                self.count += 1
                if self.count == 1:
                    return ModelReply(
                        tool_calls=(
                            ModelToolCall(
                                id="synthetic-search",
                                name="web_search",
                                arguments={"query": "fixture-query-args-must-stay-private"},
                            ),
                        ),
                        finish_reason="tool_calls",
                        usage=TokenUsage(input_tokens=1, output_tokens=1),
                    )
                if image_work:
                    harness.image_started.set()
                    await harness.image_release.wait()
                return ModelReply(
                    text="合成阿蓝资料与图片结果。",
                    usage=TokenUsage(input_tokens=1, output_tokens=1),
                )

        return PreparedExecution(
            ScopedToolEngine(BackgroundModel(), gateway),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                revision=work.goal_revision,
                trace_id=work.request_event_id,
                allowed_tools=frozenset({"web_search"}),
                required_tools=frozenset({"web_search"}),
            ),
        )

    class Validator:
        async def validate(self, work, lease, actor, snapshot, workspace):
            harness.validation_calls.append(work.id)
            assert any(
                receipt.tool_name == "web_search" and receipt.success
                for receipt in snapshot.receipts
            )
            if "梗图" not in work.goal:
                text = "主播阿蓝的合成资料。来源：https://example.com/synthetic-alan"
                return ValidatedCandidate(
                    {"result": "synthetic-profile-validation"},
                    (
                        DeliveryDraft(
                            "text:" + text,
                            hashlib.sha256(text.encode()).hexdigest(),
                            "profile:" + work.id,
                        ),
                    ),
                )
            context = ToolContext(
                ToolScope(
                    actor.group_id, work.id, actor, workspace, frozenset(), work.request_event_id
                ),
                "fixture-validation",
                "fixture-validation:" + work.id,
                asyncio.Event(),
            )
            media = MediaService(env.artifacts, InputFixtureWorker())
            drafts = []
            for index in range(2):
                reference = await media.ingest_image(
                    context,
                    MediaSource(
                        group_id=actor.group_id,
                        work_id=work.id,
                        member_id=actor.member_id,
                        source_event_id=work.request_event_id,
                        source_message_id=work.request_event_id,
                        attachment_id="synthetic-image:" + str(index),
                        declared_kind="image",
                        origin_kind="synthetic_fixture",
                    ),
                    png_bytes(index),
                )
                scope = ArtifactScope(actor.group_id, work.id, actor.member_id)
                version = env.artifacts.get(scope, reference.file_id, reference.version_id)
                record = env.artifacts.validations(scope, reference.file_id, reference.version_id)[
                    -1
                ]
                env.registry.register(actor, lease, version, validation=record.to_dict())
                drafts.append(
                    DeliveryDraft(
                        "artifact:" + version.version_id,
                        version.sha256,
                        "image:" + work.id + ":" + str(index),
                    )
                )
            return ValidatedCandidate(
                {
                    "result": "synthetic-result-validation",
                    "artifact:png": "actual-synthetic-png-decode",
                },
                tuple(drafts),
            )

    app.supervisor.factory, app.supervisor.validator = factory, Validator()
    return harness


async def turn(harness, identifier, *, actor_id=None):
    scenario = _TURNS[identifier]
    changes = {}
    if scenario["intent"] == "attach_synthetic_image":
        message_id = "synthetic-followup-image-" + uuid4().hex
        handle = harness.env.channel.add_attachment(
            message_id, "synthetic-followup.png", png_bytes(), kind="image"
        )
        changes = {
            "kind": EventKind.IMAGE,
            "attachments": [handle.as_attachment()],
            "channel_account_id": harness.env.config.account_id,
            "source_epoch": harness.env.config.source_epoch,
        }
    value = event(
        harness.env,
        scenario["text"],
        actor_id=actor_id
        or ("synthetic-peer" if scenario["actor"] == "peer" else harness.env.actor.member_id),
        **changes,
    )
    harness.scenarios[value.id] = scenario
    await intake(harness.app, harness.env, value)
    result = await harness.app.coordinator.process(value)
    if scenario.get("expected_work"):
        assert result.work_id
        harness.ids[scenario["expected_work"]] = result.work_id
    return value, result


async def completed_profile_and_running_memes(harness):
    env = harness.env
    env.channel.members_fixture = tuple(
        MemberRecord(
            group_id=env.actor.group_id,
            member_id=identifier,
            room_display_name=name,
            membership_state="present",
            name_source="roomDisplayName",
        )
        for identifier, name in ((env.actor.member_id, "小林"), ("synthetic-peer", "小周"))
    )
    synced = await harness.app.coordinator.members.sync_startup(
        env.actor, "synthetic-work-awareness-roster", ChannelRosterProvider(env.channel)
    )
    assert synced.state == "complete"
    await turn(harness, "profile_request")
    assert (await harness.app.supervisor.run_once()).state == WorkState.DELIVERING
    await harness.app.dispatcher.tick()
    await harness.app.supervisor._poll_deliveries()
    await turn(harness, "meme_request")
    running = asyncio.create_task(harness.app.supervisor.run_once())
    await asyncio.wait_for(harness.image_started.wait(), 5)
    return running


async def test_full_synthetic_dialogue_retains_two_real_works_after_topic_change_and_delivery(
    runtime_env,
):
    env, harness = runtime_env, dialogue_harness(runtime_env)
    running = None
    try:
        running = await completed_profile_and_running_memes(harness)
        _, early = await turn(harness, "early_progress")
        early_facts = facts_by_id(early.action_result["work_facts"])
        assert early_facts[harness.ids["memes"]]["state"] == "running"
        for scenario in _FIXTURE["dialogue"]:
            if scenario["id"].startswith("unrelated_"):
                _, ordinary = await turn(harness, scenario["id"])
                assert ordinary.work_id is None and not ordinary.controlled_work_ids
                if ordinary.decision.response:
                    assert "阿蓝" not in ordinary.decision.response
        _, progress = await turn(harness, "named_progress")
        snapshot = progress.action_result["work_facts"]
        assert snapshot["status"] == "matched" and snapshot["complete"]
        fact = facts_by_id(snapshot)[harness.ids["memes"]]
        assert fact["state"] == "running"
        assert "web_search" in {tool["tool_name"] for tool in fact["activity"]["recent_tools"]}
        assert progress.controlled_work_ids == []
        assert len(works(env)) == 2
        harness.image_release.set()
        assert (await asyncio.wait_for(running, 5)).state == WorkState.DELIVERING
        await harness.app.dispatcher.tick()
        await harness.app.supervisor._poll_deliveries()
        for identifier in (
            "contradiction_after_delivery",
            "wake_after_delivery",
            "playful_followup",
        ):
            _, response = await turn(harness, identifier)
            fact = facts_by_id(response.action_result["work_facts"])[harness.ids["memes"]]
            assert fact["state"] == "completed"
            assert fact["outputs"]["validated_count"] == 2
            assert fact["delivery"]["accepted"] == fact["delivery"]["total"] == 2
            assert fact["delivery"]["image_accepted"] == 2
            assert response.controlled_work_ids == []
        picture, image_result = await turn(harness, "image_followup")
        assert image_result.work_id is None and image_result.delivery_id is None
        _, look = await turn(harness, "look_yourself")
        assert (
            facts_by_id(look.action_result["work_facts"])[harness.ids["memes"]]["state"]
            == "completed"
        )
        assert any(
            row["id"] == picture.id and row["attachments"]
            for row in harness.foreground_requests[-1]["context"]["recent_messages"]
        )
        current_works = works(env)
        assert len(current_works) == 2
        assert all(work.goal_revision == 1 for work in current_works)
        with env.store.transaction(env.actor) as tx:
            attempts = tx.conn.execute(
                "SELECT work_id,count(*) AS count FROM ga_execution_attempts "
                "WHERE group_id=%s GROUP BY work_id",
                (env.actor.group_id,),
            ).fetchall()
        assert {row["work_id"]: row["count"] for row in attempts} == {
            identifier: 1 for identifier in harness.ids.values()
        }
        assert (
            sorted(harness.factory_calls)
            == sorted(harness.validation_calls)
            == sorted(harness.ids.values())
        )
        outgoing = [
            json.loads(path.read_text()) for path in (env.channel.root / "outgoing").glob("*.json")
        ]
        assert len([item for item in outgoing if item["payload"]["kind"] == "image"]) == 2
    finally:
        harness.image_release.set()
        if running:
            await asyncio.gather(running, return_exceptions=True)
        await harness.app.close()


async def test_read_query_refreshes_a_running_to_delivering_race_without_write_cas(runtime_env):
    env, harness = runtime_env, dialogue_harness(runtime_env)
    running = None
    try:
        running = await completed_profile_and_running_memes(harness)
        base_factory = harness.app.coordinator.provider_factory

        class RacingProvider:
            uses_agent_kernel = True

            async def decide(self, current, context):
                original_read = context["_read_work_facts"]

                async def read_after_transition(selector, evidence):
                    harness.image_release.set()
                    assert (await asyncio.wait_for(running, 5)).state == WorkState.DELIVERING
                    return await original_read(selector, evidence)

                context["_read_work_facts"] = read_after_transition
                return await base_factory(env.actor, current).decide(current, context)

        harness.app.coordinator.provider_factory = lambda *_: RacingProvider()
        _, result = await turn(harness, "named_progress")
        assert result.decision.reason_code != "version_conflict"
        fact = facts_by_id(result.action_result["work_facts"])[harness.ids["memes"]]
        assert fact["state"] == "delivering" and fact["delivery"]["accepted"] == 0
        prior = facts_by_id(harness.foreground_requests[-1]["context"]["work_facts_overview"])[
            harness.ids["memes"]
        ]
        assert prior["state"] == "running" and prior["revision"] < fact["revision"]
        assert len(works(env)) == 2 and not result.controlled_work_ids
    finally:
        harness.image_release.set()
        if running:
            await asyncio.gather(running, return_exceptions=True)
        await harness.app.close()


@pytest.mark.parametrize(
    "native_state,has_job,phase", [("running", True, "in_progress"), ("queued", False, "uncertain")]
)
async def test_native_progress_and_unknown_sends_are_facts_not_recipient_read_claims(
    runtime_env, native_state, has_job, phase
):
    env, channel = runtime_env, NativeQueueFixture(native_state, has_job=has_job)
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    try:
        await supervisor.run_once()
        await dispatcher.tick(limit=1)
        snapshot = WorkFacts(env.store).lookup(env.actor, ids=(work.id,))
        fact = facts_by_id(snapshot)[work.id]
        assert fact["delivery"]["phase"] == phase
        assert fact["delivery"]["accepted"] == 0
        assert "recipient_read" not in json.dumps(snapshot)
        assert len(channel.submissions) == calls.factory == calls.validation == 1
    finally:
        await supervisor.close()


def test_public_work_facts_hide_foreign_group_and_participants_only_topic(runtime_env):
    env = runtime_env
    public_source = source(env, "Synthetic public work")
    secret_source = source(env, "Synthetic restricted source")
    memory = MemoryService(env.store)
    topic = memory.create_topic(env.actor, "SYNTHETIC PRIVATE TOPIC", visibility="participants")
    memory.link_event(env.actor, secret_source.id, topic.id, evidence_event_ids=[secret_source.id])
    with env.store.transaction(env.actor) as tx:
        public = tx.create_work("Synthetic visible work", public_source.id)
        private = tx.create_work(
            "SYNTHETIC PRIVATE WORK TITLE", secret_source.id, scope={"topic_id": topic.id}
        )
    foreign_group = "synthetic-foreign-" + uuid4().hex
    env.store.register_group(foreign_group)
    foreign_actor = ActorContext(
        group_id=foreign_group, member_id="synthetic-foreign-owner", role=Role.OWNER
    )
    foreign_source = event(
        env, "SYNTHETIC FOREIGN SOURCE", group_id=foreign_group, actor_id=foreign_actor.member_id
    )
    with env.store.transaction(foreign_actor) as tx:
        tx.ingest_event(foreign_source, foreign_source.id, "1", 0)
        foreign = tx.create_work("SYNTHETIC FOREIGN WORK TITLE", foreign_source.id)
    facts = WorkFacts(env.store)
    overview = facts.overview(env.actor)
    assert public.id in facts_by_id(overview)
    assert private.id not in facts_by_id(overview) and foreign.id not in facts_by_id(overview)
    for identifier in (private.id, foreign.id, "synthetic-missing-work"):
        snapshot = facts.lookup(env.actor, ids=(identifier,))
        assert snapshot["status"] == "unavailable" and snapshot["facts"] == []
        assert "SYNTHETIC PRIVATE" not in json.dumps(snapshot)
        assert "SYNTHETIC FOREIGN" not in json.dumps(snapshot)


@pytest.mark.parametrize("boundary", ["same_actor", "other_actor", "new_quiet_barrier"])
async def test_quiet_preference_preserves_only_the_authorized_same_actor_media_session(
    runtime_env, boundary
):
    env, harness = runtime_env, dialogue_harness(runtime_env)
    running = None

    async def quiet_turn():
        quiet = event(env, "Pot，先保持安静，等我叫你。")
        harness.scenarios[quiet.id] = {"intent": "ordinary_observe"}
        await intake(harness.app, env, quiet)
        await harness.app.coordinator.process(quiet)
        with env.store.transaction(env.actor) as tx:
            row = tx.conn.execute(
                "SELECT quiet,revision FROM ga_conversation_policy WHERE group_id=%s AND subject_id='*'",
                (env.actor.group_id,),
            ).fetchone()
        assert row["quiet"]
        return row

    try:
        running = await completed_profile_and_running_memes(harness)
        harness.image_release.set()
        await asyncio.wait_for(running, 5)
        await harness.app.dispatcher.tick()
        await harness.app.supervisor._poll_deliveries()
        policy = await quiet_turn()
        _, wake = await turn(harness, "wake_after_delivery")
        assert wake.action_result["work_facts"]["facts"]
        picture, image_result = await turn(
            harness,
            "image_followup",
            actor_id="synthetic-peer" if boundary == "other_actor" else None,
        )
        assert image_result.decision.kind in {"observe", "ignore"}
        assert image_result.work_id is None and image_result.delivery_id is None
        if boundary == "new_quiet_barrier":
            policy = await quiet_turn()
        before = len(harness.foreground_requests)
        _, followup = await turn(harness, "look_yourself")
        if boundary == "same_actor":
            assert len(harness.foreground_requests) == before + 1
            fact = facts_by_id(followup.action_result["work_facts"])[harness.ids["memes"]]
            assert fact["state"] == "completed" and fact["delivery"]["accepted"] == 2
            recent = harness.foreground_requests[-1]["context"]["recent_messages"]
            assert any(row["id"] == picture.id and row["attachments"] for row in recent)
        else:
            assert len(harness.foreground_requests) == before
            assert followup.decision.kind == "observe" and followup.delivery_id is None
        with env.store.transaction(env.actor) as tx:
            assert (
                tx.conn.execute(
                    "SELECT quiet,revision FROM ga_conversation_policy WHERE group_id=%s AND subject_id='*'",
                    (env.actor.group_id,),
                ).fetchone()
                == policy
            )
        assert len(works(env)) == 2
    finally:
        harness.image_release.set()
        if running:
            await asyncio.gather(running, return_exceptions=True)
        await harness.app.close()
