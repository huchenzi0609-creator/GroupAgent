"""End-to-end synthetic turns exercise one controller, not phrase-specific rewrites."""

import asyncio

import pytest

from groupagent.channels import MemberRecord
from groupagent.control import WorkSelector
from groupagent.coordinator import Decision
from groupagent.domain import WorkState
from groupagent.memory.members import ChannelRosterProvider

from .test_app import app_for, event
from .test_early_ack import intake, setup_chat

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("owner", [True, False])
@pytest.mark.parametrize("mode", ["claim", "work", "policy"])
async def test_human_recipient_controls_speech_without_discarding_task_facts(
    runtime_env, owner, mode
):
    env = runtime_env
    request = event(
        env,
        {
            "claim": "小周，PPT我来准备。",
            "work": "小周，帮我制作PPT。",
            "policy": "小周，先保持安静。",
        }[mode],
    )
    candidates = {
        "claim": Decision(
            kind="command",
            reason_code="synthetic_claim",
            action={"op": "claim_task", "title": "准备演示PPT"},
            directness="direct",
            confidence=1,
        ),
        "work": Decision(
            kind="start_work",
            reason_code="synthetic_misdirected_request",
            goal="制作PPT",
            directness="direct",
            confidence=1,
        ),
        "policy": Decision(
            kind="command",
            reason_code="synthetic_misdirected_quiet",
            action={"op": "quiet", "scope": "group", "enabled": True},
            directness="direct",
            confidence=1,
        ),
    }
    app, _ = app_for(env, {request.id: candidates[mode]})
    try:
        env.channel.members_fixture = tuple(
            MemberRecord(
                group_id=env.actor.group_id,
                member_id=identifier,
                room_display_name=name,
                membership_state="present",
                name_source="roomDisplayName",
            )
            for identifier, name in ((env.actor.member_id, "小林"), ("synthetic-other", "小周"))
        )
        sync = await app.coordinator.members.sync_startup(
            env.actor, "human-recipient-test", ChannelRosterProvider(env.channel)
        )
        assert sync.state == "complete"
        if not owner:
            app.config = app.config.model_copy(
                update={
                    "group": app.config.group.model_copy(update={"owner_ids": ["synthetic-admin"]})
                }
            )
            app.coordinator.config = app.config
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.decision.kind == "observe" and result.delivery_id is None
        assert result.work_id is None
        assert bool(result.action_result) == (mode == "claim")
        with env.store.transaction(env.actor) as tx:
            assert not tx.conn.execute(
                "SELECT 1 FROM ga_conversation_policy WHERE group_id=%s AND quiet",
                (env.actor.group_id,),
            ).fetchone()
        if mode == "claim":
            with env.store.transaction(env.actor) as tx:
                assert (
                    tx.get_team_task(str(result.action_result["task_id"])).owner_id
                    == env.actor.member_id
                )
    finally:
        await app.close()


@pytest.mark.parametrize("direction", ["direct", "other", "none"])
async def test_task_declaration_records_once_and_only_replies_when_addressed(
    runtime_env, direction
):
    env = runtime_env
    request = event(env, "演示PPT由我准备，截止时间先记明天下午。")
    app, _ = app_for(
        env,
        {
            request.id: Decision(
                kind="command",
                reason_code="synthetic_declaration",
                action={"op": "claim_task", "title": "准备演示PPT", "due_note": "明天下午"},
                directness=direction,
                confidence=1,
            )
        },
    )
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.action_result and result.action_result["task_id"]
        assert bool(result.delivery_id) == (direction == "direct")
        assert result.decision.kind == ("command" if direction == "direct" else "observe")
        with env.store.transaction(env.actor) as tx:
            task = tx.get_team_task(str(result.action_result["task_id"]))
            assert task.owner_id == env.actor.member_id
            assert "明天下午" in task.progress
        duplicate = await app.coordinator.process(request)
        assert duplicate.duplicate and duplicate.action_result == result.action_result
    finally:
        await app.close()


async def seed_mixed_work(app, env):
    sources = [event(env, title) for title in ("旧文稿", "旧插图", "正在检索")]
    for source in sources:
        await intake(app, env, source)
    with env.store.transaction(env.actor) as tx:
        works = [tx.create_work(source.text, source.id) for source in sources]
        for work in works[:2]:
            tx.transition_work(
                work.id, WorkState.PAUSED, expected_revision=work.revision, reason="user_request"
            )
        tx.claim_work(works[2].id, "synthetic-other-worker", ttl_seconds=300)
    return works


async def test_state_filtered_plural_cancel_preserves_running_work(runtime_env):
    env = runtime_env
    command = event(env, "请取消所有处于暂停状态的工作")
    app, _ = app_for(
        env,
        {
            command.id: Decision(
                kind="control_work",
                reason_code="synthetic_semantic_plan",
                control="cancel",
                work_selector=WorkSelector(
                    basis="query", states=(WorkState.PAUSED,), quantifier="all", ownership="group"
                ),
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        works = await seed_mixed_work(app, env)
        await intake(app, env, command)
        result = await app.coordinator.process(command)
        assert set(result.controlled_work_ids) == {work.id for work in works[:2]}
        assert "2项" in result.decision.response
        with env.store.transaction(env.actor) as tx:
            assert all(tx.get_work(work.id).state == WorkState.CANCELLED for work in works[:2])
            assert tx.get_work(works[2].id).state == WorkState.RUNNING
        duplicate = await app.coordinator.process(command)
        assert duplicate.duplicate and duplicate.controlled_work_ids == result.controlled_work_ids
    finally:
        await app.close()


async def test_fresh_semantic_image_request_starts_new_work_with_its_plan(runtime_env):
    env = runtime_env
    request = event(env, "Pot，我想要几张海豹表情，帮我找一下")
    app, _ = app_for(
        env,
        {
            request.id: Decision(
                kind="start_work",
                reason_code="synthetic_image_goal",
                goal="查找三张海豹表情图片并发到群里。",
                work_capabilities=["public_images"],
                public_image_count=3,
                outputs=[{"format": "text"}],
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        old = await seed_mixed_work(app, env)
        await intake(app, env, request)
        assert app.coordinator._context(env.actor, request)["works"] == []
        result = await app.coordinator.process(request)
        assert result.work_id and result.work_id not in {work.id for work in old}
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
            assert work.scope["work_capabilities"] == ["public_images"]
            assert work.scope["public_image_count"] is None
            assert work.scope["planner_public_image_count"] == 3
            assert "web_image_import" in work.scope["required_tools"]
            assert work.scope["research_required"] is True
            assert tx.get_work(old[0].id).state == WorkState.PAUSED
    finally:
        await app.close()


async def test_query_then_plural_reference_uses_the_same_exact_set(runtime_env):
    env = runtime_env
    query = event(env, "请列出暂停状态的工作")
    command = event(env, "这些就取消吧")
    app, _ = app_for(
        env,
        {
            query.id: Decision(
                kind="query_work",
                reason_code="synthetic_query",
                work_selector=WorkSelector(
                    basis="query", states=(WorkState.PAUSED,), ownership="group", quantifier="all"
                ),
                directness="direct",
                confidence=1,
            ),
            command.id: Decision(
                kind="control_work",
                reason_code="synthetic_reference",
                control="cancel",
                work_selector=WorkSelector(
                    basis="focus", states=(WorkState.PAUSED,), ownership="group", quantifier="all"
                ),
                directness="direct",
                confidence=1,
            ),
        },
    )
    try:
        works = await seed_mixed_work(app, env)
        await intake(app, env, query)
        answer = await app.coordinator.process(query)
        assert len(answer.action_result["work_ids"]) == 2
        await intake(app, env, command)
        result = await app.coordinator.process(command)
        assert set(result.controlled_work_ids) == {work.id for work in works[:2]}
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(works[2].id).state == WorkState.RUNNING
    finally:
        await app.close()


async def test_concurrent_state_change_cannot_broaden_a_control_set(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    try:
        works = await seed_mixed_work(app, env)

        class Interpreter:
            async def decide(self, _event, _context):
                with env.store.transaction(env.actor) as tx:
                    current = tx.get_work(works[0].id)
                    tx.transition_work(
                        current.id,
                        WorkState.QUEUED,
                        expected_revision=current.revision,
                        reason="user_request",
                    )
                return Decision(
                    kind="control_work",
                    reason_code="synthetic_race",
                    control="cancel",
                    work_selector=WorkSelector(
                        basis="query",
                        states=(WorkState.PAUSED,),
                        ownership="group",
                        quantifier="all",
                    ),
                    directness="direct",
                    confidence=1,
                )

        app.coordinator.provider_factory = lambda *_: Interpreter()
        command = event(env, "请取消暂停状态的工作")
        await intake(app, env, command)
        result = await app.coordinator.process(command)
        assert result.decision.reason_code == "version_conflict"
        assert result.controlled_work_ids == []
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(works[0].id).state == WorkState.QUEUED
            assert tx.get_work(works[1].id).state == WorkState.PAUSED
            assert tx.get_work(works[2].id).state == WorkState.RUNNING
    finally:
        await app.close()


async def test_quiet_commits_without_ack_and_retires_inflight_chat(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env, waiting=True)
    question = event(env, "Pot，解释一下变量。")
    await intake(app, env, question)
    processing = asyncio.create_task(app.coordinator.process(question))
    try:
        await model.entered.wait()
        quiet = event(env, "Pot，先保持安静")
        await intake(app, env, quiet)
        result = await app.coordinator.process(quiet)
        assert result.decision.kind == "observe" and result.delivery_id is None
        with env.store.transaction(env.actor) as tx:
            row = tx.conn.execute(
                "SELECT quiet,quiet_since_sequence FROM ga_conversation_policy "
                "WHERE group_id=%s AND subject_id='*'",
                (env.actor.group_id,),
            ).fetchone()
            assert row["quiet"] and row["quiet_since_sequence"]
        model.release.set()
        old_reply = await processing
        assert old_reply.delivery_id is None
        assert await app.dispatcher.tick() == 0
        assert not list((env.channel.root / "outgoing").iterdir())
    finally:
        model.release.set()
        await asyncio.gather(processing, return_exceptions=True)
        await app.close()


async def test_work_execution_does_not_wait_forever_for_its_chat_ack(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    request = event(env, "Synthetic accepted background work")
    try:
        await intake(app, env, request)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("Synthetic work", request.id)
            intent = tx.enqueue_reply(request.id, "Synthetic initial reply")
        assert not app._foreground_ready(work)
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "UPDATE ga_deliveries SET created_at=clock_timestamp()-interval '3 seconds' "
                "WHERE group_id=%s AND id=%s",
                (env.actor.group_id, intent.id),
            )
        assert app._foreground_ready(work)
        with env.store.transaction(env.actor) as tx:
            tx.cancel_pending_delivery(intent.id, expected_revision=intent.revision)
        assert app._foreground_ready(work)
    finally:
        await app.close()
