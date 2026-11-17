"""Synthetic coverage for actual nickname refresh and truthful immediate replies."""

import json
from datetime import UTC, datetime

import pytest
from conversation.test_react_turn import ModelScript, call, finish, tool_receipt

from groupagent.channels import MemberRecord
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.runtime.roster import roster_query
from groupagent.runtime.work_status import work_status_detail

from .test_app import event
from .test_early_ack import intake, setup_chat


@pytest.mark.parametrize(
    "text,recent,expected",
    [
        ("Pot，你能看见我的群昵称了吗？", [], True),
        ("Pot，请刷新成员名单", [], True),
        ("你再看看呢，现在行了吗", [{"text": "微信接口还没返回群昵称"}], True),
        ("你再看看这份 PDF", [], False),
        ("Pot，你叫什么名字", [], False),
        ("引用原文：‘Pot，请刷新群昵称’", [], False),
    ],
)
def test_refresh_requires_current_roster_request(text, recent, expected):
    now = datetime.now(UTC)
    value = GroupEvent(
        id="query",
        source_key="query",
        group_id="g",
        actor_id="owner",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=now,
        received_at=now,
    )
    assert roster_query(value, {"recent_messages": recent}) is expected


def test_legacy_read_pause_explains_actual_page_failure_without_money_guess():
    detail = work_status_detail(
        {
            "pause_reason": "effect_unknown",
            "scope": {},
            "_runtime_failure": {"tool_name": "web_fetch", "error_code": "page_unavailable"},
        }
    )
    assert detail["phase"] == "research"
    assert "网页无法访问" in detail["message"] and "费用" not in detail["message"]


@pytest.mark.postgres
async def test_natural_query_refreshes_name_through_the_roster_tool(runtime_env):
    env = runtime_env
    app, _, _ = setup_chat(env)
    calls = []

    async def refresh(group_id, **kwargs):
        calls.append((group_id, kwargs))
        env.channel.members_fixture = (
            MemberRecord(
                group_id=group_id,
                member_id=env.actor.member_id,
                room_display_name="合成群昵称",
                membership_state="present",
                name_source="roomDisplayName",
            ),
        )

    env.channel.refresh_members = refresh
    request = event(env, "Pot，刚刚已经改妥当了，劳驾再核对一次。")

    def answer(request):
        receipt = tool_receipt(request)
        facts = receipt["facts"]
        assert facts["status"]["fresh"] and facts["status"]["source"] == "remote_refresh"
        actor = json.loads(request.messages[1].content)["context"]["requester_member_id"]
        chosen = next(row for row in facts["members"] if row["member_id"] == actor)
        return finish(f"能，群昵称是{chosen['name']}。", reports=[("roster", "observed")])

    model = ModelScript(
        call("read_group_context", "roster", resource="members", refresh=True), answer
    )
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(model)
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert "合成群昵称" in result.decision.response
        assert len(calls) == 1 and len(model.requests) == 2
        assert (await app.coordinator.process(request)).duplicate
        assert len(calls) == 1 and len(model.requests) == 2
        saved = app.coordinator.members.get(env.actor, env.actor.member_id)
        assert saved.room_name == "合成群昵称" and saved.confirmed_name is None
    finally:
        await app.close()


@pytest.mark.postgres
async def test_failed_refresh_does_not_claim_user_has_no_nickname(runtime_env):
    env = runtime_env
    app, _, _ = setup_chat(env)

    async def refresh(*args, **kwargs):
        raise DomainError("roster_refresh_timeout", "Synthetic timeout")

    env.channel.refresh_members = refresh
    request = event(env, "Pot，请检查我的群昵称")

    def explain(request):
        receipt = tool_receipt(request)
        status = receipt["facts"]["status"]
        assert not status["fresh"] and status["error_code"] == "roster_refresh_timeout"
        return finish("这次刷新没有成功，暂时无法核对。", reports=[("roster", "observed")])

    model = ModelScript(
        call("read_group_context", "roster", resource="members", refresh=True), explain
    )
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(model)
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert "没有成功" in result.decision.response
        assert "没有设置" not in result.decision.response and len(model.requests) == 2
        assert result.work_id is None
    finally:
        await app.close()


@pytest.mark.postgres
async def test_roster_refresh_does_not_mask_a_concurrent_forget(runtime_env):
    env = runtime_env
    app, _, _ = setup_chat(env)
    old = event(env, "合成资料，需要撤回。")
    await intake(app, env, old)

    async def refresh(group_id, **kwargs):
        app.coordinator.memory.forget(env.actor, event_ids=[old.id])
        env.channel.members_fixture = (
            MemberRecord(
                group_id=group_id,
                member_id=env.actor.member_id,
                room_display_name="刷新后的群昵称",
                membership_state="present",
                name_source="roomDisplayName",
            ),
        )

    env.channel.refresh_members = refresh
    model = ModelScript(call("read_group_context", "roster", resource="members", refresh=True))
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(model)
    request = event(env, "Pot，请刷新我的群昵称。")
    try:
        await intake(app, env, request)
        with pytest.raises(DomainError) as error:
            await app.coordinator.process(request)
        assert error.value.code == "memory_changed" and len(model.requests) == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_decision(request.id) is None and not tx.list_delivery()
    finally:
        await app.close()


@pytest.mark.postgres
async def test_group_change_reads_cache_once_without_remote_refresh(runtime_env):
    env = runtime_env
    app, _model, _ = setup_chat(env)
    env.channel.members_fixture = (
        MemberRecord(
            group_id=env.actor.group_id,
            member_id=env.actor.member_id,
            room_display_name="事件更新昵称",
            membership_state="present",
            name_source="roomDisplayName",
        ),
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("Group update must not POST a remote refresh")

    env.channel.refresh_members = forbidden
    try:
        app.roster.changed((env.channel._cursor(1),))
        await app.roster.sync_pending()
        before = app.coordinator.members.get(env.actor, env.actor.member_id)
        await app.roster.sync_pending()
        after = app.coordinator.members.get(env.actor, env.actor.member_id)
        assert before.room_name == "事件更新昵称" and after.revision == before.revision
    finally:
        await app.close()
