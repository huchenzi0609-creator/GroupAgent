"""Real PostgreSQL checks on authority distinct from readable model observations."""

import json

import pytest

from groupagent.domain import WorkState

from .test_group_conversation_contract_replay import DialogueModel, Replay, observation, reply, tool

pytestmark = pytest.mark.postgres


async def test_human_supplied_stable_member_reference_resolves_from_current_roster(runtime_env):
    env = runtime_env
    replay = Replay(env)
    opaque = "opaque-roster-id-0371"
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_members(group_id,member_id,membership_state) VALUES(%s,%s,'present')",
            (tx.group_id, opaque),
        )

    def choose_member(request):
        roster = observation(request)["facts"]
        missing = next(row for row in roster if row["name"] == "群昵称待补充")
        assert missing["member_id"] != opaque
        assert missing["request_reference"] == opaque
        return tool(
            "apply_business_action",
            {
                "action": {
                    "op": "confirm_name",
                    "member_id": opaque,
                    "name": "合成岚桥",
                    "expected_revision": missing["revision"],
                }
            },
        )

    def confirm(request):
        assert observation(request)["status"] == "committed"
        return reply("群名单已更新。")

    await replay.say(
        f"Pot，请把名单中的 {opaque} 补为合成岚桥。",
        DialogueModel(
            tool("read_group_context", {"resource": "members"}),
            choose_member,
            confirm,
        ),
    )
    member = replay.coordinator.members.get(env.actor, opaque)
    assert member.confirmed_name == "合成岚桥" and member.room_name is None


@pytest.mark.parametrize("explicit_reference", [False, True])
@pytest.mark.parametrize("quantifier", ["one", "all"])
async def test_reading_multiple_work_targets_does_not_authorize_a_guess(
    runtime_env, explicit_reference, quantifier
):
    env = runtime_env
    replay = Replay(env)
    ids = []
    for goal in ("整理合成材料甲", "整理合成材料乙"):
        source = replay.source(goal)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work(goal, source.id)
            work = tx.transition_work(
                work.id, WorkState.PAUSED, expected_revision=work.revision, reason="user_pause"
            )
        ids.append(work.id)

    def narrow(request):
        assert {row["id"] for row in observation(request)["facts"]} == set(ids)
        return tool(
            "control_work",
            {
                "work_selector": {
                    "basis": "ids",
                    "ids": [ids[0]],
                    "states": ["paused"],
                    "quantifier": quantifier,
                },
                "control": "cancel",
            },
        )

    def answer(request):
        observed = observation(request)
        if explicit_reference:
            assert observed["status"] == "committed"
            return reply("已停止指定的工作。")
        assert (
            observed["status"] == "rejected" and observed["code"] == "work_selection_scope_required"
        )
        return tool(
            "finish_turn",
            {"disposition": "reply", "response": "要停止哪一项暂停的工作？", "needs_input": True},
        )

    _, result, _ = await replay.say(
        "Pot，结束那件暂停的工作。" + (f"编号是 {ids[0]}。" if explicit_reference else ""),
        DialogueModel(
            tool(
                "read_work_facts",
                {"selector": {"basis": "query", "states": ["paused"], "quantifier": "all"}},
            ),
            narrow,
            answer,
        ),
    )
    actual = {work.id: work.state for work in replay.works()}
    expected = WorkState.CANCELLED if explicit_reference else WorkState.PAUSED
    assert actual == {ids[0]: expected, ids[1]: WorkState.PAUSED}
    assert (result.decision.kind == "clarify") is (not explicit_reference)


async def test_direct_native_acceptance_quote_targets_known_work(runtime_env):
    env = runtime_env
    replay = Replay(env)
    _, admission, _ = await replay.say(
        "Pot，请整理一份合成背景说明。",
        DialogueModel(
            tool(
                "start_work",
                {"goal": "整理一份合成背景说明", "finish_on_commit": True},
            )
        ),
    )
    with env.store.transaction(env.actor) as tx:
        native = tx.get_delivery(admission.delivery_id).platform_message_id
        work = tx.get_work(admission.work_id)
    assert native and work

    def quoted(request):
        context = json.loads(request.messages[1].content)["context"]
        assert context["quoted_work_id"] == work.id
        assert context["works"][0]["goal_revision"] == work.goal_revision
        return reply("这条确认对应原来的背景说明工作，目前在队列里。")

    await replay.say(
        "这项工作的实际状态是什么？",
        DialogueModel(quoted),
        reply_to=native,
    )
    assert len(replay.works()) == 1


async def test_scoped_status_reply_survives_an_interleaved_human_message(runtime_env):
    env = runtime_env
    replay = Replay(env)
    _, result, _ = await replay.say(
        "Pot，请把一份合成材料整理成可读说明。",
        DialogueModel(
            tool(
                "start_work",
                {"goal": "整理合成材料为说明", "finish_on_commit": True},
            )
        ),
    )
    await replay.say(
        "我看到了窗外的雾。",
        DialogueModel(tool("finish_turn", {"disposition": "observe"})),
        actor="synthetic-peer",
    )

    def status_answer(request):
        shown = observation(request)
        assert shown["facts"][0]["id"] == result.work_id
        assert shown["facts"][0]["state"] == "queued"
        return tool(
            "finish_turn",
            {"disposition": "reply", "response": "你的材料说明已登记，仍在排队。"},
        )

    _, followup, _ = await replay.say(
        "刚才那份说明做到哪一步了？",
        DialogueModel(
            tool("read_work_facts", {"selector": {"basis": "ids", "ids": [result.work_id]}}),
            status_answer,
        ),
    )
    assert followup.delivery_id and followup.decision.kind == "reply"
    assert len(replay.works()) == 1


async def test_verified_human_addressee_closes_wrong_reply_without_model_retry(runtime_env):
    env = runtime_env
    replay = Replay(env)
    mistaken = DialogueModel(
        tool("finish_turn", {"disposition": "reply", "response": "我来替这位同学答。"})
    )
    _, ignored, _ = await replay.say(
        "主持同学，刚才那本书是你的？",
        mistaken,
        actor="synthetic-peer",
        mentions=[env.actor.member_id],
    )
    assert len(mistaken.normal_requests) == 1
    assert ignored.decision.reason_code == "not_addressed_to_agent" and not ignored.delivery_id
    _, next_turn, _ = await replay.say(
        "Pot，帮我算一下三加五。",
        DialogueModel(reply("八。")),
        actor="synthetic-peer",
    )
    assert next_turn.delivery_id and len(replay.works()) == 0


@pytest.mark.parametrize("authorizes_bulk", [False, True])
async def test_bulk_write_needs_the_actual_author_scope(runtime_env, authorizes_bulk):
    env = runtime_env
    replay = Replay(env)
    for label in ("审阅说明", "整理笔记"):
        event = replay.source(label)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work(label, event.id)
            tx.transition_work(
                work.id, WorkState.PAUSED, expected_revision=work.revision, reason="user_pause"
            )
    phrase = "把这两项暂停的工作都取消" if authorizes_bulk else "取消之前那项暂停的工作"
    arguments = {
        "work_selector": {"basis": "query", "quantifier": "all", "states": ["paused"]},
        "control": "cancel",
    }
    if authorizes_bulk:
        arguments["scope_evidence"] = phrase

    def check(request):
        seen = observation(request)
        if authorizes_bulk:
            assert seen["status"] == "committed"
            return reply("已取消两项暂停工作。")
        assert seen["issues"] == [
            {"path": "scope_evidence", "code": "bulk_scope_needs_author_evidence"}
        ]
        return tool(
            "finish_turn",
            {"disposition": "reply", "response": "你想取消哪一项？", "needs_input": True},
        )

    _, _, _ = await replay.say(
        "Pot，" + phrase + "。", DialogueModel(tool("control_work", arguments), check)
    )
    assert {work.state for work in replay.works()} == {
        WorkState.CANCELLED if authorizes_bulk else WorkState.PAUSED
    }
