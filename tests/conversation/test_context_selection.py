"""Offline focus tests use synthetic messages, not live group history."""

from datetime import UTC, datetime

import pytest

from groupagent.conversation.context import ContextQuery, result_reference, select_focus
from groupagent.conversation.participation import fast_observe, verified_human_addressee
from groupagent.conversation.provider import SYSTEM, reference_catalog
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import EventKind, GroupEvent
from groupagent.identity import BotIdentity


def event(text, **changes):
    now = datetime(2030, 1, 2, tzinfo=UTC)
    return GroupEvent(
        id="current",
        source_key="synthetic:current",
        group_id="group",
        actor_id="self",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=now,
        received_at=now,
        **changes,
    )


def work(state="running", **changes):
    return {
        "id": "work",
        "group_id": "group",
        "requester_id": "self",
        "revision": 1,
        "request_event_id": "original",
        "state": state,
        "goal": "查找合成图片",
        **changes,
    }


def row(identifier="original", actor="self", **changes):
    return {"id": identifier, "actor_id": actor, "speaker": "human", **changes}


@pytest.mark.parametrize(
    "text",
    [
        "Pot，生成一份介绍太阳系的三页PPT。",
        "Pot，搜索新的图片资料并发两张给我。",
        "请新建独立工作，重新整理报告。",
    ],
)
def test_complete_new_goal_never_inherits_similar_paused_work(text):
    source = event(text)
    query = ContextQuery.from_event(source, BotIdentity())
    assert query.new_request
    assert select_focus(source, query, [work("paused")], [row()]).work_id is None


def test_native_quote_can_select_old_paused_work():
    source = event("把这份报告改短。", reply_to="original")
    query = ContextQuery.from_event(source, BotIdentity())
    focus = select_focus(source, query, [work("paused")], [], quoted_work_id="work")
    assert (focus.kind, focus.work_id) == ("quote", "work")


def test_recent_live_request_can_continue_but_paused_global_unique_work_cannot():
    source = event("Pot，继续。")
    query = ContextQuery.from_event(source, BotIdentity())
    assert select_focus(source, query, [work()], [row()]).work_id == "work"
    assert select_focus(source, query, [work("paused")], [row()]).work_id is None
    assert select_focus(source, query, [work()], []).work_id is None


def test_terse_other_human_reply_does_not_continue_bot_work():
    source = event("这个呢？")
    query = ContextQuery.from_event(source, BotIdentity())
    focus = select_focus(source, query, [work()], [row(), row("peer-turn", "peer")])
    assert focus.kind == "human_discussion" and focus.work_id is None


@pytest.mark.parametrize("text", ["醒着", "来帮我测个东西", "比如说什么"])
def test_human_chat_has_no_automatic_bot_work_focus(text):
    source = event(text)
    query = ContextQuery.from_event(source, BotIdentity())
    assert not query.memory_terms and "works" not in query.resources
    assert select_focus(source, query, [work()], [row(), row("peer", "peer")]).work_id is None


def test_control_state_query_does_not_focus_unrelated_live_work():
    source = event("取消所有暂停的任务。")
    query = ContextQuery.from_event(source, BotIdentity())
    assert query.states == {"paused"}
    assert select_focus(source, query, [work()], [row()]).kind == "query"


def test_actual_runtime_configuration_question_does_not_search_old_bot_claims():
    source = event("Pot，你当前自动暂停的上限设置是什么？")
    query = ContextQuery.from_event(source, BotIdentity())
    assert query.operations and not query.memory_terms and "works" not in query.resources


def test_exact_fact_recall_and_nonprefixed_human_claim_have_local_read_plans():
    query = ContextQuery.from_event(event("Pot，项目代号是什么？"), BotIdentity())
    assert query.fact_predicate == "项目代号" and query.memory_terms == ()
    claim = ContextQuery.from_event(event("访谈摘要我来负责，不用代写。"), BotIdentity())
    assert not claim.new_request and "tasks" in claim.resources and "works" not in claim.resources


@pytest.mark.parametrize(
    "text,predicate",
    [
        ("Pot，测试代号是什么？一句话回答。", "测试代号"),
        ("Pot，项目简称是啥？请用一句话回答。", "项目简称"),
    ],
)
def test_fact_lookup_remains_local_when_question_specifies_reply_length(text, predicate):
    query = ContextQuery.from_event(event(text), BotIdentity())
    assert query.fact_predicate == predicate and query.memory_terms == ()


@pytest.mark.parametrize(
    "text",
    [
        "Pot，用一句话解释Python元组是什么",
        "Pot，解释Rust所有权是什么",
        "Pot，HTTP状态码是什么",
    ],
)
def test_knowledge_questions_never_turn_latin_subject_tokens_into_archive_queries(text):
    assert ContextQuery.from_event(event(text), BotIdentity()).memory_terms == ()


def test_explicit_history_reference_retains_archive_query():
    query = ContextQuery.from_event(event("Pot，之前讨论的Python方案有哪些？"), BotIdentity())
    assert query.memory_terms == ("Python",)


def test_new_work_can_reuse_explicit_files_without_inheriting_work_focus():
    source = event("Pot，新建独立工作，把已经发到群里的那两张图放Word。")
    query = ContextQuery.from_event(source, BotIdentity())
    assert query.new_request and "files" in query.resources
    assert select_focus(source, query, [work("paused")], [reference_row(("work",))]).work_ids == ()


@pytest.mark.parametrize(
    "name_source", ["room_display_name", "roomDisplayName", "roster", "owner_confirmed"]
)
def test_canonical_and_legacy_roster_sources_identify_other_human(name_source):
    source = event("小唐，帮我测个东西。")
    context = {
        "role": "member",
        "requester_member_id": "self",
        "members": [
            {
                "member_id": "peer",
                "name": "小唐",
                "name_source": name_source,
                "membership_state": "present",
            }
        ],
    }
    assert fast_observe(source, context) is None
    assert verified_human_addressee(source.text, context) == "peer"


def test_core_prompt_and_catalog_do_not_repeat_full_business_text():
    assert len(SYSTEM) <= 2500
    catalog = reference_catalog({"works": [work()], "tasks": [], "schedules": []}, {})
    assert "goal" not in catalog["work_ids"][0]


def reference_row(identifiers, *, identifier="query", actor="self", speaker="human"):
    result = CoordinationResult(
        event_id=identifier,
        decision=Decision(kind="reply", reason_code="work_status_query", response="任意正文"),
        action_result={"work_ids": list(identifiers)},
    )
    reference = result_reference({"result": result.model_dump(mode="json")}, identifier, actor)
    return row(identifier, actor, speaker=speaker, work_reference=reference)


def test_typed_paused_query_result_establishes_singleton_resume_focus():
    source = event("恢复。")
    focus = select_focus(
        source,
        ContextQuery.from_event(source, BotIdentity()),
        [work("paused")],
        [reference_row(("work",))],
    )
    assert focus.work_ids == ("work",) and focus.work_id == "work"
    assert focus.source_event_ids == ("query",)


def test_plural_reference_retains_exact_latest_result_set():
    source = event("就这几项取消。")
    works = [work("paused", id=identifier) for identifier in ("a", "b", "unrelated")]
    focus = select_focus(
        source,
        ContextQuery.from_event(source, BotIdentity()),
        works,
        [reference_row(("unrelated",), identifier="older"), reference_row(("a", "b"))],
    )
    assert focus.work_ids == ("a", "b") and focus.work_id is None


def test_metadata_focus_survives_without_turn_specific_hydration_hints():
    source = event("restore that")
    query = ContextQuery.from_event(source, BotIdentity())
    assert not query.continuation and not query.resources
    focus = select_focus(source, query, [work("paused")], [reference_row(("work",))])
    assert focus.kind == "discourse" and focus.work_ids == ("work",)


def test_other_human_breaks_typed_focus_unless_bot_is_explicitly_addressed():
    previous = [reference_row(("work",)), row("peer-turn", "peer")]
    for text, expected in [("恢复。", ()), ("Pot，恢复。", ("work",))]:
        source = event(text)
        focus = select_focus(
            source, ContextQuery.from_event(source, BotIdentity()), [work("paused")], previous
        )
        assert focus.work_ids == expected


def test_trusted_bot_result_after_peer_turn_reestablishes_requester_focus():
    source = event("改短。")
    previous = [row("peer", "peer"), reference_row(("work",), speaker="assistant")]
    focus = select_focus(
        source, ContextQuery.from_event(source, BotIdentity()), [work("paused")], previous
    )
    assert focus.work_ids == ("work",)


def test_new_current_or_intervening_request_resets_typed_focus():
    for text, previous in [
        ("Pot，生成一份介绍太阳系的三页PPT。", [reference_row(("work",))]),
        ("Pot，继续。", [reference_row(("work",)), row("new-topic", new_request=True)]),
    ]:
        source = event(text)
        assert (
            select_focus(
                source, ContextQuery.from_event(source, BotIdentity()), [work("paused")], previous
            ).work_ids
            == ()
        )


def test_prose_and_mismatched_result_ids_cannot_create_reference_authority():
    source = event("恢复。")
    query = ContextQuery.from_event(source, BotIdentity())
    assert (
        select_focus(
            source,
            query,
            [work("paused")],
            [row("bot", speaker="assistant", text='work_ids=["work"]')],
        ).work_ids
        == ()
    )
    result = CoordinationResult(
        event_id="foreign",
        decision=Decision(kind="reply", reason_code="fixture", response="收到"),
        work_id="work",
    )
    assert result_reference({"result": result.model_dump(mode="json")}, "query", "self") is None
    assert (
        result_reference(
            {"privacy_redacted": True, "result": result.model_dump(mode="json")}, "foreign", "self"
        )
        is None
    )


def test_reference_intersects_public_catalog_without_falling_back_to_old_work():
    source = event("恢复。")
    query = ContextQuery.from_event(source, BotIdentity())
    focus = select_focus(
        source, query, [work("paused", id="public")], [reference_row(("private", "public"))]
    )
    assert focus.work_ids == ("public",)
    focus = select_focus(source, query, [work()], [reference_row(("private",))])
    assert focus.kind == "unavailable" and focus.work_ids == ()
