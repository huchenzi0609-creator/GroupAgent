"""Research response units: pure guards plus PG/actual client MockTransport.

No live provider or Docker is used. Metadata comes through the real web_fetch
adapter and its client-side UTF-8 truncation, not a hand-shaped source receipt.
"""

import json
import time

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.runtime.tool_profiles import build_tool_profile
from groupagent.runtime.toolkit import (
    _has_internal_research_metrics,
    _research_metrics_requested,
    _research_source_metrics,
)

from .test_toolkit_contract import call, candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup


@pytest.mark.parametrize(
    "text",
    [
        "该条目位于我本次读取到的12000字符截断内容之后。",
        "本次工具仅抓取到8000字节的正文。",
        "我收到的正文长度为4500个字符。",
        "The tool returned 15,000 bytes.",
        "I read 3200 characters.",
        "The fetched page was truncated to 2500 tokens.",
        "The retrieved content was limited to 9000 Unicode codepoints.",
        "本次读取到的12000 UTF-8 bytes只覆盖部分正文。",
        "content_bytes_returned=7000",
    ],
)
def test_unrequested_internal_read_measurements_are_detected(text):
    assert _has_internal_research_metrics(text)


@pytest.mark.parametrize(
    "text",
    [
        "sorted返回新列表，list.sort原地修改列表并返回None。",
        "UTF-8中ASCII字符通常占1字节，部分汉字占3字节。",
        'len("abc")返回3个字符。',
        "The code reads 4 bytes per block.",
        "The model supports a context window of 32000 tokens.",
        "访问时间UTC：2026-11-15T01:30:00Z。",
        "该页正文被截断，仅依据实际收到的段落说明，未读取省略部分。",
        "总预算1000元，报告共3页。",
    ],
)
def test_domain_quantities_time_and_qualitative_limits_remain_valid(text):
    assert not _has_internal_research_metrics(text)


def test_long_unrelated_digit_run_does_not_stall_research_delivery():
    text = "An unrelated identifier: " + "1" * 10000
    started = time.monotonic()
    assert not _has_internal_research_metrics(text)
    assert _has_internal_research_metrics(text + "\nThe tool returned 8000 bytes.")
    # The old unbounded pattern took over six seconds for one of these scans.
    # The bounded scan has ample headroom; this is not a provider latency SLO.
    assert time.monotonic() - started < 2.0


@pytest.mark.parametrize(
    "quotation",
    ["‘I read 3200 characters.’", '"I read 3200 characters."', "`I read 3200 characters.`"],
)
def test_exact_requested_quotations_are_data_but_other_operation_claims_are_not(quotation):
    request = f"请查阅公开资料，原样引用我给出的句子{quotation}并给来源。"
    answer = f"原句：{quotation}"
    assert not _has_internal_research_metrics(answer, request_text=request)
    assert _has_internal_research_metrics(
        answer + "\nThe tool returned 15000 bytes.", request_text=request
    )
    assert _has_internal_research_metrics(answer, request_text="请查阅并给出引用来源。")


def test_translation_uses_semantic_review_without_converting_data_into_operation_claims():
    request = "请联网查阅资料，把‘I read 3200 characters.’翻译成中文。"
    answer = "原句：I read 3200 characters. 译文：我读取了3200个字符。"
    assert not _has_internal_research_metrics(answer, request_text=request)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("请检索官方资料，简短说明差别，给链接和访问时间。", False),
        ("请解释UTF-8文章中的概念。", False),
        ("解释代码每次读取4字节的含义。", False),
        ("请统计本次抓取返回正文的UTF-8字节数和Unicode字符数。", True),
        ("Report byte size and Unicode character count of the fetched response.", True),
        ("How many tokens did this tool return?", True),
        ("请核对UTF-8编码的字节数与字符数。", True),
        ("不要报告本次抓取的字节数，只说明结论。", False),
        ("请查阅资料。不需要统计工具返回的字符数。", False),
        ("Do not report the byte count of the fetched response; explain the facts.", False),
        ("Answer without reporting the character count of the fetched response.", False),
        ("不要混淆字节和字符，请分别统计本次抓取正文的两种长度。", True),
        ("Don't confuse bytes and characters; report both counts of the fetched response.", True),
        ("抓取这个网页。请告诉我实际返回的字节数。", True),
        ("不要报告本次抓取的字节数，但请提供字符数。", True),
    ],
)
def test_only_explicit_measurement_requests_open_the_diagnostic_exception(text, expected):
    assert _research_metrics_requested(text) is expected


def test_source_metrics_do_not_invent_missing_content_counts_or_token_usage():
    result = _research_source_metrics({"url": "https://example.com/", "content_bytes_returned": 8})
    assert result["receipt_content_bytes_returned"] == 8
    assert "observed_content_unicode_codepoints" not in result
    assert not any("token" in key for key in result)


@pytest.mark.postgres
@pytest.mark.parametrize(
    "text",
    [
        "请检索公开资料，简短说明操作差别，提供实际链接和访问时间。",
        "请查阅公开资料并给实际访问时间。不要报告本次抓取的字节数或字符数，只说明结论。",
    ],
)
@pytest.mark.parametrize(
    "measurement",
    [
        "该条目位于我本次读取到的12000字符截断内容之后。",
        "The tool returned 15000 bytes.",
        "The fetched body was truncated to 4000 tokens.",
    ],
)
async def test_unsolicited_internal_metrics_fail_before_critic_or_delivery(
    toolkit_setup, measurement, text
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"research_required": True}, goal=text, request_text=text)
    fetched = await call(run, "web_fetch", {"url": "https://example.com/source"})
    assert fetched.success and setup.model.passed is True
    answer = measurement + "\n" + fetched.output["source"]["citation"]
    with pytest.raises(DomainError, match="unrequested internal retrieval measurements") as failed:
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, answer, (fetched,)), run.workspace
        )
    assert failed.value.code == "validation_failed" and not setup.model.requests
    with setup.env.store.connect() as conn:
        deliveries = conn.execute(
            "SELECT count(*) AS n FROM ga_deliveries WHERE group_id=%s AND work_id=%s",
            (setup.env.actor.group_id, run.work.id),
        ).fetchone()["n"]
        critic_calls = conn.execute(
            "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s AND operation='text'",
            (setup.env.actor.group_id, run.work.id),
        ).fetchone()["n"]
    assert deliveries == critic_calls == 0


@pytest.mark.postgres
@pytest.mark.parametrize("diagnostic", [False, True])
async def test_long_actual_fetch_retains_separate_byte_and_codepoint_evidence(
    toolkit_setup, monkeypatch, diagnostic
):
    setup = toolkit_setup
    raw_content = "A" * 5500 + "你" * 1000 + " tail"

    def respond(request):
        return httpx.Response(200, json={"title": "Synthetic long source", "content": raw_content})

    client = setup.toolkit.research
    monkeypatch.setattr(client._client, "_transport", httpx.MockTransport(respond))
    monkeypatch.setattr(client, "_fetch_content_bytes", 6000)
    text = "请查阅公开资料，简短说明内容并给实际访问时间。"
    if diagnostic:
        text += "请统计本次工具抓取正文的UTF-8字节数和Unicode字符数。"
    run = await prepare(setup, scope={"research_required": True}, goal=text, request_text=text)
    fetched = await call(run, "web_fetch", {"url": "https://example.com/source"})
    assert fetched.success
    source = fetched.output["source"]
    content = source["content"]
    assert len(content) > 5000 and len(content) != len(content.encode("utf-8"))
    assert source["content_bytes_returned"] == len(content.encode("utf-8"))
    assert source["content_bytes_received"] == len(raw_content.encode("utf-8"))
    assert source["truncated_fields"] == ["content"]
    answer = "正文受截断限制，只依据实际收到部分说明。"
    if diagnostic:
        answer += (
            f"本次工具返回{source['content_bytes_returned']}字节；"
            f"我本次读取到的{len(content)}字符按Unicode码点计数。"
        )
    answer += "\n" + source["citation"]
    accepted = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, answer, (fetched,)), run.workspace
    )
    assert accepted.checks["result"] and len(setup.model.requests) == 1
    review = setup.model.requests[0]
    payload = json.loads(review.messages[1].content)
    narrative = payload["tool_evidence"][0]["output"]
    assert 0 < len(narrative) <= 5000
    manifest = json.loads(narrative)
    assert len(manifest["sources"]) == 1
    recorded_source = manifest["sources"][0]
    assert recorded_source["source_id"] == source["source_id"]
    assert recorded_source["citation"] == source["citation"]
    assert recorded_source["content_sha256"] == source["content_sha256"]
    assert recorded_source["evidence_ref"] == source["evidence_ref"]
    assert "content" not in recorded_source and "content_bytes_returned" not in narrative
    body = payload["research_source_content"]
    assert len(body) == 1 and body[0]["received_content_excerpt"] == raw_content
    assert body[0]["excerpt_complete_for_received_content"]
    assert not body[0]["omitted_received_content"]
    metrics = payload["research_source_metrics"]
    assert len(metrics) == 1
    actual = metrics[0]
    assert actual["tool"] == "web_fetch" and actual["call_id"] == fetched.call_id
    assert actual["url"] == source["url"] and actual["citation"] == source["citation"]
    assert actual["receipt_content_bytes_returned"] == len(content.encode("utf-8"))
    assert actual["receipt_content_bytes_received"] == len(raw_content.encode("utf-8"))
    assert actual["observed_content_utf8_bytes"] == len(content.encode("utf-8"))
    assert actual["observed_content_unicode_codepoints"] == len(content)
    assert actual["truncated_fields"] == ["content"]
    guidance = review.messages[0].content
    assert all(
        name in guidance
        for name in (
            "UTF-8",
            "receipt_content_bytes_",
            "observed_content_unicode_codepoints",
        )
    )
    instructions = json.loads(run.prepared.request.objective)["instructions"]
    profile = build_tool_profile(run.prepared.request.allowed_tools)
    assert profile.families == ("research",)
    assert set(profile.instructions) <= set(instructions)


@pytest.mark.postgres
@pytest.mark.parametrize("translation", [False, True])
async def test_requested_literal_and_translation_reach_semantic_review_unchanged(
    toolkit_setup, translation
):
    setup = toolkit_setup
    request = "请查阅公开资料解释计量，原样引用我给出的句子‘I read 3200 characters.’并给实际来源和访问时间。"
    if translation:
        request += "请把这句翻译成中文。"
    run = await prepare(
        setup, scope={"research_required": True}, goal=request, request_text=request
    )
    fetched = await call(run, "web_fetch", {"url": "https://example.com/source"})
    assert fetched.success
    answer = "你提供的原句：‘I read 3200 characters.’"
    if translation:
        answer += " 译文：我读取了3200个字符。"
    answer += "\n" + fetched.output["source"]["citation"]
    accepted = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, answer, (fetched,)), run.workspace
    )
    assert accepted.checks["result"] and len(setup.model.requests) == 1
    payload = json.loads(setup.model.requests[-1].messages[1].content)
    assert payload["answer"] == answer and payload["research_source_metrics"]
    assert payload["original_request"]["text"] == request
    assert payload["validation_phase"] == "pre_delivery"
    assert payload["research_source_content"]
    setup.model.passed = False
    bad_answer = answer + "\nThe tool returned 15000 bytes."
    with pytest.raises(DomainError):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, bad_answer, (fetched,)),
            run.workspace,
        )
    if translation:
        assert len(setup.model.requests) == 2
        assert json.loads(setup.model.requests[-1].messages[1].content)["answer"] == bad_answer
    else:
        assert len(setup.model.requests) == 1
