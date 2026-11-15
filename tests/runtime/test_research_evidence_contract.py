"""Bounded real-client source evidence; PG control flow uses an explicit critic.

The tests establish evidence visibility and pass/fail handling, not the semantic
reliability of a live model. No live API or Docker is called.
"""

import json

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.execution import ModelReply, TokenUsage
from groupagent.runtime.toolkit import _research_source_content

from .test_toolkit_contract import call, candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup


def source(url, content, *, operation="fetch", truncated=()):
    return {
        "source_id": url,
        "url": url,
        "citation": url + " (actual retrieval fixture)",
        "operation": operation,
        "evidence_kind": "provider_fetched_page" if operation == "fetch" else "search_result",
        "content": content,
        "truncated_fields": list(truncated),
    }


def test_bounded_content_keeps_late_received_evidence_and_truthful_limits():
    late = "The recommendation is optional; other implementations are supported."
    raw = "Preface. " * 700 + late
    actual = source("https://example.com/reference", raw, truncated=["content"])
    evidence = _research_source_content(
        [actual], "Explain implementations", "An implementation is optional"
    )
    assert len(evidence) == 1 and late in evidence[0]["received_content_excerpt"]
    assert evidence[0]["excerpt_complete_for_received_content"] is True
    assert evidence[0]["omitted_received_content"] is False
    assert evidence[0]["source_truncated_fields"] == ["content"]
    assert evidence[0]["url"] == actual["url"]


def test_total_content_budget_is_utf8_bounded_and_omissions_are_explicit():
    actual = [
        source("https://example.com/large", "你" * 5000),
        source("https://example.com/other", "later"),
    ]
    evidence = _research_source_content(actual, "Question", "Answer")
    assert sum(len(row["received_content_excerpt"].encode("utf-8")) for row in evidence) <= 12_000
    assert all(row["omitted_received_content"] for row in evidence)
    assert not any(row["excerpt_complete_for_received_content"] for row in evidence)
    assert evidence[1]["received_content_excerpt"] == ""


def test_actual_fetch_and_question_identifiers_get_priority_without_duplicate_bodies():
    other = source("https://example.com/unrelated", "Other material " * 1000)
    relevant = source("https://example.com/target", "target_operation returns text")
    search = source("https://example.com/search", "target_operation " * 1000, operation="search")
    evidence = _research_source_content(
        [other, search, relevant, relevant], "target_operation", "text"
    )
    assert evidence[0]["url"] == relevant["url"]
    assert len(evidence) == 3
    assert (
        next(row for row in evidence if row["url"] == search["url"])["evidence_kind"]
        == "search_result"
    )


def test_same_url_and_body_keep_fetch_provenance_when_search_arrives_first():
    searched = source("https://example.com/shared", "Identical received text", operation="search")
    fetched = source("https://example.com/shared", "Identical received text", operation="fetch")
    evidence = _research_source_content([searched, fetched], "received text", "Answer")
    assert len(evidence) == 1
    assert evidence[0]["operation"] == "fetch"
    assert evidence[0]["evidence_kind"] == "provider_fetched_page"


@pytest.mark.postgres
@pytest.mark.parametrize("extra_required", [False, True])
async def test_late_actual_source_reaches_critic_and_optional_is_not_required(
    toolkit_setup, monkeypatch, extra_required
):
    setup = toolkit_setup
    late = (
        "A text sink accepts the returned text value. Format-R is a recommendation, "
        "not a requirement; other representations are supported."
    )
    raw = "Reference preface and navigation. " * 190 + "\n\n" + late
    assert raw.index(late) > 5000

    def respond(request):
        return httpx.Response(200, json={"title": "Synthetic operation reference", "content": raw})

    monkeypatch.setattr(setup.toolkit.research._client, "_transport", httpx.MockTransport(respond))
    text = "请查阅公开资料，简短说明文本输出操作的能力，提供来源和访问时间。"
    run = await prepare(setup, scope={"research_required": True}, goal=text, request_text=text)
    fetched = await call(run, "web_fetch", {"url": "https://example.com/reference"})
    assert fetched.success and fetched.output["source"]["content"] == raw
    answer = "该操作返回文本值。"
    answer += (
        "写入时唯一必需使用Format-R。"
        if extra_required
        else "Format-R只是一种建议，资料允许其它形式。"
    )
    answer += "\n" + fetched.output["source"]["citation"]
    seen = []

    async def explicit_evidence_critic(request):
        seen.append(request)
        payload = json.loads(request.messages[1].content)
        # This is the old evidence gap: the actual required paragraph is not in
        # the first 5000 JSON characters despite being in the received body.
        assert late not in payload["tool_evidence"][0]["output"]
        body = payload["research_source_content"]
        assert len(body) == 1 and late in body[0]["received_content_excerpt"]
        assert body[0]["url"] == fetched.output["source"]["url"]
        assert body[0]["citation"] == fetched.output["source"]["citation"]
        assert body[0]["excerpt_complete_for_received_content"] is True
        assert sum(len(row["received_content_excerpt"].encode("utf-8")) for row in body) <= 12_000
        instruction = request.messages[0].content
        assert "默认值、推荐或网页示例不能推导为唯一必需" in instruction
        assert "用户要求的合成示例字面值" in instruction
        assert "不要求在网页出现相同字面值" in instruction
        return ModelReply(
            text=json.dumps(
                {
                    "passed": not extra_required,
                    "reason": "Actual source says optional, not uniquely required",
                }
            ),
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", explicit_evidence_critic)
    if extra_required:
        with pytest.raises(DomainError, match="Actual source says optional"):
            await setup.toolkit.validate(
                run.work,
                run.lease,
                setup.env.actor,
                candidate(run, answer, (fetched,)),
                run.workspace,
            )
    else:
        result = await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, answer, (fetched,)), run.workspace
        )
        assert result.checks["result"] and len(result.deliveries) == 1
    assert len(seen) == 1
    with setup.env.store.transaction(setup.env.actor) as tx:
        recorded = tx.get_work(run.work.id).scope["_runtime_semantic_reviews"][-1]
    assert recorded["passed"] is (not extra_required)
    assert recorded["accepted"] is (not extra_required)
    instructions = json.loads(run.prepared.request.objective)["instructions"]
    assert any(
        "Avoid long source quotations and unrelated operating tutorials" in item
        for item in instructions
    )
