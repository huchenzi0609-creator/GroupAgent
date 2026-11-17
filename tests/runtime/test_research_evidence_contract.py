"""Bounded real-client source evidence; PG control flow uses an explicit critic.

The tests establish evidence visibility and pass/fail handling, not the semantic
reliability of a live model. No live API or Docker is called.
"""

import json

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.execution import ModelReply, TokenUsage
from groupagent.runtime.tool_profiles import build_tool_profile
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
    returned = sum(len(row["received_content_excerpt"].encode("utf-8")) for row in evidence)
    assert 11_000 < returned <= 12_000
    by_url = {row["url"]: row for row in evidence}
    large = by_url[actual[0]["url"]]
    small = by_url[actual[1]["url"]]
    assert large["omitted_received_content"] and not large["excerpt_complete_for_received_content"]
    assert small["received_content_excerpt"] == "later"
    assert small["excerpt_complete_for_received_content"] and not small["omitted_received_content"]


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


@pytest.mark.parametrize("search_first", [False, True])
def test_same_url_and_body_keep_fetch_provenance_in_either_receipt_order(search_first):
    searched = source("https://example.com/shared", "Identical received text", operation="search")
    fetched = source("https://example.com/shared", "Identical received text", operation="fetch")
    fetched["citation"] = "Actual fetched-page citation with its own retrieval time"
    ordered = [searched, fetched] if search_first else [fetched, searched]
    evidence = _research_source_content(ordered, "received text", "Answer")
    assert len(evidence) == 1
    assert evidence[0]["operation"] == "fetch"
    assert evidence[0]["evidence_kind"] == "provider_fetched_page"
    assert evidence[0]["citation"] == fetched["citation"]


def test_changed_received_body_at_same_url_keeps_distinct_source_evidence():
    first = source("https://example.com/versioned", "The earlier received fact.")
    second = source("https://example.com/versioned", "The corrected received fact.")
    evidence = _research_source_content([first, second], "received fact", "Answer")
    assert len(evidence) == 2
    assert {row["received_content_excerpt"] for row in evidence} == {
        first["content"],
        second["content"],
    }


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
        # Bulk evidence belongs to the separately bounded body projection, not
        # a second JSON-prefix copy that changes when persisted keys reorder.
        observation = payload["tool_evidence"][0]["output"]
        manifest = json.loads(observation)
        assert len(observation) <= 5000 and late not in observation
        assert "content" not in manifest["sources"][0]
        assert manifest["sources"][0]["evidence_ref"] == fetched.output["source"]["evidence_ref"]
        body = payload["research_source_content"]
        assert len(body) == 1 and late in body[0]["received_content_excerpt"]
        assert body[0]["url"] == fetched.output["source"]["url"]
        assert body[0]["citation"] == fetched.output["source"]["citation"]
        assert body[0]["excerpt_complete_for_received_content"] is True
        assert sum(len(row["received_content_excerpt"].encode("utf-8")) for row in body) <= 12_000
        assert payload["original_request"]["text"] == text
        assert payload["answer"] == answer
        guidance = request.messages[0].content
        assert "research_source_content" in guidance
        assert all(concept in guidance for concept in ("建议", "必需", "合成", "演算"))
        return ModelReply(
            text=json.dumps(
                {
                    "content_passed": not extra_required,
                    "group_reply_passed": True,
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
    profile = build_tool_profile(run.prepared.request.visible_tools)
    assert profile.families == ("discovery", "research")
    assert set(profile.instructions) <= set(instructions)
