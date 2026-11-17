"""Brief group delivery and complete evidence use one execution/critic path.

PostgreSQL and the real research adapter are used with synthetic source bodies
and explicit critic fixtures. No live model or provider acceptance is claimed.
"""

import json

import httpx
import pytest

from groupagent.delivery import group_delivery_policy, group_reply_instruction
from groupagent.domain import DomainError
from groupagent.execution import ModelReply, TokenUsage
from groupagent.research import validate_research_citations

from .test_toolkit_contract import call, candidate, create_document, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

URL = "https://example.com/service-reference"
SOURCE = "The service uses metered billing. This public reference does not publish exact fees."
ANSWER = "公开资料提到按用量计费，具体价格没有写明。"


def test_shared_policy_keeps_explicit_requirements_separate_from_display_defaults():
    policy = group_delivery_policy("one_to_three_short_sentences")
    assert policy["instruction"] == group_reply_instruction("one_to_three_short_sentences")
    assert policy["short_reply_style"] == "one_to_three_short_sentences"
    assert policy["retrieval_time_display_default"] == "internal_evidence_only"
    assert set(policy["requirement_sources"]) >= {
        "human_requirements.requests",
        "human_requirements.required_outputs",
    }
    assert "不做字数硬截断" in policy["instruction"]
    assert "邀约" in policy["instruction"]


async def fetch_reference(setup, monkeypatch, run):
    def respond(request):
        return httpx.Response(200, json={"title": "Synthetic service reference", "content": SOURCE})

    monkeypatch.setattr(setup.toolkit.research._client, "_transport", httpx.MockTransport(respond))
    fetched = await call(run, "web_fetch", {"url": URL})
    assert fetched.success
    return fetched


@pytest.mark.postgres
@pytest.mark.parametrize("show_link", [False, True])
async def test_default_lookup_keeps_actual_provenance_internal_without_a_visible_bibliography(
    toolkit_setup, monkeypatch, show_link
):
    setup = toolkit_setup
    text = "帮我查一下这项服务怎么计费。"
    run = await prepare(
        setup, goal="Look up service billing", request_text=text, scope={"research_required": True}
    )
    objective = json.loads(run.prepared.request.objective)
    fetched = await fetch_reference(setup, monkeypatch, run)
    source = fetched.output["source"]
    answer = ANSWER + ("\n" + URL if show_link else "")
    requests = []

    async def critic(request):
        requests.append(request)
        payload = json.loads(request.messages[1].content)
        assert payload["answer"] == answer
        assert payload["human_requirements"]["requests"][0]["text"] == text
        assert payload["delivery_policy"] == objective["delivery_policy"]
        assert objective["delivery_policy"]["instruction"] in objective["instructions"]
        assert payload["human_requirements"]["required_outputs"] == objective["required_outputs"]
        metrics = payload["research_source_metrics"][0]
        assert metrics["retrieved_at"] == source["retrieved_at"]
        assert metrics["citation"] == source["citation"]
        assert payload["research_source_content"][0]["received_content_excerpt"] == SOURCE
        manifest = json.loads(payload["tool_evidence"][0]["output"])["sources"][0]
        assert manifest["citation"] == source["citation"]
        assert manifest["retrieved_at"] == source["retrieved_at"]
        assert manifest["evidence_ref"] == source["evidence_ref"]
        return ModelReply(
            text='{"content_passed":true,"group_reply_passed":true,"reason":"Synthetic source supports unknown exact fees"}',
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", critic)
    accepted = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, answer, (fetched,)),
        run.workspace,
    )
    assert accepted.deliveries[0].payload_ref == "text:" + answer
    assert len(requests) == 1
    assert source["citation"] not in accepted.deliveries[0].payload_ref


@pytest.mark.postgres
@pytest.mark.parametrize("include_time", [False, True])
async def test_explicit_time_requirement_reaches_critic_even_when_goal_is_abbreviated(
    toolkit_setup, monkeypatch, include_time
):
    setup = toolkit_setup
    text = "帮我查计费规则，给出实际来源链接和访问时间。"
    run = await prepare(
        setup,
        goal="Look up service billing",
        request_text=text,
        scope={"research_required": True, "outputs": [{"format": "text"}]},
    )
    fetched = await fetch_reference(setup, monkeypatch, run)
    source = fetched.output["source"]
    answer = ANSWER + "\n" + (source["citation"] if include_time else URL)
    seen = []

    async def explicit_time_critic(request):
        seen.append(request)
        payload = json.loads(request.messages[1].content)
        assert payload["human_requirements"]["requests"][0]["text"] == text
        assert payload["human_requirements"]["required_outputs"] == [{"format": "text"}]
        assert "真人明确要求的细节、引用和访问时间" in request.messages[0].content
        # The fixture declares the human's explicit requirement; it does not
        # pretend an offline keyword parser establishes semantic reliability.
        try:
            validate_research_citations(payload["answer"], [source], require_access_time=True)
            passed = True
        except DomainError:
            passed = False
        return ModelReply(
            text=json.dumps(
                {
                    "content_passed": passed,
                    "group_reply_passed": True,
                    "reason": "Explicit requested time checked",
                }
            ),
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", explicit_time_critic)
    if include_time:
        accepted = await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, answer, (fetched,)),
            run.workspace,
        )
        assert accepted.deliveries[0].payload_ref == "text:" + answer
    else:
        with pytest.raises(DomainError, match="Explicit requested time checked"):
            await setup.toolkit.validate(
                run.work,
                run.lease,
                setup.env.actor,
                candidate(run, answer, (fetched,)),
                run.workspace,
            )
    assert len(seen) == 1


@pytest.mark.postgres
@pytest.mark.parametrize(
    "extra,reason",
    [
        ("\nhttps://invented.example.com/pricing", "no successful research provenance"),
        ("\n" + URL + "（访问时间 UTC：2099-01-01T00:00:00Z）", "displayed retrieval time"),
    ],
)
async def test_brief_delivery_still_rejects_fabricated_links_and_displayed_times_before_critic(
    toolkit_setup, monkeypatch, extra, reason
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"research_required": True})
    fetched = await fetch_reference(setup, monkeypatch, run)
    with pytest.raises(DomainError, match=reason):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, ANSWER + extra, (fetched,)),
            run.workspace,
        )
    assert not setup.model.requests


@pytest.mark.postgres
async def test_brief_message_cannot_replace_unknown_fees_with_an_unsupported_price(
    toolkit_setup, monkeypatch
):
    setup = toolkit_setup
    text = "查一下这项服务的价格。"
    run = await prepare(setup, scope={"research_required": True}, goal=text, request_text=text)
    fetched = await fetch_reference(setup, monkeypatch, run)

    async def unsupported_price_critic(request):
        payload = json.loads(request.messages[1].content)
        assert payload["research_source_content"][0]["received_content_excerpt"] == SOURCE
        assert payload["answer"] == "每月99元。"
        return ModelReply(
            text='{"content_passed":false,"group_reply_passed":true,"reason":"The actual source gives no exact price"}',
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", unsupported_price_critic)
    with pytest.raises(DomainError, match="actual source gives no exact price"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "每月99元。", (fetched,)),
            run.workspace,
        )


@pytest.mark.postgres
@pytest.mark.parametrize("empty_answer", [False, True])
async def test_artifact_delivery_preserves_reviewed_requested_explanation_and_empty_fallback(
    toolkit_setup, empty_answer
):
    setup = toolkit_setup
    text = "生成Markdown报告，在群里说明结论是87。"
    run = await prepare(setup, scope={"outputs": [{"format": "md"}]}, goal=text, request_text=text)
    generated = await create_document(run)
    selected = await call(
        run, "artifact_deliver", {"version_ids": [generated.output["version_id"]]}
    )
    assert selected.success
    answer = "" if empty_answer else "报告已准备好，结论是87。"
    accepted = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, answer, (generated, selected)),
        run.workspace,
    )
    assert accepted.deliveries[0].payload_ref == "text:" + (answer or "文件已准备好。")
    assert len(setup.model.requests) == 1
