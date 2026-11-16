"""Synthetic provider failures keep fee holds without blocking independent reads."""

import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.research import DeepSeekResearchClient, register_research_tools
from groupagent.tools import EffectState, PolicyDecision, ToolContext, ToolGateway, ToolScope


class HeldFeeFixtureBudget:
    """An explicit no-charge fixture; attempted fees are never released."""

    fixture_only = True

    def __init__(self, limit=10, reserve_error=None):
        self.limit = limit
        self.reserve_error = reserve_error
        self.reservations = {}
        self.reserve_calls = []

    def reserve(self, actor, key, operation, price_id, estimated_units, **kwargs):
        self.reserve_calls.append(key)
        if self.reserve_error is not None:
            raise DomainError(self.reserve_error, "Synthetic preflight refusal")
        if key in self.reservations:
            return SimpleNamespace(id=key, is_new=False)
        if len(self.reservations) >= self.limit:
            raise DomainError("budget_exhausted", "Synthetic held fees exhaust the cap")
        assert estimated_units == {"calls": Decimal(1)}
        self.reservations[key] = "reserved"
        return SimpleNamespace(id=key, is_new=True)

    def begin(self, actor, identifier):
        assert self.reservations[identifier] == "reserved"
        self.reservations[identifier] = "inflight"
        return True

    def settle(self, actor, identifier, actual_units, evidence_ref):
        assert self.reservations[identifier] == "inflight"
        assert actual_units == {"calls": Decimal(1)} and evidence_ref.startswith("web.")
        self.reservations[identifier] = "settled"
        return SimpleNamespace(overrun=False)

    def unknown(self, actor, identifier, reason):
        assert self.reservations[identifier] == "inflight"
        self.reservations[identifier] = "unknown"


async def public(host):
    return ("93.184.216.34",)


def scope_for(tmp_path, gateway):
    scope = ToolScope(
        "synthetic-group",
        "synthetic-work",
        ActorContext(group_id="synthetic-group", member_id="synthetic-owner", role=Role.OWNER),
        tmp_path.resolve(),
        frozenset({"web_search", "web_fetch"}),
        "synthetic-trace",
    )
    gateway.bind_scope(scope)
    return scope


def register(gateway, http, budget, **options):
    client = DeepSeekResearchClient("synthetic-key", client=http, resolver=public)
    register_research_tools(
        gateway,
        client,
        budget=budget,
        price_ids={"search": "synthetic-search", "fetch": "synthetic-fetch"},
        retry_backoff_seconds=options.pop("retry_backoff_seconds", 0),
        **options,
    )


def request_for(operation):
    return (
        {"query": "synthetic public evidence"}
        if operation == "search"
        else {"url": "https://example.com/source"}
    )


def success_for(operation):
    return (
        {"results": []}
        if operation == "search"
        else {"title": "Synthetic source", "content": "Synthetic available evidence."}
    )


@pytest.mark.parametrize("operation", ["search", "fetch"])
@pytest.mark.parametrize("first_failure", [429, 503, "timeout"])
async def test_retry_funds_a_distinct_attempt_and_preserves_the_unknown_fee(
    tmp_path, operation, first_failure
):
    budget = HeldFeeFixtureBudget()
    requests = []

    def response(request):
        assert list(budget.reservations.values())[-1] == "inflight"
        requests.append(request)
        if len(requests) == 1:
            if first_failure == "timeout":
                raise httpx.ReadTimeout("Synthetic response lost", request=request)
            return httpx.Response(first_failure)
        return httpx.Response(200, json=success_for(operation))

    gateway = ToolGateway()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        context = ToolContext(scope, "call", "key", asyncio.Event())
        receipt = await gateway.invoke("web_" + operation, request_for(operation), context)
        replay = await gateway.invoke("web_" + operation, request_for(operation), context)

    assert receipt.success and receipt.effect_state == EffectState.NO_EFFECT
    assert replay.success and replay.replayed and len(requests) == 2
    assert budget.reservations == {
        "key:web." + operation: "unknown",
        "key:web." + operation + ":retry:1": "settled",
    }
    assert receipt.output["budget_receipt"] == "key:web." + operation + ":retry:1"
    restored = ToolGateway()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        register(restored, http, budget)
        restored.bind_scope(scope)
        restored.restore_ledger(scope, gateway.export_ledger(scope))
        assert (await restored.invoke("web_" + operation, request_for(operation), context)).replayed
    assert len(requests) == 2


@pytest.mark.parametrize(
    "first_operation,status,error_code,request_count",
    [
        ("search", 503, "provider_unavailable", 3),
        ("fetch", 404, "page_unavailable", 2),
        ("fetch", 200, "page_unavailable", 2),
    ],
)
async def test_failed_read_allows_another_source_and_partial_candidate(
    tmp_path, first_operation, status, error_code, request_count
):
    budget = HeldFeeFixtureBudget()
    requests = []

    def response(request):
        requests.append(request)
        if request.url.path.endswith("/search") or json.loads(request.content).get(
            "url", ""
        ).endswith("/unavailable"):
            return httpx.Response(status, json={"title": "Unavailable", "content": ""})
        return httpx.Response(200, json=success_for("fetch"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        model = ScriptedFakeModel(
            [
                ModelReply(
                    tool_calls=(
                        ModelToolCall(
                            id="unavailable",
                            name="web_" + first_operation,
                            arguments=request_for("search")
                            if first_operation == "search"
                            else {"url": "https://example.com/unavailable"},
                        ),
                        ModelToolCall(
                            id="available", name="web_fetch", arguments=request_for("fetch")
                        ),
                    ),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=5, output_tokens=5),
                ),
                ModelReply(
                    text="Partial evidence is available from the alternate source; one read failed.",
                    usage=TokenUsage(input_tokens=5, output_tokens=5),
                ),
            ]
        )
        engine = ScopedToolEngine(model, gateway)
        outcome = await engine.wait(
            await engine.start(
                ExecutionRequest(
                    work_id=scope.work_id,
                    attempt_id="synthetic-attempt",
                    actor=scope.actor,
                    objective="Find public evidence and deliver useful available results.",
                    workspace=scope.workspace,
                    allowed_tools=scope.allowed_tools,
                    trace_id=scope.trace_id,
                )
            )
        )

    assert outcome.state == RunState.CANDIDATE and outcome.checkpoint.safe_to_resume
    assert not outcome.checkpoint.unresolved_tools and len(requests) == request_count
    assert len(outcome.receipts) == 2 and outcome.usage.tool_calls == 2
    failed, successful = outcome.receipts
    assert failed.error_code == error_code and failed.effect_state == EffectState.NO_EFFECT
    assert successful.success
    # Independent reads may settle between retries of the failed source. Assert
    # each real attempt's identity/state rather than an incidental insertion order.
    expected = {
        f"{scope.work_id}:unavailable:web.{first_operation}": "unknown",
        f"{scope.work_id}:available:web.fetch": "settled",
    }
    for number in range(1, request_count - 1):
        expected[f"{scope.work_id}:unavailable:web.{first_operation}:retry:{number}"] = "unknown"
    assert budget.reservations == expected
    assert error_code in model.requests[-1].messages[-2].content


async def test_retry_budget_denial_keeps_prior_unknown_and_starts_no_unfunded_http(tmp_path):
    budget = HeldFeeFixtureBudget(limit=1)
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(503))
    ) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        receipt = await gateway.invoke(
            "web_search", request_for("search"), ToolContext(scope, "call", "key", asyncio.Event())
        )
    assert (
        receipt.error_code == "budget_exhausted" and receipt.effect_state == EffectState.NO_EFFECT
    )
    assert len(calls) == 1 and budget.reservations == {"key:web.search": "unknown"}
    assert budget.reserve_calls == ["key:web.search", "key:web.search:retry:1"]


@pytest.mark.parametrize("status", [400, 401, 402, 403, 404])
async def test_permanent_provider_failure_does_not_retry_or_block_execution(tmp_path, status):
    budget = HeldFeeFixtureBudget()
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: calls.append(request) or httpx.Response(status)
        )
    ) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        receipt = await gateway.invoke(
            "web_fetch", request_for("fetch"), ToolContext(scope, "call", "key", asyncio.Event())
        )
    assert not receipt.success and receipt.effect_state == EffectState.NO_EFFECT
    assert len(calls) == 1 and budget.reservations == {"key:web.fetch": "unknown"}


@pytest.mark.parametrize("code", ["budget_exhausted", "permission_denied", "price_missing"])
async def test_preflight_refusal_neither_attempts_http_nor_creates_unknown_fees(tmp_path, code):
    budget = HeldFeeFixtureBudget(reserve_error=code)
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(503))
    ) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        receipt = await gateway.invoke(
            "web_search", request_for("search"), ToolContext(scope, "call", "key", asyncio.Event())
        )
    assert receipt.error_code == code and receipt.effect_state == EffectState.NO_EFFECT
    assert not calls and not budget.reservations and len(budget.reserve_calls) == 1


async def test_revocation_stops_retry_before_another_reservation_or_request(tmp_path):
    budget = HeldFeeFixtureBudget()
    calls = []

    async def policy(scope):
        return PolicyDecision(
            actor=scope.actor, allowed_tools=scope.allowed_tools, active=not calls
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(503))
    ) as http:
        gateway = ToolGateway(policy)
        register(gateway, http, budget)
        scope = scope_for(tmp_path, gateway)
        receipt = await gateway.invoke(
            "web_search", request_for("search"), ToolContext(scope, "call", "key", asyncio.Event())
        )
    assert receipt.error_code == "permission_denied" and len(calls) == 1
    assert budget.reserve_calls == ["key:web.search"]
    assert budget.reservations == {"key:web.search": "unknown"}


async def test_work_deadline_bounds_backoff_without_another_paid_attempt(tmp_path):
    budget = HeldFeeFixtureBudget()
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(503))
    ) as http:
        gateway = ToolGateway()
        register(gateway, http, budget, retry_backoff_seconds=1)
        scope = scope_for(tmp_path, gateway)
        receipt = await gateway.invoke(
            "web_search",
            request_for("search"),
            ToolContext(
                scope,
                "call",
                "key",
                asyncio.Event(),
                deadline_monotonic=asyncio.get_running_loop().time() + 0.05,
            ),
        )
    assert receipt.error_code == "tool_timeout" and receipt.effect_state == EffectState.NO_EFFECT
    assert len(calls) == 1 and budget.reservations == {"key:web.search": "unknown"}
