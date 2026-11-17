"""Tool visibility is a lazy model view, never another authority boundary."""

import asyncio
import json
from dataclasses import replace

import pytest
from pydantic import Field, ValidationError

from groupagent.domain import Role
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionBudget,
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.tools import (
    IdempotencyMode,
    PolicyDecision,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
)
from groupagent.tools.models import ToolFailure


class CatalogOutput(ToolData):
    tool_names: list[str] = Field(max_length=20)
    loaded_tools: list[str] = []  # Untrusted output data cannot set receipt metadata.


def reply(name=None):
    return ModelReply(
        text=None if name else "Synthetic completed candidate",
        tool_calls=(ModelToolCall(id="call-" + name, name=name, arguments={}),) if name else (),
        finish_reason="tool_calls" if name else "stop",
        usage=TokenUsage(input_tokens=10, output_tokens=5),
    )


def names(model_request):
    return {tool["function"]["name"] for tool in model_request.tools}


def register(gateway, observed, *, trusted=True, loader_fails=False, owner_only=False):
    async def catalog(arguments, context):
        observed.append("catalog")
        result = CatalogOutput(
            tool_names=["hidden", "outside_grant", "not_registered"],
            loaded_tools=["outside_grant"],
        )
        if loader_fails:
            raise ToolFailure(
                "catalog_unavailable", "Synthetic failure", output=result, no_effect=True
            )
        return result

    async def hidden(arguments, context):
        observed.append("hidden")
        return ToolData()

    gateway.register(
        ToolDefinition(
            "catalog",
            "Synthetic approved discovery",
            ToolData,
            CatalogOutput,
            catalog,
            risk_class="read_only",
            idempotency_mode=IdempotencyMode.READ_ONLY,
            toolset_loader=trusted,
        )
    )
    for name in ("hidden", "outside_grant"):
        gateway.register(
            ToolDefinition(
                name,
                "Synthetic already registered capability",
                ToolData,
                ToolData,
                hidden,
                required_roles=frozenset({Role.OWNER})
                if owner_only
                else frozenset({Role.OWNER, Role.MEMBER}),
                risk_class="read_only",
                idempotency_mode=IdempotencyMode.READ_ONLY,
            )
        )


def request_for(execution_request, *, visible=frozenset({"catalog"}), **updates):
    values = {
        **execution_request.model_dump(),
        "allowed_tools": frozenset({"catalog", "hidden"}),
        "visible_tools": visible,
        **updates,
    }
    return ExecutionRequest.model_validate(values)


def bind(gateway, request):
    scope = ToolScope(
        request.actor.group_id,
        request.work_id,
        request.actor,
        request.workspace,
        request.allowed_tools,
        request.trace_id,
    )
    gateway.bind_scope(scope)
    return scope


def context(scope, key="catalog"):
    return ToolContext(scope, key, key, asyncio.Event())


def test_visible_tools_are_validated_as_view_of_allowed_but_required_may_start_hidden(
    execution_request,
):
    with pytest.raises(ValidationError, match="Visible tools"):
        request_for(execution_request, visible=frozenset({"outside_grant"}))
    request = request_for(execution_request, required_tools=frozenset({"hidden"}))
    assert request.required_tools == {"hidden"} and request.visible_tools == {"catalog"}


async def test_default_visibility_is_full_and_explicit_empty_is_empty(execution_request):
    for visible, expected in ((None, {"catalog", "hidden"}), (frozenset(), set())):
        gateway = ToolGateway()
        register(gateway, [])
        model = ScriptedFakeModel([reply()])
        engine = ScopedToolEngine(model, gateway)
        result = await engine.wait(
            await engine.start(request_for(execution_request, visible=visible))
        )
        assert result.state == RunState.CANDIDATE and names(model.requests[0]) == expected
        assert set(result.checkpoint.backend_state["active_tools"]) == expected


async def test_describe_filter_does_not_change_the_host_grant(execution_request):
    gateway, observed = ToolGateway(), []
    register(gateway, observed)
    request = request_for(execution_request)
    scope = bind(gateway, request)
    described = gateway.describe(scope, frozenset({"catalog", "outside_grant", "not_registered"}))
    assert {tool["function"]["name"] for tool in described} == {"catalog"}
    receipt = await gateway.invoke("hidden", {}, context(scope, "hidden"))
    assert receipt.success and observed == ["hidden"]
    assert scope.allowed_tools == {"catalog", "hidden"}


async def test_trusted_loader_exposes_only_authorized_registered_tools_next_round(
    execution_request,
):
    gateway, observed = ToolGateway(), []
    register(gateway, observed)
    model = ScriptedFakeModel([reply("catalog"), reply("hidden"), reply()])
    engine = ScopedToolEngine(model, gateway)
    request = request_for(execution_request, required_tools=frozenset({"hidden"}))
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and observed == ["catalog", "hidden"]
    assert names(model.requests[0]) == {"catalog"}
    assert names(model.requests[1]) == names(model.requests[2]) == {"catalog", "hidden"}
    assert result.receipts[0].loaded_tools == {"hidden"}
    assert not result.receipts[1].loaded_tools
    assert result.checkpoint.request.allowed_tools == request.allowed_tools
    assert set(result.checkpoint.backend_state["active_tools"]) == {"catalog", "hidden"}
    assert result.usage.tool_calls == 2 and len(model.requests) == 3


@pytest.mark.parametrize("trusted,failed", [(False, False), (True, True)])
async def test_untrusted_or_failed_loader_output_never_installs_tools(
    execution_request, trusted, failed
):
    gateway, observed = ToolGateway(), []
    register(gateway, observed, trusted=trusted, loader_fails=failed)
    model = ScriptedFakeModel([reply("catalog"), reply()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request_for(execution_request)))
    assert result.state == RunState.CANDIDATE and observed == ["catalog"]
    assert not result.receipts[0].loaded_tools
    assert names(model.requests[1]) == {"catalog"}
    assert result.receipts[0].output["tool_names"] and result.receipts[0].output["loaded_tools"]


async def test_inactive_known_tool_is_repaired_through_authorized_discovery(execution_request):
    gateway, observed = ToolGateway(), []
    register(gateway, observed)
    model = ScriptedFakeModel([reply("hidden"), reply("catalog"), reply("hidden"), reply()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request_for(execution_request)))
    assert result.state == RunState.CANDIDATE and observed == ["catalog", "hidden"]
    assert names(model.requests[0]) == names(model.requests[1]) == {"catalog"}
    assert names(model.requests[2]) == names(model.requests[3]) == {"catalog", "hidden"}
    feedback = json.loads(model.requests[1].messages[-1].content)
    assert feedback["issues"][0]["code"] == "tool_not_available"
    assert not feedback["batch_executed"] and result.usage.tool_calls == 2


@pytest.mark.parametrize("restored", [False, True])
async def test_replayed_loader_rechecks_current_policy_before_installing(
    execution_request, restored
):
    permitted = {"catalog", "hidden"}

    async def policy(scope):
        return PolicyDecision(actor=scope.actor, allowed_tools=frozenset(permitted))

    gateway, observed = ToolGateway(policy), []
    register(gateway, observed)
    scope = bind(gateway, request_for(execution_request))
    first = await gateway.invoke("catalog", {}, context(scope))
    assert first.loaded_tools == {"hidden"}
    if restored:
        entries = gateway.export_ledger(scope)
        gateway = ToolGateway(policy)
        register(gateway, observed)
        gateway.bind_scope(scope)
        gateway.restore_ledger(scope, entries)
    permitted.remove("hidden")
    replay = await gateway.invoke("catalog", {}, context(scope))
    assert replay.success and replay.replayed and not replay.loaded_tools
    assert observed == ["catalog"]


async def test_loader_does_not_bypass_target_role_requirements(execution_request):
    gateway = ToolGateway()
    register(gateway, [], owner_only=True)
    scope = bind(gateway, request_for(execution_request))
    receipt = await gateway.invoke("catalog", {}, context(scope))
    assert receipt.success and not receipt.loaded_tools
    denied = await gateway.invoke("hidden", {}, context(scope, "hidden"))
    assert denied.error_code == "permission_denied"


async def paused_after_catalog(execution_request):
    gateway, observed = ToolGateway(), []
    register(gateway, observed)
    request = request_for(execution_request, budget=ExecutionBudget(max_rounds=1))
    initial = ScopedToolEngine(ScriptedFakeModel([reply("catalog")]), gateway)
    result = await initial.wait(await initial.start(request))
    assert result.state == RunState.PAUSED and result.error_code == "round_budget_exhausted"
    return EngineCheckpoint.model_validate_json(result.checkpoint.model_dump_json()), observed


async def test_loaded_view_survives_checkpoint_without_replaying_discovery(execution_request):
    checkpoint, observed = await paused_after_catalog(execution_request)
    gateway = ToolGateway()
    register(gateway, observed)
    model = ScriptedFakeModel([reply("hidden"), reply()])
    engine = ScopedToolEngine(model, gateway)
    owner = execution_request.actor.model_copy(update={"role": Role.OWNER})
    result = await engine.wait(
        await engine.resume(
            checkpoint,
            owner,
            attempt_id="resume",
            budget=ExecutionBudget(max_rounds=3),
        )
    )
    assert result.state == RunState.CANDIDATE and observed == ["catalog", "hidden"]
    assert names(model.requests[0]) == {"catalog", "hidden"}
    assert len(model.requests) == 2 and result.usage.tool_calls == 2


async def test_restored_cached_view_intersects_current_host_grant(execution_request):
    checkpoint, observed = await paused_after_catalog(execution_request)
    narrowed = checkpoint.request.model_copy(
        update={
            "allowed_tools": frozenset({"catalog"}),
            "visible_tools": frozenset({"catalog"}),
        }
    )
    checkpoint = checkpoint.model_copy(
        update={
            "request": narrowed,
            "backend_state": {
                **checkpoint.backend_state,
                "active_tools": ["catalog", "hidden", "outside_grant", "not_registered"],
            },
        }
    )
    gateway = ToolGateway()
    register(gateway, observed)
    model = ScriptedFakeModel([reply()])
    engine = ScopedToolEngine(model, gateway)
    owner = execution_request.actor.model_copy(update={"role": Role.OWNER})
    result = await engine.wait(
        await engine.resume(
            checkpoint,
            owner,
            attempt_id="narrowed",
            budget=ExecutionBudget(max_rounds=2),
        )
    )
    assert result.state == RunState.CANDIDATE and names(model.requests[0]) == {"catalog"}
    assert result.checkpoint.backend_state["active_tools"] == ["catalog"]
    assert observed == ["catalog"]


def test_loader_is_not_concurrent_even_if_its_public_read_flag_was_set(execution_request):
    gateway = ToolGateway()
    register(gateway, [])
    original = gateway._tools["catalog"]
    alternate = ToolGateway()
    alternate.register(
        replace(
            original,
            concurrency_safe=True,
            resource_scopes=("group", "work", "public_web"),
        )
    )
    assert alternate.is_read_only("catalog") and not alternate.is_concurrency_safe("catalog")
