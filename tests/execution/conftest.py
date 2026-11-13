from pathlib import Path

import pytest

from groupagent.domain import ActorContext, Role
from groupagent.execution import ExecutionRequest
from groupagent.tools import ToolGateway, ToolScope, register_workspace_tools


@pytest.fixture
def actor() -> ActorContext:
    return ActorContext(group_id="synthetic-group", member_id="synthetic-member", role=Role.MEMBER)


@pytest.fixture
def gateway() -> ToolGateway:
    gateway = ToolGateway()
    register_workspace_tools(gateway)
    return gateway


@pytest.fixture
def execution_request(actor: ActorContext, tmp_path: Path) -> ExecutionRequest:
    return ExecutionRequest(
        work_id="synthetic-work",
        attempt_id="attempt-1",
        actor=actor,
        objective="Create a small independent text draft and check it.",
        workspace=tmp_path.resolve(),
        allowed_tools=frozenset({"workspace_write_text", "workspace_read_text"}),
        trace_id="trace-offline-test",
    )


@pytest.fixture
def scope(execution_request: ExecutionRequest, gateway: ToolGateway) -> ToolScope:
    scope = ToolScope(
        group_id=execution_request.actor.group_id,
        work_id=execution_request.work_id,
        actor=execution_request.actor,
        workspace=execution_request.workspace,
        allowed_tools=execution_request.allowed_tools,
        trace_id=execution_request.trace_id,
    )
    gateway.bind_scope(scope)
    return scope
