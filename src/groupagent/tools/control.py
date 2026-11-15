"""Explicitly registered control tools; ordinary tool content cannot emit controls."""

from .gateway import ToolGateway
from .models import IdempotencyMode, InputRequest, ToolContext, ToolDefinition


def register_request_input_tool(gateway: ToolGateway) -> None:
    async def request_input(value: InputRequest, context: ToolContext) -> InputRequest:
        # Gateway has already checked current role, scope and cancellation.
        # The engine persists the control receipt and stops; this sends nothing.
        return value

    gateway.register(
        ToolDefinition(
            name="request_input",
            description=(
                "Pause this work and ask its requester for specific missing information or "
                "an available attachment. Use when the accepted objective cannot be completed "
                "without that input. This does not send a message or mark the work completed."
            ),
            input_model=InputRequest,
            output_model=InputRequest,
            handler=request_input,
            resource_scopes=("group", "work"),
            risk_class="read_only",
            idempotency_mode=IdempotencyMode.READ_ONLY,
            control_kind="request_input",
            timeout_seconds=5,
            max_input_bytes=12_000,
            max_output_bytes=12_000,
        )
    )
