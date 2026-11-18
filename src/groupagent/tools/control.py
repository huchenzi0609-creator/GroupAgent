"""Explicitly registered control tools; ordinary tool content cannot emit controls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Annotated, Literal

from pydantic import Field

from .gateway import ToolGateway
from .models import (
    IdempotencyMode,
    InputRequest,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolFailure,
    ToolFailureDisposition,
    ToolFailureOrigin,
)


class MissingInformation(ToolData):
    kind: Literal["information"]
    field: str = Field(
        min_length=1,
        max_length=200,
        description="A specific unresolved decision, value, or fact only the requester can supply.",
    )


class MissingAttachment(ToolData):
    kind: Literal["attachment"]
    reference: str = Field(
        min_length=1,
        max_length=200,
        description=(
            "For an earlier or selected attachment, use its observed version_id, file_id or "
            "filename. For a distinct attachment never received, give its concrete name or role."
        ),
    )


class WorkInputRequest(ToolData):
    question: str = Field(min_length=1, max_length=2000)
    missing: tuple[
        Annotated[MissingInformation | MissingAttachment, Field(discriminator="kind")], ...
    ] = Field(
        min_length=1,
        max_length=8,
    )


def register_request_input_tool(
    gateway: ToolGateway,
    *,
    prepared_files: Callable[[], Sequence[Mapping[str, object]]] | None = None,
    review_missing: Callable[[WorkInputRequest, ToolContext], Awaitable[None]] | None = None,
    timeout_seconds: float = 5,
) -> None:
    async def request_input(
        value: InputRequest | WorkInputRequest, context: ToolContext
    ) -> InputRequest:
        if isinstance(value, InputRequest):
            return value
        files = prepared_files() if prepared_files is not None else ()
        available = {
            handle
            for file in files
            for key in ("file_id", "version_id", "from_version", "attachment_id", "name")
            if isinstance(handle := file.get(key), str)
        }
        for ledger in gateway.export_ledger(context.scope):
            receipt = ledger.receipt
            if receipt.tool_name == "artifact_export" and receipt.success:
                export = receipt.output or {}
                path, version = export.get("path"), export.get("source_version_id")
                if isinstance(path, str) and isinstance(version, str) and version in available:
                    available.add(path)
        required = []
        for item in value.missing:
            if isinstance(item, MissingInformation):
                required.append(item.field)
                continue
            reference = item.reference.strip()
            if reference in available:
                raise ToolFailure(
                    "input_already_available",
                    "That attachment is already prepared for this work. Use its ready "
                    "input_files entry or successful artifact_import receipt and any "
                    "successful read evidence to continue. Ask only about unresolved input.",
                    not_started=True,
                    origin=ToolFailureOrigin.PRECONDITION,
                    disposition=ToolFailureDisposition.REPAIR,
                )
            required.append(reference)
        if review_missing is not None:
            await review_missing(value, context)
        return InputRequest(question=value.question, required_inputs=tuple(required))

    gateway.register(
        ToolDefinition(
            name="request_input",
            description=(
                "Pause this work and ask its requester for specific missing information or "
                "an attachment that only this requester can supply. Inspect the authoritative "
                "human requirements, ready input_files and available tool receipts first. "
                "Use only when their accepted objective cannot proceed without the specified "
                "input. Continue from available evidence and reasonable implementation defaults "
                "when a choice is already answered, optional or obtainable with authorized tools."
            ),
            input_model=WorkInputRequest
            if prepared_files is not None or review_missing is not None
            else InputRequest,
            output_model=InputRequest,
            handler=request_input,
            resource_scopes=("group", "work"),
            risk_class="read_only",
            idempotency_mode=IdempotencyMode.READ_ONLY,
            control_kind="request_input",
            timeout_seconds=timeout_seconds,
            max_input_bytes=12_000,
            max_output_bytes=12_000,
        )
    )
