import asyncio
from typing import ClassVar, Literal

from pydantic import model_validator

from groupagent.tools import (
    EffectState,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
)


class DocumentInput(ToolData):
    name: str
    kind: Literal["docx", "pptx"]
    validation_hints: ClassVar[dict[str, str]] = {
        "value_error": "name must end with the extension matching kind: .docx or .pptx",
        "kind": "Choose docx or pptx",
    }

    @model_validator(mode="after")
    def extension(self):
        if not self.name.endswith("." + self.kind):
            # Deliberately unsafe exception text proves it never reaches a receipt.
            raise ValueError("Invalid private filename: " + self.name)
        return self


class Result(ToolData):
    ready: bool


async def test_safe_pydantic_fields_types_and_reviewed_root_hint(execution_request):
    called = False

    async def handler(args, context):
        nonlocal called
        called = True
        return Result(ready=True)

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="synthetic_document",
            description="Known validation contract",
            input_model=DocumentInput,
            output_model=Result,
            handler=handler,
        )
    )
    scope = ToolScope(
        execution_request.actor.group_id,
        execution_request.work_id,
        execution_request.actor,
        execution_request.workspace,
        frozenset({"synthetic_document"}),
        "trace",
    )
    gateway.bind_scope(scope)

    async def invoke(arguments, key):
        return await gateway.invoke(
            "synthetic_document", arguments, ToolContext(scope, key, key, asyncio.Event())
        )

    secret_value = "SYNTHETIC_PRIVATE_VALUE_NEVER_A_CREDENTIAL"
    root = await invoke({"name": secret_value, "kind": "docx"}, "root")
    assert root.error_code == "invalid_input" and root.effect_state == EffectState.NOT_STARTED
    assert root.input_issues[0].field == () and root.input_issues[0].kind == "value_error"
    assert ".docx or .pptx" in root.message and secret_value not in root.model_dump_json()
    secret_key = "SYNTHETIC_PRIVATE_KEY_NAME"
    unknown = await invoke({"name": "file.docx", "kind": "docx", secret_key: secret_value}, "extra")
    assert unknown.input_issues[0].field == ("<extra>",)
    assert (
        secret_key not in unknown.model_dump_json()
        and secret_value not in unknown.model_dump_json()
    )
    wrong = await invoke({"name": {"content": secret_value}, "kind": "unknown"}, "type")
    assert {issue.field for issue in wrong.input_issues} == {("name",), ("kind",)}
    assert secret_value not in wrong.model_dump_json() and "unknown" not in wrong.model_dump_json()
    assert not called
    fixed = await invoke({"name": "file.docx", "kind": "docx"}, "fixed")
    assert fixed.success and called
