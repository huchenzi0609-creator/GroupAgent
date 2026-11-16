"""Allowlisted, audited dispatch for trusted tools. No shell is registered implicitly."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, replace
from typing import Any

from pydantic import ValidationError

from groupagent.domain import DomainError, Role
from groupagent.resources import ResourceDeferred, ResourceGovernor

from .models import (
    ISOLATED_PROCESS_TOOLS,
    EffectState,
    IdempotencyMode,
    InputRequest,
    PolicyDecision,
    PolicyProvider,
    ToolContext,
    ToolControl,
    ToolDefinition,
    ToolFailure,
    ToolInputIssue,
    ToolInputRequired,
    ToolLedgerEntry,
    ToolReceipt,
    ToolScope,
)
from .paths import scoped_path


@dataclass(frozen=True)
class _PreparedTool:
    definition: ToolDefinition[Any, Any]
    context: ToolContext
    encoded: bytes
    arguments: Any


def tool_call_fingerprint(name: str, arguments: dict[str, Any]) -> str:
    """The exact model proposal identity used by the durable tool ledger."""
    encoded = json.dumps(arguments, sort_keys=True, allow_nan=False).encode("utf-8")
    return hashlib.sha256(name.encode("utf-8") + b"\0" + encoded).hexdigest()


def _failure(
    name: str,
    context: ToolContext,
    code: str,
    effect: EffectState = EffectState.NOT_STARTED,
    message: str = "The tool did not complete",
) -> ToolReceipt:
    return ToolReceipt(
        call_id=context.call_id,
        tool_name=name,
        idempotency_key=context.idempotency_key,
        success=False,
        effect_state=effect,
        error_code=code,
        message=message,
    )


_INPUT_ERROR_TYPES = frozenset(
    {
        "missing",
        "extra_forbidden",
        "string_type",
        "string_too_short",
        "string_too_long",
        "list_type",
        "tuple_type",
        "int_type",
        "int_parsing",
        "int_from_float",
        "float_type",
        "float_parsing",
        "bool_type",
        "bool_parsing",
        "dict_type",
        "literal_error",
        "enum",
        "greater_than",
        "greater_than_equal",
        "less_than",
        "less_than_equal",
        "finite_number",
        "too_long",
        "too_short",
        "union_tag_invalid",
        "union_tag_not_found",
        "model_type",
        "value_error",
        "datetime_type",
        "datetime_parsing",
        "datetime_from_date_parsing",
        "timezone_aware",
        "json_invalid",
        "string_pattern_mismatch",
    }
)


def _input_issues(
    error: ValidationError, definition: ToolDefinition[Any, Any]
) -> tuple[ToolInputIssue, ...]:
    names: set[str] = set()

    def fields(schema: Any) -> None:
        if isinstance(schema, dict):
            properties = schema.get("properties")
            if isinstance(properties, dict):
                names.update(properties)
            for value in schema.values():
                fields(value)
        elif isinstance(schema, list):
            for value in schema:
                fields(value)

    fields(definition.input_model.model_json_schema())
    issues = []
    for entry in error.errors(include_input=False, include_context=False, include_url=False)[:8]:
        # Unknown keys may themselves contain credentials. Only declared schema
        # names and bounded array indices may appear in model-visible diagnostics.
        location = tuple(
            part
            if isinstance(part, str) and part in names
            else part
            if type(part) is int and 0 <= part < 4096
            else "<extra>"
            for part in entry["loc"][:6]
        )
        kind = entry["type"] if entry["type"] in _INPUT_ERROR_TYPES else "invalid_value"
        hints = getattr(definition.input_model, "validation_hints", {})
        hint = None
        if isinstance(hints, dict):
            hint = hints.get(
                ".".join(str(part) for part in location), hints.get(kind, hints.get("*"))
            )
        if not isinstance(hint, str) or len(hint) > 600:
            hint = None
        issues.append(ToolInputIssue(field=location, kind=kind, hint=hint))
    return tuple(issues)


async def static_policy(scope: ToolScope) -> PolicyDecision:
    """Offline default. Production must supply a durable current-policy lookup."""
    return PolicyDecision(actor=scope.actor, allowed_tools=scope.allowed_tools)


class ToolGateway:
    def __init__(self, policy_provider: PolicyProvider = static_policy) -> None:
        self._policy = policy_provider
        self._tools: dict[str, ToolDefinition[Any, Any]] = {}
        self._scopes: dict[tuple[str, str], ToolScope] = {}
        self._ledger: dict[tuple[str, str, str], tuple[str, ToolReceipt]] = {}
        self._active: dict[tuple[str, str, str], asyncio.Task[Any]] = {}
        self._resource_admission: tuple[ResourceGovernor, int, frozenset[str]] | None = None

    def bind_resource_admission(
        self, governor: ResourceGovernor, *, memory_mb: int, tool_names: frozenset[str]
    ) -> None:
        """Admit reviewed worker tools before recording an in-flight effect.

        The reservation follows the handler through ContextVar inheritance and
        is reused by its DockerSandbox calls. It is never a model capability.
        """
        if self._active or not tool_names.issubset(self._tools):
            raise ValueError("Bind resources to registered tools before invocation")
        if type(memory_mb) is not int or memory_mb <= 0:
            raise ValueError("A full child memory limit is required")
        self._resource_admission = (governor, memory_mb, tool_names)

    @property
    def registered_names(self) -> frozenset[str]:
        return frozenset(self._tools)

    @property
    def control_names(self) -> frozenset[str]:
        return frozenset(
            name for name, definition in self._tools.items() if definition.control_kind
        )

    def is_read_only(self, name: str) -> bool:
        """Trusted business-effect classification, independent of a model's claim."""
        definition = self._tools.get(name)
        return definition is not None and (
            definition.risk_class == "read_only"
            and definition.idempotency_mode == IdempotencyMode.READ_ONLY
            and not definition.control_kind
            and not definition.may_request_input
        )

    def is_isolated_draft(self, name: str) -> bool:
        """Reviewed worker effects cross only the declared workspace publication boundary."""
        definition = self._tools.get(name)
        return definition is not None and (
            definition.risk_class == "local_draft"
            and definition.idempotency_mode == IdempotencyMode.RECONCILE_BEFORE_RETRY
            and "isolated_worker" in definition.resource_scopes
            and not definition.control_kind
            and not definition.may_request_input
        )

    def is_concurrency_safe(self, name: str) -> bool:
        """Only reviewed public reads may overlap; this grants no authority.

        Worker-admitted tools keep the existing serial resource-deferral path.
        A model output or a tool's name cannot declare its execution class.
        """
        definition = self._tools.get(name)
        if definition is None or not (
            self.is_read_only(name)
            and definition.concurrency_safe is True
            and "public_web" in definition.resource_scopes
            and not definition.toolset_loader
        ):
            return False
        if self._resource_admission is not None:
            governor, _, names = self._resource_admission
            if governor.policy.enabled and name in names:
                return False
        return True

    def normalize_arguments(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Pure schema normalization for adapters; this does not authorize invocation."""
        definition = self._tools.get(name)
        if definition is None:
            raise DomainError("tool_not_registered", "The requested tool is not registered")
        return definition.input_model.model_validate(arguments).model_dump(mode="json")

    def register(self, definition: ToolDefinition[Any, Any]) -> None:
        if definition.required_tool_family is not None and (
            definition.name not in ISOLATED_PROCESS_TOOLS
            or definition.risk_class != "local_draft"
            or definition.idempotency_mode != IdempotencyMode.RECONCILE_BEFORE_RETRY
        ):
            raise ValueError("Only reviewed isolated process registrations can share requirements")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,63}", definition.name):
            raise ValueError("Tool names must be compatible with function-calling APIs")
        if definition.name in self._tools:
            raise ValueError("A tool name is already registered")
        if definition.input_model.model_config.get("extra") != "forbid":
            raise ValueError("Tool input models must forbid extra fields")
        if not definition.required_roles or Role.OBSERVER in definition.required_roles:
            raise ValueError("Observers cannot be granted tool execution")
        if definition.risk_class not in {"read_only", "local_draft"}:
            raise ValueError("This foundation supports reviewed read/local-draft tools only")
        if (
            definition.timeout_seconds <= 0
            or min(
                definition.max_input_bytes,
                definition.max_output_bytes,
                definition.max_artifact_bytes,
            )
            <= 0
        ):
            raise ValueError("Tool limits must be positive")
        if not {"group", "work"}.issubset(definition.resource_scopes):
            raise ValueError("Every tool must declare group and work resource scopes")
        if definition.control_kind and (
            definition.control_kind != "request_input"
            or definition.output_model is not InputRequest
            or definition.idempotency_mode != IdempotencyMode.READ_ONLY
        ):
            raise ValueError("Control tools require the reviewed typed read-only contract")
        self._tools[definition.name] = definition

    def bind_scope(self, scope: ToolScope) -> None:
        if scope.actor.group_id != scope.group_id:
            raise DomainError("scope_violation", "The actor belongs to another group")
        if not scope.group_id or not scope.work_id or not scope.trace_id:
            raise DomainError("invalid_input", "Group, work and trace identity are required")
        if not scope.workspace.is_absolute() or not scope.workspace.is_dir():
            raise DomainError("invalid_path", "The control plane must provide a workspace")
        if scope.workspace != scope.workspace.resolve():
            raise DomainError("scope_violation", "The workspace root must be canonical")
        if not scope.allowed_tools.issubset(self._tools):
            raise DomainError("tool_not_registered", "A granted tool is not registered")
        key = (scope.group_id, scope.work_id)
        previous = self._scopes.get(key)
        if previous and previous != scope:
            raise DomainError("scope_violation", "An existing work grant cannot be rebound")
        for other_key, other in self._scopes.items():
            if other_key != key and (
                scope.workspace.is_relative_to(other.workspace)
                or other.workspace.is_relative_to(scope.workspace)
            ):
                raise DomainError("scope_violation", "Workspaces must not overlap between works")
        self._scopes[key] = scope

    def describe(
        self, scope: ToolScope, visible_tools: frozenset[str] | None = None
    ) -> tuple[dict[str, Any], ...]:
        self._check_scope(scope)
        names = (
            scope.allowed_tools if visible_tools is None else scope.allowed_tools & visible_tools
        )
        return tuple(self._tools[name].describe() for name in sorted(names))

    def _check_scope(self, scope: ToolScope) -> None:
        if self._scopes.get((scope.group_id, scope.work_id)) != scope:
            raise DomainError("scope_violation", "The tool context has no matching work grant")

    async def authorize(self, name: str, context: ToolContext) -> ToolContext:
        self._check_scope(context.scope)
        if context.cancelled.is_set():
            raise DomainError("cancelled", "This execution was cancelled")
        definition = self._tools.get(name)
        if definition is None:
            raise DomainError("tool_not_registered", "The requested tool is not registered")
        decision = await self._policy(context.scope)
        actor = decision.actor
        if (
            actor.group_id != context.group_id
            or actor.member_id != context.actor.member_id
            or actor.policy_revision < context.actor.policy_revision
        ):
            raise DomainError("scope_violation", "Current policy did not match the work identity")
        if (
            not decision.active
            or name not in context.scope.allowed_tools
            or name not in decision.allowed_tools
            or actor.role not in definition.required_roles
        ):
            raise DomainError("permission_denied", "Current policy does not permit this tool")
        if context.cancelled.is_set():
            raise DomainError("cancelled", "This execution was cancelled")
        return replace(context, scope=replace(context.scope, actor=actor))

    async def _trusted_metadata(self, result: ToolReceipt, context: ToolContext) -> ToolReceipt:
        definition = self._tools.get(result.tool_name)
        evidence = set()
        if (
            result.success
            and result.effect_state == EffectState.SUCCEEDED
            and definition is not None
            and definition.required_tool_family == "isolated_process"
        ):
            evidence.add(result.tool_name)
            for name in ISOLATED_PROCESS_TOOLS - {result.tool_name}:
                alternate = self._tools.get(name)
                if alternate is None or alternate.required_tool_family != "isolated_process":
                    continue
                try:
                    # Recheck the alternative on replay too: old receipts cannot
                    # revive an unregistered or newly forbidden tool requirement.
                    await self.authorize(name, context)
                except Exception:
                    continue
                evidence.add(name)
        loaded = set()
        if (
            result.success
            and result.effect_state in {EffectState.SUCCEEDED, EffectState.NO_EFFECT}
            and definition is not None
            and definition.toolset_loader is True
        ):
            requested = (result.output or {}).get("tool_names")
            if isinstance(requested, (list, tuple)):
                candidates = {
                    name
                    for name in requested
                    if isinstance(name, str)
                    and name in context.scope.allowed_tools
                    and name in self._tools
                }
                for name in sorted(candidates):
                    try:
                        await self.authorize(name, context)
                    except Exception:
                        continue
                    loaded.add(name)
        return result.model_copy(
            update={
                "required_tool_evidence": frozenset(evidence),
                "loaded_tools": frozenset(loaded),
            }
        )

    async def _prepare(
        self, name: str, arguments: dict[str, Any], context: ToolContext
    ) -> _PreparedTool | ToolReceipt:
        def receipt(
            code: str,
            effect: EffectState = EffectState.NOT_STARTED,
            message: str = "The tool did not complete",
        ) -> ToolReceipt:
            return _failure(name, context, code, effect, message)

        validating_arguments = False
        try:
            effective_context = await self.authorize(name, context)
            definition = self._tools[name]
            encoded = json.dumps(arguments, sort_keys=True, allow_nan=False).encode("utf-8")
            if len(encoded) > definition.max_input_bytes:
                return receipt("input_too_large")
            validating_arguments = True
            validated = definition.input_model.model_validate(arguments)
            validating_arguments = False
            for path_field in definition.input_path_fields:
                value = getattr(validated, path_field)
                if not isinstance(value, str):
                    return receipt("invalid_input")
                scoped_path(context.workspace, value)
        except DomainError as exc:
            return receipt(exc.code)
        except ValidationError as error:
            if not validating_arguments:
                return receipt("policy_unavailable")
            issues = _input_issues(error, definition)
            description = "; ".join(
                (".".join(str(part) for part in issue.field) or "<root>")
                + " ("
                + issue.kind
                + ")"
                + (": " + issue.hint if issue.hint else "")
                for issue in issues
            )
            return receipt("invalid_input", message="Correct input: " + description).model_copy(
                update={"input_issues": issues}
            )
        except (TypeError, ValueError):
            return receipt("invalid_input")
        except Exception:
            return receipt("policy_unavailable")

        return _PreparedTool(definition, effective_context, encoded, validated)

    async def preflight(
        self, name: str, arguments: dict[str, Any], context: ToolContext
    ) -> ToolReceipt | None:
        """Check current authority/schema/size without a ledger entry or handler.

        The invocation must still recheck current authority before execution;
        preflight is not a reusable permission token.
        """
        prepared = await self._prepare(name, arguments, context)
        if isinstance(prepared, ToolReceipt):
            return prepared
        if context.deadline_monotonic is not None and (
            context.deadline_monotonic <= asyncio.get_running_loop().time()
        ):
            return _failure(name, context, "time_budget_exhausted")
        return None

    async def invoke(
        self, name: str, arguments: dict[str, Any], context: ToolContext
    ) -> ToolReceipt:
        def receipt(
            code: str,
            effect: EffectState = EffectState.NOT_STARTED,
            message: str = "The tool did not complete",
        ) -> ToolReceipt:
            return _failure(name, context, code, effect, message)

        prepared = await self._prepare(name, arguments, context)
        if isinstance(prepared, ToolReceipt):
            return prepared
        definition, effective_context = prepared.definition, prepared.context
        validated = prepared.arguments

        key = (context.group_id, context.work_id, context.idempotency_key)
        fingerprint = tool_call_fingerprint(name, arguments)
        previous = self._ledger.get(key)
        if previous:
            if previous[0] != fingerprint:
                return receipt("idempotency_conflict")
            replay = previous[1].model_copy(update={"call_id": context.call_id, "replayed": True})
            return await self._trusted_metadata(replay, context)
        if context.cancelled.is_set():
            return receipt("cancelled")
        if context.deadline_monotonic is not None and (
            context.deadline_monotonic <= asyncio.get_running_loop().time()
        ):
            return receipt("time_budget_exhausted")

        reservation = None
        if self._resource_admission is not None:
            governor, memory_mb, names = self._resource_admission
            if governor.policy.enabled and name in names:
                try:
                    reservation = governor.reserve(memory_mb, owner=context.idempotency_key)
                except ResourceDeferred:
                    # No handler, timeout, in-flight ledger entry, or effect has
                    # started. The exact proposal can resume without a new model
                    # request and without replaying earlier tools in its batch.
                    return receipt("resource_deferred")

        uncertain = (
            EffectState.NO_EFFECT
            if definition.idempotency_mode == IdempotencyMode.READ_ONLY
            else EffectState.UNKNOWN
        )
        self._ledger[key] = (fingerprint, receipt("tool_in_progress", uncertain))

        async def execute() -> Any:
            if reservation is None:
                return await definition.handler(validated, effective_context)
            with reservation.activate():
                return await definition.handler(validated, effective_context)

        task: asyncio.Task[Any] = asyncio.create_task(execute())
        cancellation = asyncio.create_task(context.cancelled.wait())
        self._active[key] = task
        result: ToolReceipt
        try:
            timeout = definition.timeout_seconds
            if context.deadline_monotonic is not None:
                timeout = min(
                    timeout, max(0, context.deadline_monotonic - asyncio.get_running_loop().time())
                )
            done, _ = await asyncio.wait(
                {task, cancellation},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation in done or context.cancelled.is_set():
                result = receipt("cancelled", uncertain)
            elif task not in done:
                result = receipt("tool_timeout", uncertain)
            else:
                raw_output = task.result()
                output = definition.output_model.model_validate(raw_output)
                encoded_output = output.model_dump_json().encode("utf-8")
                if len(encoded_output) > definition.max_output_bytes:
                    result = receipt("output_too_large", uncertain)
                else:
                    for path_field in definition.output_path_fields:
                        path = scoped_path(context.workspace, getattr(output, path_field))
                        if (
                            not path.is_file()
                            or path.stat().st_size > definition.max_artifact_bytes
                        ):
                            raise DomainError("invalid_artifact", "Tool output is not a valid file")
                    result = ToolReceipt(
                        call_id=context.call_id,
                        tool_name=name,
                        idempotency_key=context.idempotency_key,
                        success=True,
                        effect_state=(
                            EffectState.NO_EFFECT
                            if definition.idempotency_mode == IdempotencyMode.READ_ONLY
                            else EffectState.SUCCEEDED
                        ),
                        output=output.model_dump(mode="json"),
                        control=ToolControl(input_request=InputRequest.model_validate(output))
                        if definition.control_kind == "request_input"
                        else None,
                    )
        except asyncio.CancelledError:
            result = receipt("cancelled", uncertain)
        except ToolInputRequired as exc:
            if definition.may_request_input:
                result = receipt(
                    "input_required",
                    EffectState.NO_EFFECT,
                    message="This action was not performed; confirmed input is required.",
                ).model_copy(update={"control": ToolControl(input_request=exc.input_request)})
            else:
                result = receipt("unregistered_tool_control", uncertain)
        except ToolFailure as exc:
            effect = EffectState.NO_EFFECT if exc.no_effect else uncertain
            if exc.output is None:
                result = receipt(
                    exc.code,
                    effect,
                    message=exc.message,
                )
            else:
                try:
                    failed_output = definition.output_model.model_validate(exc.output)
                    if len(failed_output.model_dump_json().encode()) > definition.max_output_bytes:
                        result = receipt("output_too_large", effect)
                    else:
                        result = receipt(exc.code, effect, message=exc.message).model_copy(
                            update={"output": failed_output.model_dump(mode="json")}
                        )
                except Exception:
                    # Bad diagnostics cannot erase the handler's independently
                    # established unpublished/rollback fact or escape dispatch.
                    # Never expose Pydantic errors containing tool output.
                    result = receipt("invalid_output", effect)
        except ResourceDeferred:
            # This trusted control exception attests admission refused before
            # the effectful operation. It is not a failed/unknown business call.
            result = receipt("resource_deferred", EffectState.NOT_STARTED)
        except DomainError as exc:
            result = receipt(exc.code, uncertain)
        except Exception:
            result = receipt("tool_failed", uncertain)
        finally:
            cancellation.cancel()
            await asyncio.gather(cancellation, return_exceptions=True)
            if not task.done():
                task.cancel()
                # Never wait indefinitely for a badly behaved in-process handler.
                await asyncio.wait({task}, timeout=0.05)
            if task.done():
                self._active.pop(key, None)
                # Retrieve the exception, including cancellation, to avoid orphan warnings.
                if not task.cancelled():
                    task.exception()
            else:
                task.add_done_callback(
                    lambda finished: finished.exception() if not finished.cancelled() else None
                )
            if reservation is not None:
                if task.done():
                    reservation.release()
                else:
                    # An uncooperative handler may still start/write a worker.
                    # Retain capacity until it actually ends, not just until the
                    # gateway's cancellation grace or execution timeout expires.
                    task.add_done_callback(lambda _finished: reservation.release())
        result = await self._trusted_metadata(result, context)
        if (
            result.error_code == "resource_deferred"
            and result.effect_state == EffectState.NOT_STARTED
        ) or (
            result.control is not None
            and not result.success
            and result.effect_state == EffectState.NO_EFFECT
        ):
            # Fresh capacity/input may permit this same logical call after
            # resume. A denied admission/question cannot become a cached fence.
            self._ledger.pop(key, None)
        else:
            self._ledger[key] = (fingerprint, result)
        return result

    def inspect_effect(
        self, group_id: str, work_id: str, idempotency_key: str
    ) -> ToolReceipt | None:
        entry = self._ledger.get((group_id, work_id, idempotency_key))
        return entry[1] if entry else None

    async def cancel(self, group_id: str, work_id: str) -> None:
        for key, task in tuple(self._active.items()):
            if key[:2] == (group_id, work_id):
                task.cancel()

    async def quiesce(self, scope: ToolScope, *, timeout_seconds: float) -> None:
        self._check_scope(scope)
        active = {
            task
            for key, task in self._active.items()
            if key[:2] == (scope.group_id, scope.work_id) and not task.done()
        }
        for task in active:
            task.cancel()
        if active:
            _, pending = await asyncio.wait(active, timeout=timeout_seconds)
            if pending:
                raise DomainError(
                    "work_busy", "A scoped tool has not acknowledged termination", retryable=True
                )
        for key, task in tuple(self._active.items()):
            if key[:2] == (scope.group_id, scope.work_id) and task.done():
                self._active.pop(key, None)

    def is_quiescent(self, scope: ToolScope) -> bool:
        self._check_scope(scope)
        return not any(
            not task.done()
            for key, task in self._active.items()
            if key[:2] == (scope.group_id, scope.work_id)
        )

    def export_ledger(self, scope: ToolScope) -> tuple[ToolLedgerEntry, ...]:
        self._check_scope(scope)
        return tuple(
            ToolLedgerEntry(fingerprint=fingerprint, receipt=receipt)
            for key, (fingerprint, receipt) in self._ledger.items()
            if key[:2] == (scope.group_id, scope.work_id)
        )

    def restore_ledger(self, scope: ToolScope, entries: tuple[ToolLedgerEntry, ...]) -> None:
        self._check_scope(scope)
        if not self.is_quiescent(scope):
            raise DomainError("work_busy", "A previous tool has not acknowledged termination")
        restored: dict[tuple[str, str, str], tuple[str, ToolReceipt]] = {}
        for entry in entries:
            if entry.receipt.effect_state == EffectState.UNKNOWN:
                raise DomainError("recovery_required", "An unknown effect requires reconciliation")
            key = (scope.group_id, scope.work_id, entry.receipt.idempotency_key)
            value = (entry.fingerprint, entry.receipt)
            previous = self._ledger.get(key)
            if previous is not None and previous != value:
                # Explicit read reconciliation may update an existing gateway
                # as well as a fresh one. Retain the exact failed call and its
                # result; only its business-effect classification can change.
                old = previous[1]
                reconciled_read = (
                    previous[0] == entry.fingerprint
                    and self.is_read_only(old.tool_name)
                    and not old.success
                    and old.effect_state == EffectState.UNKNOWN
                    and bool(old.error_code)
                    and old.error_code != "tool_in_progress"
                    and entry.receipt
                    == old.model_copy(update={"effect_state": EffectState.NO_EFFECT})
                )
                if not reconciled_read:
                    raise DomainError("idempotency_conflict", "Recovered tool ledger differs")
            if key in restored and restored[key] != value:
                raise DomainError("idempotency_conflict", "Recovered tool keys conflict")
            restored[key] = value
        if any(
            key[:2] == (scope.group_id, scope.work_id)
            and receipt.effect_state == EffectState.UNKNOWN
            and key not in restored
            for key, (_, receipt) in self._ledger.items()
        ):
            raise DomainError(
                "recovery_required", "An unrecorded unknown action requires reconciliation"
            )
        self._ledger.update(restored)
