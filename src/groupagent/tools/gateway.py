"""Allowlisted, audited dispatch for trusted tools. No shell is registered implicitly."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import replace
from typing import Any

from pydantic import ValidationError

from groupagent.domain import DomainError, Role

from .models import (
    EffectState,
    IdempotencyMode,
    PolicyDecision,
    PolicyProvider,
    ToolContext,
    ToolDefinition,
    ToolLedgerEntry,
    ToolReceipt,
    ToolScope,
)
from .paths import scoped_path


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

    def register(self, definition: ToolDefinition[Any, Any]) -> None:
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

    def describe(self, scope: ToolScope) -> tuple[dict[str, Any], ...]:
        self._check_scope(scope)
        return tuple(self._tools[name].describe() for name in sorted(scope.allowed_tools))

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

    async def invoke(
        self, name: str, arguments: dict[str, Any], context: ToolContext
    ) -> ToolReceipt:
        def receipt(
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

        try:
            effective_context = await self.authorize(name, context)
            definition = self._tools[name]
            encoded = json.dumps(arguments, sort_keys=True, allow_nan=False).encode("utf-8")
            if len(encoded) > definition.max_input_bytes:
                return receipt("input_too_large")
            validated = definition.input_model.model_validate(arguments)
            for path_field in definition.input_path_fields:
                value = getattr(validated, path_field)
                if not isinstance(value, str):
                    return receipt("invalid_input")
                scoped_path(context.workspace, value)
        except DomainError as exc:
            return receipt(exc.code)
        except (ValidationError, TypeError, ValueError):
            return receipt("invalid_input")
        except Exception:
            return receipt("policy_unavailable")

        key = (context.group_id, context.work_id, context.idempotency_key)
        fingerprint = hashlib.sha256(name.encode("utf-8") + b"\0" + encoded).hexdigest()
        previous = self._ledger.get(key)
        if previous:
            if previous[0] != fingerprint:
                return receipt("idempotency_conflict")
            return previous[1].model_copy(update={"call_id": context.call_id, "replayed": True})
        if context.cancelled.is_set():
            return receipt("cancelled")
        if context.deadline_monotonic is not None and (
            context.deadline_monotonic <= asyncio.get_running_loop().time()
        ):
            return receipt("time_budget_exhausted")

        uncertain = (
            EffectState.NO_EFFECT
            if definition.idempotency_mode == IdempotencyMode.READ_ONLY
            else EffectState.UNKNOWN
        )
        self._ledger[key] = (fingerprint, receipt("tool_in_progress", uncertain))

        async def execute() -> Any:
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
                    )
        except asyncio.CancelledError:
            result = receipt("cancelled", uncertain)
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

    def export_ledger(self, scope: ToolScope) -> tuple[ToolLedgerEntry, ...]:
        self._check_scope(scope)
        return tuple(
            ToolLedgerEntry(fingerprint=fingerprint, receipt=receipt)
            for key, (fingerprint, receipt) in self._ledger.items()
            if key[:2] == (scope.group_id, scope.work_id)
        )

    def restore_ledger(self, scope: ToolScope, entries: tuple[ToolLedgerEntry, ...]) -> None:
        self._check_scope(scope)
        restored: dict[tuple[str, str, str], tuple[str, ToolReceipt]] = {}
        for entry in entries:
            if entry.receipt.effect_state == EffectState.UNKNOWN:
                raise DomainError("recovery_required", "An unknown effect requires reconciliation")
            key = (scope.group_id, scope.work_id, entry.receipt.idempotency_key)
            value = (entry.fingerprint, entry.receipt)
            if key in self._ledger and self._ledger[key] != value:
                raise DomainError("idempotency_conflict", "Recovered tool ledger differs")
            if key in restored and restored[key] != value:
                raise DomainError("idempotency_conflict", "Recovered tool keys conflict")
            restored[key] = value
        self._ledger.update(restored)
