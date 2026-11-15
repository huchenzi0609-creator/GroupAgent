"""EnginePort backed by the real pinned Harness SDK inside Docker.

Native SDK/MCP/proxy behavior lives in harness_worker. Every paid model call and
every effect returns to this trusted control plane, where existing budgets and
ToolGateway apply. The worker has no provider credential or external network.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from groupagent.domain import ActorContext, DomainError
from groupagent.sandbox import DockerSandbox
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolReceipt
from groupagent.tools.models import successful_required_tools

from .engine import SYSTEM_PROMPT, ScopedToolEngine, _Run
from .errors import ModelRejected
from .models import (
    FinishedToolPayload,
    InputPayload,
    ModelMessage,
    ModelReply,
    ModelRequest,
    ModelToolCall,
    PendingInput,
    ProgressPayload,
    RequestedPayload,
    RunState,
)
from .ports import ModelPort, estimate_input_tokens


@dataclass
class _Bridge:
    process: asyncio.subprocess.Process
    lock: asyncio.Lock

    async def send(self, value: dict) -> None:
        assert self.process.stdin
        async with self.lock:
            if self.process.stdin.is_closing() or self.process.returncode is not None:
                raise DomainError("harness_unavailable", "Harness relay is no longer connected")
            try:
                self.process.stdin.write(json.dumps(value, ensure_ascii=True).encode() + b"\n")
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                raise DomainError("harness_unavailable", "Harness relay disconnected") from None


class HarnessEngine(ScopedToolEngine):
    def __init__(
        self,
        model: ModelPort,
        gateway: ToolGateway,
        runner: DockerSandbox,
        *,
        synthetic_diagnostics: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(model, gateway, **kwargs)
        self.runner = runner
        self.synthetic_diagnostics = synthetic_diagnostics
        self.last_diagnostic: str | None = None
        self._bridges: dict[str, _Bridge] = {}
        self._ready: set[str] = set()
        self._forwarded: dict[str, set[str]] = {}

    async def append_input(
        self,
        run_id: str,
        actor: ActorContext,
        text: str,
        expected_revision: int,
        *,
        input_id: str | None = None,
    ) -> int:
        revision = await super().append_input(
            run_id, actor, text, expected_revision, input_id=input_id
        )
        if run_id in self._ready:
            await self._forward_pending(self._get(run_id))
        return revision

    async def _forward_pending(self, run: _Run) -> None:
        bridge = self._bridges.get(run.request.attempt_id)
        if bridge is None:
            return
        sent = self._forwarded.setdefault(run.request.attempt_id, set())
        for item in tuple(run.pending):
            if item.input_id not in sent:
                sent.add(item.input_id)
                await bridge.send(
                    {
                        "command": "append",
                        "text": f"[groupagent-update:{item.input_id}]\n{item.text}",
                        "revision": item.revision,
                        "input_id": item.input_id,
                    }
                )

    async def _loop(self, run: _Run) -> None:
        metadata = await self.runner.preflight()
        if metadata.get("harness_sdk") != "0.2.2rc2":
            raise DomainError(
                "harness_unavailable", "Image does not contain the pinned Harness SDK"
            )
        name = "groupagent-harness-" + uuid.uuid4().hex
        request = run.request
        for saved in run.backend_state.get("accepted_inputs", []):
            item = PendingInput.model_validate(saved)
            run.accepted_inputs[item.input_id] = item
        if run.backend_state and (
            run.backend_state.get("group_id") != run.scope.group_id
            or run.backend_state.get("work_id") != request.work_id
            or run.backend_state.get("version") != "0.2.2rc2"
        ):
            raise DomainError("recovery_required", "Harness checkpoint does not match this work")
        with tempfile.TemporaryDirectory(prefix="ga-harness-client-") as directory:
            home = Path(directory)
            argv = self.runner._argv(name, scope=run.scope)
            # The hash-pinned native SDK extracts native addons
            # into its temporary cache and dlopens them. Compute workers retain
            # noexec; only this tool-less SDK container gets executable /tmp.
            argv = [
                value.replace("/tmp:rw,noexec,nosuid,nodev,", "/tmp:rw,exec,nosuid,nodev,")
                if value.startswith("/tmp:")
                else value
                for value in argv
            ]
            argv = [
                *argv[:-1],
                "--entrypoint",
                "/usr/local/bin/python3",
                argv[-1],
                "-I",
                "-m",
                "groupagent.execution.harness_worker",
            ]
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=self.runner._env(home),
                limit=16_000_000,
            )
            self.runner.track_container(name, run.scope)
            bridge = _Bridge(process, asyncio.Lock())
            self._bridges[request.attempt_id] = bridge
            stop_reason: str | None = None
            pending_calls: list[ModelToolCall] = []
            reply_revision = run.revision
            try:
                session_id = (
                    "ga-"
                    + hashlib.sha256(
                        (run.scope.group_id + "\0" + request.work_id).encode()
                    ).hexdigest()
                )
                sessions = run.backend_state.get("sessions")
                await bridge.send(
                    {
                        "session_id": session_id,
                        "tools": self.gateway.describe(run.scope),
                        "max_tokens": request.budget.max_output_tokens,
                        "system_prompt": SYSTEM_PROMPT,
                        "prompt": (
                            run.backend_state.get("resume_instruction")
                            or "Continue the accepted objective from the existing checkpoint."
                            if sessions
                            else request.objective
                        ),
                        "sessions": sessions,
                        "synthetic_diagnostics": self.synthetic_diagnostics,
                    }
                )
                assert process.stdout
                while True:
                    if run.cancelled.is_set():
                        await self._finish(run, RunState.CANCELLED, "cancelled")
                        return
                    remaining = request.budget.max_active_seconds - self._usage(run).active_seconds
                    if remaining <= 0:
                        run.safe_to_resume = False
                        await self._finish(run, RunState.PAUSED, "time_budget_exhausted")
                        return
                    reader = asyncio.create_task(process.stdout.readline())
                    cancellation = asyncio.create_task(run.cancelled.wait())
                    done, _ = await asyncio.wait(
                        {reader, cancellation},
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    cancellation.cancel()
                    await asyncio.gather(cancellation, return_exceptions=True)
                    if reader not in done or run.cancelled.is_set():
                        reader.cancel()
                        await asyncio.gather(reader, return_exceptions=True)
                        run.safe_to_resume = False
                        await self._finish(
                            run,
                            RunState.CANCELLED if run.cancelled.is_set() else RunState.PAUSED,
                            "cancelled" if run.cancelled.is_set() else "time_budget_exhausted",
                        )
                        return
                    raw = reader.result()
                    if not raw:
                        raise DomainError(
                            "harness_unavailable", "Harness worker exited before completion"
                        )
                    message = json.loads(raw)
                    if message["type"] == "failed":
                        self.last_diagnostic = message.get("diagnostic")
                        raise DomainError("harness_unavailable", "Harness runtime did not complete")
                    if message["type"] == "input":
                        # Native session_prompt acknowledges enqueue, not model consumption.
                        await self._emit(
                            run,
                            "progress",
                            ProgressPayload(reason="harness_input_queued", usage=self._usage(run)),
                        )
                    elif message["type"] == "request":
                        request_id = message["id"]
                        try:
                            if message["method"] == "model":
                                self._ready.add(request.attempt_id)
                                await self._forward_pending(run)
                                if stop_reason:
                                    raise DomainError(
                                        stop_reason, "Execution stopped at its budget checkpoint"
                                    )
                                for unobserved in pending_calls:
                                    # This MCP-only runtime never reached the host
                                    # gateway for the proposal (for example SDK
                                    # schema rejection). Count it, but invent no effect.
                                    skipped = ToolReceipt(
                                        call_id=unobserved.id,
                                        tool_name=unobserved.name.removeprefix("mcp__groupagent__"),
                                        idempotency_key=f"{run.request.work_id}:{unobserved.id}",
                                        success=False,
                                        effect_state=EffectState.NOT_STARTED,
                                        error_code="sdk_tool_not_started",
                                    )
                                    run.receipts.append(skipped)
                                    run.usage = run.usage.model_copy(
                                        update={"tool_calls": run.usage.tool_calls + 1}
                                    )
                                    await self._emit(
                                        run, "tool_finished", FinishedToolPayload(receipt=skipped)
                                    )
                                pending_calls.clear()
                                response_revision = run.revision
                                reply = await self._relay_model(run, message["payload"])
                                reply_revision = response_revision
                                pending_calls = list(reply.tool_calls)
                                response = self._wire_reply(reply)
                            elif message["method"] == "tool":
                                if run.input_request is not None:
                                    raise DomainError(
                                        "input_requested", "Work awaits the requested input"
                                    )
                                response = await self._relay_tool(
                                    run, message["payload"], pending_calls, reply_revision
                                )
                                if run.input_request is not None:
                                    stop_reason = "input_requested"
                            else:
                                raise DomainError("permission_denied", "Unknown relay operation")
                            await bridge.send({"response_to": request_id, "result": response})
                        except DomainError as exc:
                            stop_reason = exc.code
                            await bridge.send({"response_to": request_id, "error": exc.code})
                    elif message["type"] == "finished":
                        if self.synthetic_diagnostics and message["finish_reason"] != "completed":
                            self.last_diagnostic = json.dumps(
                                {
                                    key: message.get(key)
                                    for key in ("finish_reason", "text", "diagnostics")
                                },
                                ensure_ascii=False,
                            )
                        uncertain_usage = bool(run.backend_state.get("uncertain_model_usage"))
                        run.backend_state = {
                            **{
                                key: value
                                for key, value in run.backend_state.items()
                                if key != "resume_instruction"
                            },
                            "version": "0.2.2rc2",
                            "group_id": run.scope.group_id,
                            "work_id": request.work_id,
                            "sessions": message["sessions"],
                            "accepted_inputs": [
                                item.model_dump(mode="json")
                                for item in run.accepted_inputs.values()
                            ],
                            "uncertain_model_usage": uncertain_usage,
                            "resource_usage": message.get("resource_usage", {}),
                        }
                        # A successful live retry establishes a new execution
                        # boundary without resolving/refunding old unknown
                        # billing. Legacy unsafe checkpoints retain the guard.
                        recovery_pending = run.backend_state.get(
                            "model_recovery_pending", uncertain_usage
                        )
                        run.safe_to_resume = not recovery_pending and not any(
                            r.effect_state == EffectState.UNKNOWN for r in run.receipts
                        )
                        if stop_reason == "input_requested" and run.input_request is not None:
                            await self._finish(
                                run,
                                RunState.WAITING_INPUT,
                                "input_requested",
                                text=run.input_request.question,
                            )
                        elif stop_reason:
                            state = (
                                RunState.FAILED
                                if stop_reason in {"permission_denied", "scope_violation"}
                                else RunState.PAUSED
                            )
                            await self._finish(run, state, stop_reason)
                        elif message["finish_reason"] != "completed":
                            await self._finish(run, RunState.FAILED, "harness_incomplete")
                        elif not isinstance(message["text"], str) or not message["text"].strip():
                            await self._finish(run, RunState.FAILED, "invalid_model_reply")
                        elif run.pending or reply_revision != run.revision:
                            await self._finish(run, RunState.PAUSED, "superseded_model_response")
                        elif run.unresolved_tools or not request.required_tools.issubset(
                            successful_required_tools(run.receipts, request.allowed_tools)
                        ):
                            await self._finish(run, RunState.FAILED, "validation_required")
                        else:
                            await self._finish(run, RunState.CANDIDATE, text=message["text"])
                        return
                    elif message["type"] == "protocol_error":
                        raise DomainError(
                            "harness_protocol_error", "Harness input bridge rejected a message"
                        )
            finally:
                self._bridges.pop(request.attempt_id, None)
                self._ready.discard(request.attempt_id)
                self._forwarded.pop(request.attempt_id, None)
                try:
                    await self.runner.remove_container(home, name)
                finally:
                    if process.returncode is None:
                        process.kill()
                    await process.wait()

    async def _relay_model(self, run: _Run, payload: dict, retry_number: int = 0) -> ModelReply:
        budget = run.request.budget
        if any(key.startswith("dsh_") for key in payload):
            raise DomainError(
                "harness_log_upload_blocked", "Unexpected extra runtime log contribution"
            )
        tools = tuple(payload.get("tools") or ())
        expected = {"mcp__groupagent__" + name for name in run.scope.allowed_tools}
        if {tool["function"]["name"] for tool in tools} != expected:
            raise DomainError(
                "permission_denied", "Harness exposed tools outside the scoped gateway"
            )
        messages = []
        for item in payload["messages"]:
            calls = tuple(
                ModelToolCall(
                    id=call["id"],
                    name=call["function"]["name"],
                    arguments=json.loads(call["function"]["arguments"]),
                )
                for call in item.get("tool_calls") or ()
            )
            content = item.get("content")
            if isinstance(content, list):
                if any(part.get("type") != "text" for part in content):
                    raise DomainError(
                        "unsupported_media", "This Harness relay accepts text observations only"
                    )
                content = "\n".join(part["text"] for part in content)
            messages.append(
                ModelMessage(
                    role=item["role"],
                    content=content,
                    tool_call_id=item.get("tool_call_id"),
                    tool_calls=calls,
                )
            )
        # Native inboxes can wait until their current turn ends. Project trusted
        # accepted updates at the next host model boundary until native history
        # contains their marker. Persist these inputs in the business checkpoint.
        user_text = json.dumps(
            [item.content for item in messages if item.role == "user"], ensure_ascii=False
        )
        for item in run.accepted_inputs.values():
            marker = f"[groupagent-update:{item.input_id}]"
            if marker not in user_text:
                messages.append(ModelMessage(role="user", content=f"{marker}\n{item.text}"))
        consumed, run.pending = run.pending, []
        for item in consumed:
            await self._emit(
                run, "input_consumed", InputPayload(revision=item.revision, input_id=item.input_id)
            )
        request = ModelRequest(
            messages=tuple(messages), tools=tools, max_output_tokens=budget.max_output_tokens
        )
        if len(request.model_dump_json().encode()) > budget.max_input_bytes:
            raise DomainError("input_budget_exhausted", "Harness input exceeds the work limit")
        if run.usage.rounds >= budget.max_rounds:
            raise DomainError("round_budget_exhausted", "Model round budget exhausted")
        if self._usage(run).active_seconds >= budget.max_active_seconds:
            raise DomainError("time_budget_exhausted", "Execution active time budget exhausted")
        estimate = estimate_input_tokens(request)
        allowance = budget.max_output_tokens
        if budget.max_tokens is not None:
            allowance = min(allowance, budget.max_tokens - run.usage.tokens - estimate)
        if allowance < 1:
            raise DomainError("token_budget_exhausted", "Model token budget exhausted")
        request = request.model_copy(update={"max_output_tokens": allowance})
        reservation_id = None
        reserved = estimate + allowance
        run.usage = run.usage.model_copy(
            update={"rounds": run.usage.rounds + 1, "tokens": run.usage.tokens + reserved}
        )
        run.safe_to_resume = False
        await self._save(run)
        if self._model_budget:
            try:
                reservation_id = await self._model_budget.reserve(
                    run.request,
                    round_number=run.usage.rounds,
                    input_tokens=estimate,
                    output_tokens=allowance,
                )
            except Exception:
                run.usage = run.usage.model_copy(
                    update={"rounds": run.usage.rounds - 1, "tokens": run.usage.tokens - reserved}
                )
                raise
        try:
            model_task = asyncio.create_task(self._model_call(run, request))
            worker_exit = asyncio.create_task(self._bridges[run.request.attempt_id].process.wait())
            try:
                done, _ = await asyncio.wait(
                    {model_task, worker_exit}, return_when=asyncio.FIRST_COMPLETED
                )
                if model_task not in done:
                    raise DomainError(
                        "harness_unavailable", "SDK worker exited during a model request"
                    )
                reply = model_task.result()
            finally:
                model_task.cancel()
                worker_exit.cancel()
                await asyncio.gather(model_task, worker_exit, return_exceptions=True)
        except BaseException as exc:
            if not isinstance(exc, ModelRejected):
                run.backend_state["uncertain_model_usage"] = True
                run.backend_state["model_recovery_pending"] = True
            if self._model_budget and reservation_id is not None:
                await self._model_budget.settle(reservation_id, None)
            if retry_number < budget.additional_retry_limit and self._can_retry_model(run, exc):
                await self._wait_retry(run, exc, retry_number + 1)
                return await self._relay_model(run, payload, retry_number + 1)
            raise
        if self._model_budget and reservation_id is not None:
            try:
                await self._model_budget.settle(reservation_id, reply.usage)
            except BaseException:
                run.backend_state["uncertain_model_usage"] = True
                run.backend_state["model_recovery_pending"] = True
                raise
        run.usage = run.usage.model_copy(
            update={"tokens": run.usage.tokens - reserved + reply.usage.total}
        )
        if reply.usage.output_tokens > allowance or reply.usage.input_tokens > estimate:
            raise DomainError("token_estimate_exceeded", "Model usage exceeded its reservation")
        if any(call.name not in expected for call in reply.tool_calls):
            raise DomainError("permission_denied", "Model requested an ungranted Harness tool")
        if len(reply.tool_calls) > budget.max_tool_calls - run.usage.tool_calls:
            raise DomainError("tool_budget_exhausted", "The proposed tool batch exceeds its budget")
        if bool(reply.tool_calls) != (reply.finish_reason == "tool_calls"):
            raise DomainError(
                "invalid_model_reply", "Tool proposals and model finish reason disagree"
            )
        if reply.finish_reason == "length":
            raise DomainError("model_output_truncated", "Model response was truncated")
        if reply.finish_reason == "content_filter":
            raise DomainError("model_response_filtered", "Model response was filtered")
        run.backend_state["model_recovery_pending"] = False
        await self._emit(
            run,
            "progress",
            ProgressPayload(reason="harness_model_returned", usage=self._usage(run)),
        )
        return reply

    async def _relay_tool(
        self, run: _Run, payload: dict, pending: list[ModelToolCall], revision: int
    ) -> dict:
        name, arguments = payload["name"], payload.get("arguments", {})

        def matches(call: ModelToolCall) -> bool:
            if call.name != "mcp__groupagent__" + name:
                return False
            if call.arguments == arguments:
                return True
            try:
                return self.gateway.normalize_arguments(
                    name, call.arguments
                ) == self.gateway.normalize_arguments(name, arguments)
            except (ValueError, DomainError):
                return False

        call = next((call for call in pending if matches(call)), None)
        if call is None:
            raise DomainError(
                "permission_denied", "Tool request has no matching budgeted model proposal"
            )
        pending.remove(call)
        if revision != run.revision or run.pending:
            return {"isError": True, "content": [{"type": "text", "text": "superseded_by_input"}]}
        if name not in self.gateway.control_names and any(
            other.name.removeprefix("mcp__groupagent__") in self.gateway.control_names
            for other in pending
        ):
            # Native MCP calls can arrive concurrently/out of model order. A
            # request for essential input takes priority over every other effect.
            return {"isError": True, "content": [{"type": "text", "text": "awaiting_input"}]}
        if run.usage.tool_calls >= run.request.budget.max_tool_calls:
            raise DomainError("tool_budget_exhausted", "Tool budget exhausted")
        run.usage = run.usage.model_copy(update={"tool_calls": run.usage.tool_calls + 1})
        run.safe_to_resume = False
        await self._save(run)
        key = f"{run.request.work_id}:{call.id}"
        await self._emit(
            run,
            "tool_requested",
            RequestedPayload(
                call_id=call.id,
                name=name,
                revision=run.revision,
                idempotency_key=key,
            ),
        )
        if revision != run.revision or run.pending:
            return {"isError": True, "content": [{"type": "text", "text": "superseded_by_input"}]}
        receipt = await self.gateway.invoke(
            name,
            arguments,
            ToolContext(
                scope=run.scope,
                call_id=call.id,
                idempotency_key=key,
                cancelled=run.cancelled,
                deadline_monotonic=asyncio.get_running_loop().time()
                + run.request.budget.max_active_seconds
                - self._usage(run).active_seconds,
            ),
        )
        run.receipts.append(receipt)
        run.unresolved_tools = {
            item.tool_name for item in run.receipts if item.effect_state == EffectState.UNKNOWN
        }
        await self._emit(run, "tool_finished", FinishedToolPayload(receipt=receipt))
        if receipt.effect_state == EffectState.UNKNOWN:
            raise DomainError("effect_unknown", "Tool effect requires reconciliation")
        if receipt.control is not None:
            run.input_request = receipt.control.input_request
        return {
            "isError": not receipt.success,
            "content": [{"type": "text", "text": receipt.model_dump_json()}],
        }

    @staticmethod
    def _wire_reply(reply: ModelReply) -> dict:
        message: dict[str, Any] = {"role": "assistant", "content": reply.text}
        if reply.tool_calls:
            message["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": json.dumps(call.arguments),
                    },
                }
                for call in reply.tool_calls
            ]
        return {
            "id": "ga-" + uuid.uuid4().hex,
            "object": "chat.completion",
            "created": 0,
            "model": "deepseek-flash",
            "choices": [{"index": 0, "message": message, "finish_reason": reply.finish_reason}],
            "usage": {
                "prompt_tokens": reply.usage.input_tokens,
                "completion_tokens": reply.usage.output_tokens,
                "total_tokens": reply.usage.total,
            },
        }
