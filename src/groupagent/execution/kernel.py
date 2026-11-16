"""The shared agent kernel: LangChain's agent graph, with host-owned authority.

There is exactly one model/tools loop, built by ``create_agent``. The adapters
below translate existing ModelPort messages and persist LangGraph's checkpoint
and pending writes through the caller's existing durable storage boundary.
"""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain.tools import ToolRuntime
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from langchain_core.utils.function_calling import convert_to_openai_tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.errors import GraphDrained
from langgraph.graph.message import REMOVE_ALL_MESSAGES, add_messages
from langgraph.runtime import RunControl
from langsmith import tracing_context
from pydantic import ConfigDict

from groupagent.domain import DomainError

from .models import ModelMessage, ModelReply, ModelRequest, ModelToolCall, TokenUsage
from .ports import ModelPort

Messages = tuple[ModelMessage, ...]
ModelCall = Callable[[ModelRequest], Awaitable[ModelReply]]


class _PortCancelled(Exception):
    """Carry cancellation across BaseChatModel's batched result collection."""


@dataclass(frozen=True)
class KernelTool:
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[[ModelToolCall], Awaitable[str]]


@dataclass(frozen=True)
class KernelDirective:
    """A standard middleware state update, never an independent execution loop."""

    messages: Messages | None = None
    feedback: str | None = None
    jump_to: Literal["model", "tools", "end"] | None = None


@dataclass
class KernelHooks:
    before_model: Callable[[Messages], Awaitable[Messages]] | None = None
    model_call: Callable[[ModelRequest, ModelCall], Awaitable[ModelReply]] | None = None
    after_model: Callable[[ModelReply, Messages], Awaitable[KernelDirective | None]] | None = None
    after_agent: Callable[[ModelReply, Messages], Awaitable[str | None]] | None = None
    visible_tools: Callable[[], frozenset[str]] | None = None
    checkpoint: Callable[[dict[str, Any], Messages], Awaitable[None]] | None = None


@dataclass(frozen=True)
class KernelResult:
    messages: Messages
    reply: ModelReply
    yielded: bool
    checkpoint_state: dict[str, Any]


def to_langchain(messages: Messages) -> list[BaseMessage]:
    converted: list[BaseMessage] = []
    for item in messages:
        if item.role == "system":
            converted.append(SystemMessage(content=item.content or ""))
        elif item.role == "user":
            converted.append(HumanMessage(content=item.content or ""))
        elif item.role == "tool":
            converted.append(
                ToolMessage(content=item.content or "", tool_call_id=item.tool_call_id or "")
            )
        else:
            converted.append(
                AIMessage(
                    content=item.content or "",
                    tool_calls=[
                        {"id": call.id, "name": call.name, "args": call.arguments}
                        for call in item.tool_calls
                    ],
                )
            )
    return converted


def from_langchain(messages: Sequence[BaseMessage]) -> Messages:
    converted = []
    for item in messages:
        content = item.content
        if not isinstance(content, str):
            content = "\n".join(
                part if isinstance(part, str) else str(part.get("text", "")) for part in content
            )
        role = {"human": "user", "ai": "assistant"}.get(item.type, item.type)
        if role not in {"system", "user", "assistant", "tool"}:
            raise DomainError("recovery_required", "Unsupported checkpoint message type")
        converted.append(
            ModelMessage(
                role=cast(Literal["system", "user", "assistant", "tool"], role),
                content=content,
                tool_calls=tuple(
                    ModelToolCall(id=call["id"] or "", name=call["name"], arguments=call["args"])
                    for call in (item.tool_calls if isinstance(item, AIMessage) else ())
                ),
                tool_call_id=item.tool_call_id if isinstance(item, ToolMessage) else None,
            )
        )
    return tuple(converted)


def _reply(message: AIMessage) -> ModelReply:
    saved = message.additional_kwargs.get("groupagent_reply")
    if saved is not None:
        return ModelReply.model_validate(saved)
    # Legacy messages have no provider metadata. They are history, not a new
    # billed completion; zero usage must never become a fabricated charge.
    item = from_langchain([message])[0]
    return ModelReply(
        text=item.content,
        tool_calls=item.tool_calls,
        finish_reason="tool_calls" if item.tool_calls else "stop",
        usage=TokenUsage(input_tokens=0, output_tokens=0),
    )


class _PortChatModel(BaseChatModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    port: Any
    max_output_tokens: int
    timeout_seconds: float
    model_call: Any = None
    bound_tools: tuple[dict[str, Any], ...] = ()

    @property
    def _llm_type(self) -> str:
        return "groupagent-model-port"

    def bind_tools(self, tools, *, tool_choice=None, **kwargs):
        return self.model_copy(
            update={"bound_tools": tuple(convert_to_openai_tool(tool) for tool in tools)}
        )

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise RuntimeError("AgentKernel supports asynchronous inference only")

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        import asyncio

        request = ModelRequest(
            messages=from_langchain(messages),
            tools=self.bound_tools,
            max_output_tokens=self.max_output_tokens,
        )
        try:
            if self.model_call is None:
                reply = await asyncio.wait_for(
                    self.port.complete(request), timeout=self.timeout_seconds
                )
            else:
                reply = await self.model_call(request, self.port.complete)
        except asyncio.CancelledError as error:
            # langchain-core 1.8.1's batch result collector checks Exception,
            # although CancelledError derives from BaseException. Preserve its
            # semantics without allowing it to become an AttributeError.
            raise _PortCancelled() from error
        message = AIMessage(
            content=reply.text or "",
            tool_calls=[
                {"id": call.id, "name": call.name, "args": call.arguments}
                for call in reply.tool_calls
            ],
            additional_kwargs={"groupagent_reply": reply.model_dump(mode="json")},
            usage_metadata={
                "input_tokens": reply.usage.input_tokens,
                "output_tokens": reply.usage.output_tokens,
                "total_tokens": reply.usage.total,
            },
        )
        return ChatResult(generations=[ChatGeneration(message=message)])


class DurableGraphSaver(InMemorySaver):
    """Persist real LangGraph channel state and task writes in business checkpoints.

    The serialized payload uses LangGraph's typed serializer without pickle.
    Only the latest checkpoint is required: this is recovery, not time travel.
    """

    def __init__(self, thread_id: str, callback=None, state=None):
        super().__init__()
        self.thread_id = thread_id
        self.callback = callback
        self.config: RunnableConfig = {
            "configurable": {"thread_id": thread_id, "checkpoint_ns": ""}
        }
        if state:
            self.restore(state)

    def _encode(self, value: Any) -> dict[str, str]:
        kind, data = self.serde.dumps_typed(value)
        return {"type": kind, "data": base64.b64encode(data).decode("ascii")}

    def _decode(self, value: dict[str, str]) -> Any:
        if value["type"] not in {"msgpack", "json", "empty"}:
            raise ValueError("Unsupported durable graph serialization")
        return self.serde.loads_typed(
            (value["type"], base64.b64decode(value["data"], validate=True))
        )

    def export(self) -> dict[str, Any]:
        saved = self.get_tuple(self.config)
        if saved is None:
            return {}
        return {
            "version": 1,
            "thread_id": self.thread_id,
            "checkpoint": self._encode(saved.checkpoint),
            "metadata": self._encode(saved.metadata),
            "pending_writes": self._encode(saved.pending_writes),
        }

    def restore(self, state: dict[str, Any]) -> None:
        try:
            if state["version"] != 1 or state["thread_id"] != self.thread_id:
                raise ValueError("Invalid graph checkpoint identity")
            checkpoint = self._decode(state["checkpoint"])
            metadata = self._decode(state["metadata"])
            writes = self._decode(state["pending_writes"])
            config = self.put(self.config, checkpoint, metadata, checkpoint["channel_versions"])
            grouped = defaultdict(list)
            for task_id, channel, value in writes:
                grouped[task_id].append((channel, value))
            for task_id, task_writes in grouped.items():
                self.put_writes(config, task_writes, task_id)
        except (KeyError, TypeError, ValueError) as exc:
            raise DomainError("recovery_required", "Invalid durable graph checkpoint") from exc

    async def _persist(self) -> None:
        if self.callback:
            saved = self.get_tuple(self.config)
            projected = (
                []
                if saved is None
                else saved.checkpoint.get("channel_values", {}).get("messages", [])
            )
            if saved:
                for _, channel, value in saved.pending_writes or []:
                    if channel == "messages":
                        projected = add_messages(projected, value)
            messages = from_langchain(projected)
            await self.callback(self.export(), messages)
        self._prune()

    def _prune(self) -> None:
        """Retain the latest scheduler boundary and its immediate parent only.

        InMemorySaver normally keeps time-travel history and every old message
        blob. The durable business event log already owns that history. Keeping
        it here would make a long-running Goal consume quadratic memory.
        """
        latest = self.get_tuple(self.config)
        if latest is None:
            return
        current_id = latest.config["configurable"]["checkpoint_id"]
        keep = {current_id}
        if latest.parent_config:
            keep.add(latest.parent_config["configurable"]["checkpoint_id"])
        referenced: set[tuple[str, str, str, Any]] = set()
        for namespace, checkpoints in self.storage[self.thread_id].items():
            for checkpoint_id in list(checkpoints):
                if checkpoint_id not in keep:
                    del checkpoints[checkpoint_id]
                    continue
                checkpoint = self.serde.loads_typed(checkpoints[checkpoint_id][0])
                referenced.update(
                    (self.thread_id, namespace, channel, version)
                    for channel, version in checkpoint["channel_versions"].items()
                )
        for blob_key in list(self.blobs):
            if blob_key[0] == self.thread_id and blob_key not in referenced:
                del self.blobs[blob_key]
        for write_key in list(self.writes):
            if write_key[0] == self.thread_id and write_key[2] not in keep:
                del self.writes[write_key]

    async def aput(self, config, checkpoint, metadata, new_versions):
        result = await super().aput(config, checkpoint, metadata, new_versions)
        await self._persist()
        return result

    async def aput_writes(self, config, writes, task_id, task_path=""):
        await super().aput_writes(config, writes, task_id, task_path)
        await self._persist()


class _HostMiddleware(AgentMiddleware):
    def __init__(self, hooks: KernelHooks):
        self.hooks = hooks

    @hook_config(can_jump_to=["tools"])
    async def abefore_agent(self, state, runtime):
        # Importing a safe legacy checkpoint may begin at an unfinished tool
        # proposal. Route it into the ordinary ToolNode without another model.
        messages = state.get("messages", [])
        last_ai = next((item for item in reversed(messages) if isinstance(item, AIMessage)), None)
        if last_ai is not None and last_ai.tool_calls:
            observed = {
                item.tool_call_id
                for item in messages[messages.index(last_ai) + 1 :]
                if isinstance(item, ToolMessage)
            }
            if any(call["id"] not in observed for call in last_ai.tool_calls):
                return {"jump_to": "tools"}
        return None

    async def abefore_model(self, state, runtime):
        if self.hooks.before_model:
            messages = await self.hooks.before_model(from_langchain(state["messages"]))
            return {"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *to_langchain(messages)]}
        return None

    async def awrap_model_call(self, request, handler):
        if self.hooks.visible_tools:
            visible = self.hooks.visible_tools()
            request = request.override(
                tools=[tool for tool in request.tools if tool.name in visible]
            )
        return await handler(request)

    async def awrap_tool_call(self, request, handler):
        # Keep the standard ToolNode in the graph even for an empty grant. The
        # middleware's registered jump targets and checkpoint topology then
        # remain valid for tool-free foreground/model calls as well.
        return await handler(request)

    @hook_config(can_jump_to=["model", "tools", "end"])
    async def aafter_model(self, state, runtime):
        message = state["messages"][-1]
        reply = _reply(message)
        messages = from_langchain(state["messages"])
        directive = (
            await self.hooks.after_model(reply, messages) if self.hooks.after_model else None
        )
        if directive is None and not reply.tool_calls and self.hooks.after_agent:
            feedback = await self.hooks.after_agent(reply, messages)
            if feedback:
                directive = KernelDirective(feedback=feedback, jump_to="model")
        if directive is None:
            return None
        update: dict[str, Any] = {}
        if directive.messages is not None:
            update["messages"] = [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                *to_langchain(directive.messages),
            ]
        if directive.feedback:
            update.setdefault("messages", []).append(HumanMessage(content=directive.feedback))
        if directive.jump_to:
            update["jump_to"] = directive.jump_to
        return update


class AgentKernel:
    def __init__(self, model: ModelPort):
        self.model = model

    async def invoke(
        self,
        messages: Messages,
        *,
        tools: Sequence[KernelTool] = (),
        max_output_tokens: int = 4096,
        model_timeout_seconds: float = 60,
        hooks: KernelHooks | None = None,
        checkpoint_state: dict[str, Any] | None = None,
        run_control: RunControl | None = None,
        thread_id: str | None = None,
    ) -> KernelResult:
        # Include graph creation, restoration and host state overlays, not just
        # inference. Ambient tracing must never export private checkpoint data.
        with tracing_context(enabled=False):
            return await self._invoke(
                messages,
                tools=tools,
                max_output_tokens=max_output_tokens,
                model_timeout_seconds=model_timeout_seconds,
                hooks=hooks,
                checkpoint_state=checkpoint_state,
                run_control=run_control,
                thread_id=thread_id,
            )

    async def _invoke(
        self,
        messages: Messages,
        *,
        tools: Sequence[KernelTool] = (),
        max_output_tokens: int = 4096,
        model_timeout_seconds: float = 60,
        hooks: KernelHooks | None = None,
        checkpoint_state: dict[str, Any] | None = None,
        run_control: RunControl | None = None,
        thread_id: str | None = None,
    ) -> KernelResult:
        hooks = hooks or KernelHooks()
        thread_id = thread_id or (checkpoint_state or {}).get("thread_id") or uuid.uuid4().hex
        saver = DurableGraphSaver(thread_id, hooks.checkpoint, checkpoint_state)

        def make_tool(spec: KernelTool):
            async def execute(runtime: ToolRuntime, **arguments) -> str:
                return await spec.handler(
                    ModelToolCall(
                        id=runtime.tool_call_id or uuid.uuid4().hex,
                        name=spec.name,
                        arguments=arguments,
                    )
                )

            return StructuredTool.from_function(
                coroutine=execute,
                name=spec.name,
                description=spec.description,
                args_schema=spec.parameters,
            )

        graph = create_agent(
            _PortChatModel(
                port=self.model,
                max_output_tokens=max_output_tokens,
                timeout_seconds=model_timeout_seconds,
                model_call=hooks.model_call,
            ),
            tools=[make_tool(spec) for spec in tools],
            middleware=[_HostMiddleware(hooks)],
            checkpointer=saver,
        )
        config: RunnableConfig = {**saver.config, "recursion_limit": 1_000_000_000}
        graph_input: Any = None if checkpoint_state else {"messages": to_langchain(messages)}
        if checkpoint_state:
            previous = await graph.aget_state(config)
            # The control plane may append validation feedback to a completed
            # candidate. Only a completed graph accepts that new invocation;
            # unfinished graph tasks always resume from their recorded writes.
            previous_messages = from_langchain(previous.values.get("messages", []))
            if not previous.next and messages != previous_messages:
                graph_input = {
                    "messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *to_langchain(messages)]
                }
            elif previous.next and messages != previous_messages:
                # Canonical business history may be amended by the trusted
                # reconciler (receipt classification) or by host validation.
                # Update channels in place; retain the scheduler's next tasks.
                await graph.aupdate_state(
                    config,
                    {"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *to_langchain(messages)]},
                )
        yielded = False
        # Even an inherited LANGSMITH_TRACING setting cannot export private
        # group messages or tools from this application.
        with tracing_context(enabled=False):
            try:
                await graph.ainvoke(
                    graph_input,
                    config,
                    durability="sync",
                    control=run_control,
                )
            except GraphDrained:
                yielded = True
            except _PortCancelled as error:
                raise asyncio.CancelledError() from error
            state = await graph.aget_state(config)
        result_messages = from_langchain(state.values.get("messages", []))
        last = next(
            (
                message
                for message in reversed(state.values.get("messages", []))
                if isinstance(message, AIMessage)
            ),
            None,
        )
        return KernelResult(
            messages=result_messages,
            reply=_reply(last)
            if last
            else ModelReply(text=None, usage=TokenUsage(input_tokens=0, output_tokens=0)),
            yielded=yielded,
            checkpoint_state=saver.export(),
        )
