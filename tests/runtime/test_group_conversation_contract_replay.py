"""Redacted multi-member dialogue through real graph, PG and LocalChannel.

Names/IDs/messages below are synthetic reconstructions of the complete failure
sequence. Model decisions and background states are explicit fixtures, not live
language accuracy or an actual PDF-generation acceptance claim.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from conversation.turn_model_fixture import finish_fixture_reply

from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.domain import EventKind, GroupEvent
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage
from groupagent.memory import DeterministicFixtureSummarizer, MemberService, MemoryService
from groupagent.runtime.dispatch import Dispatcher
from groupagent.scheduling import Scheduler

pytestmark = pytest.mark.postgres
USAGE = TokenUsage(input_tokens=1, output_tokens=1)


def reply(text):
    return ModelReply(text=text, usage=USAGE)


def tool(name, arguments):
    return ModelReply(
        tool_calls=(ModelToolCall(id=uuid4().hex, name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=USAGE,
    )


class DialogueModel:
    name = "explicit-redacted-dialogue-fixture"

    def __init__(self, *steps, recipient="agent", required=(), work_context=False):
        self.steps = list(steps)
        self.normal_requests = []
        self.recipient = recipient

    async def complete(self, request):
        self.normal_requests.append(request)
        if self.recipient != "agent" and not self.steps:
            return tool("finish_turn", {"disposition": "observe"})
        assert self.steps, "Unexpected extra model or repair turn"
        step = self.steps.pop(0)
        result = step(request) if callable(step) else step
        return (
            result if result.text == "我现在去网上搜。" else finish_fixture_reply(request, result)
        )


def observation(request):
    return json.loads(next(m.content for m in reversed(request.messages) if m.role == "tool"))


class Replay:
    def __init__(self, env):
        self.env = env
        self.models = {}
        self.events = []
        self.echoes = set()
        self.config = AppConfig(
            group=GroupConfig(
                enabled=True,
                internal_id=env.actor.group_id,
                external_group_id=env.config.external_group_id,
                owner_ids=[env.actor.member_id],
                bot_member_id=env.actor.member_id,
                shared_account=True,
            )
        )
        self.coordinator = ConversationCoordinator(
            self.config,
            env.store,
            lambda actor, event: ModelDecisionProvider(self.models[event.id]),
            memory=MemoryService(env.store, DeterministicFixtureSummarizer()),
            members=MemberService(env.store),
            scheduler=Scheduler(env.store),
            artifacts=env.registry,
        )
        self.dispatcher = Dispatcher(
            env.state,
            env.service,
            self.actor,
            env.channel,
            env.registry,
            bot_member_id=env.actor.member_id,
            shared_account=True,
        )
        with env.store.transaction(env.actor) as tx:
            for identity, name in ((env.actor.member_id, "主持同学"), ("synthetic-peer", "同学甲")):
                tx.conn.execute(
                    "INSERT INTO ga_members(group_id,member_id,confirmed_name,confirmed_by,membership_state) "
                    "VALUES(%s,%s,%s,%s,'present') ON CONFLICT DO NOTHING",
                    (env.actor.group_id, identity, name, env.actor.member_id),
                )

    def actor(self, member):
        return self.coordinator.actor(self.source("", actor=member, persist=False))

    def source(self, text, *, actor=None, reply_to=None, mentions=(), persist=True, platform=None):
        now = datetime.now(UTC)
        event = GroupEvent(
            id=uuid4().hex,
            source_key=uuid4().hex,
            group_id=self.env.actor.group_id,
            actor_id=actor or self.env.actor.member_id,
            kind=EventKind.QUOTE if reply_to else EventKind.TEXT,
            text=text,
            occurred_at=now,
            received_at=now,
            reply_to=reply_to,
            mentioned_member_ids=tuple(mentions),
            platform_message_id=platform,
            channel_account_id=self.env.config.account_id,
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
        )
        if persist:
            with self.env.store.transaction(self.coordinator.actor(event)) as tx:
                tx.ingest_event(event, "redacted:" + event.id, "1", 0)
            self.events.append(event)
        return event

    def works(self):
        with self.env.store.transaction(self.env.actor) as tx:
            return tx.list_work()

    async def drain(self):
        await self.dispatcher.tick()
        for path in sorted((self.env.root / "channel/outgoing").glob("*.json")):
            body = json.loads(path.read_text())
            receipt = body["receipt"]
            key = receipt["platform_message_id"]
            if key in self.echoes or body["payload"]["kind"] != "text":
                continue
            self.echoes.add(key)
            text = body["payload"]["text"]
            event = self.source(text, platform=key)
            model = DialogueModel()
            self.models[event.id] = model
            result = await self.coordinator.process(event)
            assert result.decision.kind in {"observe", "ignore"}
            assert not model.normal_requests

    async def say(self, text, model, **values):
        event = self.source(text, **values)
        self.models[event.id] = model
        before = len(self.works())
        result = await self.coordinator.process(event)
        assert not model.steps
        await self.drain()
        return event, result, before


async def test_complete_redacted_group_dialogue_preserves_silence_style_and_real_commitments(
    runtime_env,
):
    r = Replay(runtime_env)
    pdf_goal = "实际计算17乘23，制作单页PDF，验证后发到本群。"
    _, pdf, _ = await r.say(
        "Pot，请实际计算17乘23，做一页PDF发来。",
        DialogueModel(
            tool(
                "start_work",
                {
                    "goal": pdf_goal,
                    "work_capabilities": ["compute", "documents"],
                    "outputs": [{"format": "pdf", "exact_units": 1}],
                },
            ),
            reply("我来计算并整理成一页PDF，验证后发来。"),
            required=("start_work",),
        ),
    )
    pdf_id = pdf.work_id
    assert pdf_id and len(r.works()) == 1
    with r.env.store.transaction(r.env.actor) as tx:
        tx.conn.execute("UPDATE ga_work_orders SET state='running' WHERE id=%s", (pdf_id,))

    def current_pdf(request):
        facts = observation(request)["facts"]
        assert facts[0]["id"] == pdf_id and facts[0]["state"] == "running"
        return reply("我在计算17乘23，并把过程整理成一页PDF。")

    await r.say(
        "Pot，你现在正在做什么？一句话。",
        DialogueModel(
            tool(
                "read_work_facts",
                {"selector": {"basis": "ids", "ids": [pdf_id], "states": [], "quantifier": "one"}},
            ),
            current_pdf,
            required=("read_work_facts",),
        ),
    )

    def casual(text):
        def check(request):
            context = json.loads(request.messages[1].content)["context"]
            assert context["trusted_routing"]["recipient"] == "agent"
            assert "短句" in request.messages[0].content
            return reply(text)

        return DialogueModel(check)

    peer, _, _ = await r.say(
        "pot，可以弄你吗？", casual("我在，想试什么直接说就好。"), actor="synthetic-peer"
    )
    await r.say(
        "POT，你好，我是人，请告诉我人是什么，在你有空的时候。",
        casual("人是有复杂思考、情感和社会关系的动物。"),
        actor="synthetic-peer",
    )
    for text, values in (
        ("我用LangChain和LangGraph重写了Pot内核，刚才在测验。", {"reply_to": peer.id}),
        ("同学甲，你先忙，我在调试Pot。", {}),
        (
            "@主持同学 你刚才说的测试是什么？",
            {"actor": "synthetic-peer", "mentions": [r.env.actor.member_id]},
        ),
    ):
        silent = DialogueModel(recipient="other")
        _, result, before = await r.say(text, silent, **values)
        assert result.decision.kind in {"observe", "ignore"} and not result.delivery_id
        assert len(r.works()) == before and len(silent.normal_requests) == 1

    lookup_goal = "公开检索Polaris Studio官网，核实官方下载入口及Windows和Mac安装步骤。"
    promise = "我现在去网上搜。"

    def committed_before_ack(request):
        assert observation(request)["status"] == "committed"
        assert len(r.works()) == 2
        return reply("我已开始核对官网，查清后给你下载入口和安装步骤。")

    searched, search, _ = await r.say(
        "Pot，告诉我如何下载并安装Polaris Studio。",
        DialogueModel(
            reply(promise),
            tool(
                "start_work",
                {
                    "goal": lookup_goal,
                    "work_capabilities": ["public_research"],
                    "outputs": [{"format": "text"}],
                },
            ),
            committed_before_ack,
            required=("start_work",),
        ),
    )
    assert search.work_id and len(r.works()) == 2
    with r.env.store.transaction(r.env.actor) as tx:
        deliveries = tx.list_delivery()
        assert not any(d.payload_ref == "text:" + promise for d in deliveries)
        tx.conn.execute("UPDATE ga_work_orders SET state='running' WHERE id=%s", (search.work_id,))
    await r.say("何意味？", casual("我是在核对官网，不需要你先提供链接。"))

    def lookup_progress(request):
        facts = observation(request)["facts"]
        assert facts[0]["id"] == search.work_id and facts[0]["state"] == "running"
        return reply("正在核对同一项检索，确认后给你官网链接和安装步骤。")

    for question in (
        "你去网上搜一下，搜完告诉我怎么安装，并给官方下载链接。",
        "你搜出来了吗？",
    ):
        _, result, before = await r.say(
            question,
            DialogueModel(
                tool(
                    "read_work_facts",
                    {
                        "selector": {
                            "basis": "ids",
                            "ids": [search.work_id],
                            "states": [],
                            "quantifier": "one",
                        }
                    },
                ),
                lookup_progress,
                required=("read_work_facts",),
            ),
            reply_to=searched.id,
        )
        assert result.work_id == search.work_id and len(r.works()) == before == 2

    # Synthetic completed worker result: validates message delivery integration,
    # not external research/PDF production (those have independent acceptance).
    final_text = "Polaris Studio官网：https://vendor.example/download\n1. 选择自己的系统下载。\n2. 安装后启动并登录。"
    with r.env.store.transaction(r.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET state='completed' WHERE id=ANY(%s)",
            ([pdf_id, search.work_id],),
        )
        tx.enqueue_delivery(
            None,
            "text:" + final_text,
            hashlib.sha256(final_text.encode()).hexdigest(),
            "synthetic-reviewed-research-result",
        )
    await r.drain()
    messages = [
        json.loads(p.read_text())["payload"]["text"]
        for p in (r.env.root / "channel/outgoing").glob("*.json")
    ]
    assert any("https://vendor.example/download" in text for text in messages)
    assert not any(
        "需要我把" in text or "citation" in text.lower() or "UTC" in text for text in messages
    )
    assert not any("**" in text or "## " in text or "```" in text for text in messages)
    assert len(r.works()) == 2
    with r.env.store.transaction(r.env.actor) as tx:
        rows = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_events WHERE group_id=%s", (r.env.actor.group_id,)
        ).fetchone()
        assert rows["n"] >= 20  # Human turns, quiet context and receipt-proven echoes all retained.
