"""Canonical delivery authority, real PG/local delivery, explicit model fixtures.

No live provider, Docker worker, or paid acceptance ledger is used.
"""

import json
from types import SimpleNamespace

import httpx
import pytest

from groupagent.artifacts import ArtifactScope
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import DomainError, WorkState
from groupagent.execution import ModelReply, ModelToolCall, TokenUsage
from groupagent.memory import MemberService
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.executor import WorkSupervisor
from groupagent.runtime.tool_selection import message_only_research_request, select_tools
from groupagent.scheduling import Scheduler

from .conftest import source
from .test_tool_selection import work as catalog_work
from .test_toolkit_contract import candidate, create_document, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

QUESTION = "请联网检索官方资料，简短说明两个操作的区别，提供实际来源链接和访问时间。"
URL = "https://example.com/reference"
WEB_TOOLS = {"request_input", "web_search", "web_fetch", "browser_open"}


@pytest.mark.parametrize(
    "text",
    [
        QUESTION,
        "请访问https://example.com/manual.html，解释这个概念。",
        "请读取https://example.com/readme.md并简短说明内容。",
        "请联网解释示例“save report.md”，给来源链接。",
        '请联网说明这个例子：```python\nsave("result.pdf")\n```并给来源。',
        "Search official docs and briefly explain the return values with actual citations.",
        "请联网解释Markdown文件的用途并给出处。",
    ],
)
def test_information_requests_do_not_authorize_files_via_url_or_quoted_suffixes(text):
    assert message_only_research_request(text)


@pytest.mark.parametrize(
    "text",
    [
        QUESTION + "并生成Markdown文件。",
        "请查阅资料，说明两种方案的差异并导出PDF文档。",
        "Search official sources, explain the difference and export a PDF report.",
        "请联网检索公开文档，说明概念的作用并以PDF格式给我。",
        "请联网检索公开文档，解释概念的作用且导出PDF。",
        "请联网检索并把结果保存为“reference.md”。",
        "请联网检索并以PDF格式输出报告。",
        "请检索后提供一份调查报告。",
        "请联网查阅资料，解释这个概念并用语音回复。",
        "请联网查阅资料，解释这个概念并朗读给我。",
        "请联网查阅资料，简短说明这个概念，请用语音讲给我听。",
        "请联网查阅资料，简短说明这个概念，用音频回复。",
        "请用Python实际计算后检索资料解释结果。",
        "请联网查阅资料并登记团队任务。",
        "请联网查询资料后明天提醒我。",
    ],
)
def test_explicit_file_media_or_actual_execution_requirements_keep_the_original_plan(text):
    assert not message_only_research_request(text)


def test_message_contract_never_falls_back_to_file_tools_for_conflicting_required_tools():
    current = catalog_work(
        QUESTION,
        outputs=[{"format": "text"}],
        research_required=True,
        research_message_only=True,
        required_tools=["artifact_deliver"],
    )
    with pytest.raises(DomainError) as failed:
        select_tools(current, [], WEB_TOOLS | {"artifact_deliver", "workspace_write_text"})
    assert failed.value.code == "permission_denied"


async def admitted(setup, text, *, outputs=None, input_versions=()):
    env = setup.env
    original = source(env, text)
    planned = Decision(
        kind="start_work",
        reason_code="explicit-bad-delivery-fixture",
        goal="Prepare and deliver a Markdown attachment",
        acceptance_criteria=["交付Markdown文件"],
        outputs=outputs or [{"format": "md"}],
        input_versions=list(input_versions),
        directness="direct",
        confidence=1,
    )

    class ExplicitProvider:
        async def decide(self, event, context):
            return planned

    coordinator = ConversationCoordinator(
        setup.toolkit.config,
        env.store,
        lambda actor, event: ExplicitProvider(),
        memory=setup.toolkit.memory,
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    if input_versions:
        context_builder = coordinator._context

        def known_inputs(actor, event):
            context = context_builder(actor, event)
            for version_id in input_versions:
                version = env.registry.get(actor, version_id)
                context["files"].append(
                    {
                        "version_id": version_id,
                        "file_id": version.file_id,
                        "name": version.name,
                        "format": version.format,
                    }
                )
            return context

        coordinator._context = known_inputs
    result = await coordinator.process(original)
    assert result.work_id
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(result.work_id)
        raw = tx.get_decision(original.id)["result"]["decision"]
    return current, original, raw, coordinator


@pytest.mark.postgres
async def test_router_md_goal_and_criteria_cannot_expand_message_research_into_artifacts(
    toolkit_setup, monkeypatch
):
    setup, env = toolkit_setup, toolkit_setup.env
    current, original, raw, _ = await admitted(setup, QUESTION)
    assert raw["outputs"] == [{"format": "md"}] and raw["acceptance_criteria"] == [
        "交付Markdown文件"
    ]
    assert raw["goal"] == "Prepare and deliver a Markdown attachment"
    assert current.goal == QUESTION
    assert current.scope["research_message_only"] is True
    assert current.scope["planner_outputs"] == [{"format": "md"}]
    assert current.scope["outputs"][0]["format"] == "text"
    assert current.required_checks == ["result"]
    assert all("Markdown" not in condition for condition in current.acceptance_criteria)
    models = []

    def research_response(request):
        if request.url.path.endswith("/search"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "url": URL,
                            "title": "Synthetic official source",
                            "snippet": "Two operations",
                            "summary": "Bounded summary",
                            "content": "",
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "title": "Synthetic official source",
                "content": "Operation A returns a new value. Operation B modifies its input and returns None.",
            },
        )

    monkeypatch.setattr(
        setup.toolkit.research._client, "_transport", httpx.MockTransport(research_response)
    )

    class ExplicitResearchModel:
        name = "synthetic-message-research-loop"

        def __init__(self):
            self.calls = 0

        async def complete(self, request):
            objective = json.loads(next(m.content for m in request.messages if m.role == "user"))
            assert objective["goal"] == QUESTION
            assert objective["required_outputs"][0]["format"] == "text"
            assert all("Markdown" not in value for value in objective["acceptance_criteria"])
            self.calls += 1
            if self.calls <= 2:
                name = "web_search" if self.calls == 1 else "web_fetch"
                args = (
                    {"query": "public operation documentation"} if self.calls == 1 else {"url": URL}
                )
                return ModelReply(
                    tool_calls=(ModelToolCall(id=name, name=name, arguments=args),),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=10, output_tokens=5),
                )
            receipt = json.loads(
                next(m.content for m in reversed(request.messages) if m.role == "tool")
            )
            return ModelReply(
                text="Operation A returns a new value; B modifies its input and returns None.\n"
                + receipt["output"]["source"]["citation"],
                usage=TokenUsage(input_tokens=10, output_tokens=5),
            )

    async def critic(request):
        payload = json.loads(request.messages[1].content)
        assert payload["goal"] == QUESTION and not payload["artifacts"]
        assert all("Markdown" not in value for value in payload["criteria"])
        assert payload["research_source_content"]
        return ModelReply(
            text='{"passed":true,"reason":"Canonical message requirements and actual sources verified"}',
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", critic)

    async def factory(work, lease, actor, workspace):
        prepared = await setup.toolkit.prepare(work, lease, actor, workspace)
        assert prepared.request.allowed_tools <= WEB_TOOLS
        assert "web_search" in prepared.request.required_tools
        model = ExplicitResearchModel()
        models.append(model)
        prepared.engine.model = model
        return prepared

    async def never_send_file(*args, **kwargs):
        raise AssertionError("No file is authorized by this message request")

    monkeypatch.setattr(env.channel, "send_file", never_send_file)
    supervisor = WorkSupervisor(
        env.store,
        env.state,
        env.service,
        lambda _: env.actor,
        factory,
        setup.toolkit,
        env.root / "workers",
    )
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    try:
        await supervisor.run_once()
        await dispatcher.tick()
        await supervisor.tick()
        with env.store.transaction(env.actor) as tx:
            completed = tx.get_work(current.id)
            deliveries = tx.conn.execute(
                "SELECT payload_ref,state FROM ga_deliveries WHERE group_id=%s AND work_id=%s",
                (env.actor.group_id, current.id),
            ).fetchall()
        assert completed.state == WorkState.COMPLETED
        assert models[0].calls == 3 and deliveries
        assert all(
            row["payload_ref"].startswith("text:") and row["state"] == "accepted"
            for row in deliveries
        )
        assert not env.registry.outputs(env.actor, current.id, current.goal_revision)
    finally:
        await supervisor.close()


@pytest.mark.postgres
@pytest.mark.parametrize("format_name", ["md", "pdf"])
async def test_explicit_research_file_format_is_preserved(toolkit_setup, format_name):
    setup = toolkit_setup
    text = QUESTION + f"并导出{format_name.upper()}文件。"
    current, _, _, _ = await admitted(setup, text, outputs=[{"format": format_name}])
    assert current.scope["research_message_only"] is False
    assert current.scope["outputs"][0]["format"] == format_name
    assert "artifact:" + format_name in current.required_checks
    assert current.acceptance_criteria == ["交付Markdown文件"]


@pytest.mark.postgres
async def test_message_contract_rejects_an_extra_result_before_critic(toolkit_setup):
    setup, env = toolkit_setup, toolkit_setup.env
    current, _, _, _ = await admitted(setup, QUESTION)
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(current.id, "message-contract-fixture", ttl_seconds=300)
        current = tx.get_work(current.id)
    workspace = env.root / current.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(current, lease, env.actor, workspace)
    assert prepared.request.allowed_tools <= WEB_TOOLS
    extra = env.artifacts.create(
        ArtifactScope(env.actor.group_id, current.id, env.actor.member_id),
        "unrequested.md",
        b"# Synthetic unrequested file\n",
    )
    env.registry.register(env.actor, lease, extra, purpose="result")
    run = SimpleNamespace(work=current, lease=lease, workspace=workspace, prepared=prepared)
    with pytest.raises(DomainError, match="message-only research"):
        await setup.toolkit.validate(current, lease, env.actor, candidate(run), workspace)
    assert not setup.model.requests


@pytest.mark.postgres
@pytest.mark.parametrize(
    "text",
    [
        "请联网查阅资料，解释这个概念并用语音回复。",
        "请联网查阅资料，解释这个概念并朗读给我。",
        "请联网查阅资料，解释这个概念，请用语音讲给我听。",
        "请联网查阅资料，解释这个概念，用音频回复。",
    ],
)
async def test_explicit_speech_research_preserves_audio_and_required_tool(toolkit_setup, text):
    current, _, _, _ = await admitted(toolkit_setup, text, outputs=[{"format": "wav"}])
    assert current.scope["research_message_only"] is False
    assert current.scope["outputs"][0]["format"] == "wav"
    assert current.scope["speech_requested"] is True
    assert "speech_synthesize" in current.scope["required_tools"]


@pytest.mark.postgres
async def test_input_and_later_file_revision_keep_existing_file_authority(toolkit_setup):
    setup, env = toolkit_setup, toolkit_setup.env
    original_run = await prepare(setup, scope={"outputs": [{"format": "md"}]})
    generated = await create_document(original_run)
    assert generated.success
    with_input, _, _, _ = await admitted(
        setup, QUESTION, input_versions=[generated.output["version_id"]]
    )
    assert (
        with_input.scope["research_message_only"] is False
        and with_input.scope["outputs"][0]["format"] == "md"
    )
    assert with_input.input_versions == [generated.output["version_id"]]
    current, first, _, coordinator = await admitted(setup, QUESTION)
    update = source(env, "请把这项工作改为交付Markdown文件，保留研究说明。")

    class ExplicitRevision:
        async def decide(self, event, context):
            return Decision(
                kind="update_work",
                reason_code="explicit-file-revision",
                goal=update.text,
                target_work_id=current.id,
                expected_revision=current.revision,
                outputs=[{"format": "md"}],
                directness="direct",
                confidence=1,
            )

    coordinator.provider_factory = lambda actor, event: ExplicitRevision()
    result = await coordinator.process(update)
    with env.store.transaction(env.actor) as tx:
        changed = tx.get_work(result.work_id)
    assert changed.goal_revision == 2 and changed.scope["outputs"][0]["format"] == "md"
    assert "artifact:md" in changed.required_checks
    assert "artifact_deliver" in select_tools(
        changed, [], WEB_TOOLS | {"artifact_deliver", "document_generate"}
    )
