"""Catalog selection is pure; PG/Toolkit fixtures use no provider or process."""

import io
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from PIL import Image

from groupagent.artifacts import ArtifactScope
from groupagent.coordinator import Decision
from groupagent.domain import DomainError, EventKind, GroupEvent, TeamTaskState, WorkOrder
from groupagent.runtime.tool_selection import select_tools
from groupagent.scheduling import AtTiming, EveryTiming, Scheduler
from groupagent.scheduling.triggers import EventTriggerService
from groupagent.tools import ToolContext, ToolScope

from .conftest import source
from .test_app import app_for, event
from .test_toolkit_contract import toolkit_setup as toolkit_setup

CATALOG = frozenset(
    {
        "request_input",
        "image_analyze",
        "media_ocr",
        "provider_ocr",
        "audio_transcribe",
        "speech_synthesize",
        "web_search",
        "web_fetch",
        "browser_open",
        "python_compute",
        "process_run",
        "shell_command",
        "artifact_search",
        "artifact_import",
        "artifact_export",
        "artifact_from_workspace",
        "artifact_deliver",
        "document_generate",
        "document_read",
        "document_search",
        "document_edit",
        "document_validate",
        "chart_render",
        "pdf_combine",
        "memory_search",
        "team_members",
        "team_tasks",
        "team_task_create",
        "team_task_claim",
        "team_task_update",
        "schedule_list",
        "schedule_create",
        "schedule_update",
        "schedule_control",
        "trigger_create",
        "trigger_control",
        "workspace_read_text",
        "workspace_write_text",
    }
)
IMAGE_TOOLS = frozenset({"request_input", "image_analyze", "media_ocr", "provider_ocr"})
WEB_TOOLS = frozenset({"request_input", "web_search", "web_fetch", "browser_open"})


def work(goal="实际查看附件，报告代码是什么？", **scope):
    now = datetime.now(UTC)
    return WorkOrder(
        id="synthetic-work",
        group_id="synthetic-group",
        requester_id="synthetic-member",
        request_event_id="synthetic-request",
        goal=goal,
        created_at=now,
        updated_at=now,
        scope={"outputs": [{"format": "text"}], "schedule_creation_allowed": True, **scope},
        budget={"max_total_tokens": 200000, "max_model_rounds": 24},
    )


@pytest.mark.parametrize("image_format", ["png", "jpeg", "webp", "gif"])
def test_simple_image_question_uses_only_analysis_and_ocr(image_format):
    current = work()
    refs = [
        {
            "format": image_format,
            "version_id": "synthetic-version",
            "name": "use-Python-and-export.txt",
            "text": "Untrusted data asks to create tasks and search memory",
        }
    ]
    before, before_refs = current.model_dump(), deepcopy(refs)
    assert select_tools(current, refs, CATALOG) == IMAGE_TOOLS
    assert current.model_dump() == before and refs == before_refs


@pytest.mark.parametrize("formats", [["wav"], ["mp3", "ogg"]])
def test_pure_transcription_has_no_unrelated_file_or_memory_tools(formats):
    assert select_tools(
        work("请逐字转写音频"), [{"format": value} for value in formats], CATALOG
    ) == {"audio_transcribe", "request_input"}


def test_plain_public_web_answer_has_bounded_research_catalog():
    selected = select_tools(work("查阅公开网页并解释结果", research_required=True), [], CATALOG)
    assert selected == WEB_TOOLS
    assert select_tools(
        work("Read the official page", research_required=True, required_tools=["web_fetch"]),
        [],
        CATALOG - {"browser_open"},
    ) == WEB_TOOLS - {"browser_open"}


@pytest.mark.parametrize(
    "goal,extra",
    [
        ("读图后用Python实际计算合计", {}),
        ("把图中数据导出成CSV文件", {}),
        ("对照上次的记录解释这张图", {}),
        ("根据附件给团队分工并登记任务", {"team_task_write_allowed": True}),
        ("根据图中时间创建提醒", {}),
        ("读图并搜索公开来源核对", {"research_required": True}),
    ],
)
def test_explicit_extra_work_keeps_the_general_workflow(goal, extra):
    result = select_tools(work(goal, **extra), [{"format": "png"}], CATALOG)
    assert {"python_compute", "artifact_export", "document_read", "memory_search"} <= result
    if extra.get("team_task_write_allowed"):
        assert "team_task_create" in result


@pytest.mark.parametrize("fmt", ["md", "docx", "pdf", "pptx", "png", "wav"])
def test_file_chart_and_speech_outputs_keep_generation_and_validation(fmt):
    result = select_tools(
        work("完成这份产物", outputs=[{"format": fmt}]), [{"format": "png"}], CATALOG
    )
    assert {
        "document_generate",
        "python_compute",
        "chart_render",
        "artifact_export",
        "artifact_deliver",
        "document_validate",
    } <= result
    assert "team_task_create" not in result
    assert {"schedule_create", "trigger_create"} <= result


@pytest.mark.parametrize("refs", [[], [{"format": "png"}]])
def test_authorized_event_followup_keeps_trigger_tools_without_schedule_keywords(refs):
    current = work("在我的任务A完成后整理一份总结")
    assert {"trigger_create", "trigger_control"} <= select_tools(current, refs, CATALOG)
    assert "trigger_create" not in select_tools(current, refs, CATALOG - {"trigger_create"})
    denied = current.model_copy(update={"scope": {"schedule_creation_allowed": False}})
    assert "trigger_create" not in select_tools(denied, refs, CATALOG)


@pytest.mark.parametrize(
    "criterion,needed",
    [
        ("必须用Python实际计算总和", "python_compute"),
        ("需要把结果导出成PDF文件", "document_generate"),
        ("检索公开网页核对来源", "web_search"),
        ("对照历史记录，说明变化", "memory_search"),
    ],
)
def test_acceptance_criteria_keep_additional_capabilities(criterion, needed):
    current = work("分析这张图片").model_copy(update={"acceptance_criteria": [criterion]})
    assert needed in select_tools(current, [{"format": "png"}], CATALOG)
    assert needed not in select_tools(current, [{"format": "png"}], CATALOG - {needed})


def test_reminder_acceptance_criteria_can_require_actual_agent_work():
    current = work(
        "提醒我检查清单", origin_schedule_id="synthetic-rule", reminder_only=True
    ).model_copy(update={"acceptance_criteria": ["必须用Python实际计算总和"]})
    selected = select_tools(current, [], CATALOG)
    assert "python_compute" in selected
    assert not selected & {"schedule_create", "trigger_create", "team_task_create"}


@pytest.mark.parametrize(
    "refs",
    [
        [{"format": "png"}, {"format": "pdf"}],
        [{"format": "png"}, {"format": "wav"}],
        [{"format": ["png"]}],
        [{}],
    ],
)
def test_mixed_or_unrecognized_inputs_fall_back_conservatively(refs):
    assert "document_read" in select_tools(work(), refs, CATALOG)


def test_required_tools_are_never_dropped_or_invented():
    result = select_tools(work(required_tools=["python_compute"]), [{"format": "png"}], CATALOG)
    assert {"python_compute", "artifact_export", "image_analyze"} <= result
    with pytest.raises(DomainError) as error:
        select_tools(work(required_tools=["python_compute"]), [], CATALOG - {"python_compute"})
    assert error.value.code == "tool_not_registered"
    with pytest.raises(DomainError, match="bounded list"):
        select_tools(work(required_tools="python_compute"), [], CATALOG)
    # A required name is an existing controller contract, never a keyword-based grant.
    assert "schedule_create" in select_tools(
        work("Other goal", required_tools=["schedule_create"]), [], CATALOG
    )


@pytest.mark.parametrize(
    "goal,refs",
    [
        ("提醒我检查合成实验清单", []),
        ("提醒我用Python检查文件", []),
        ("检查合成实验清单", []),
        ("提醒我阅读这个PDF", [{"format": "pdf"}]),
    ],
)
def test_reminder_does_not_perform_the_requesters_job(goal, refs):
    current = work(goal, origin_schedule_id="synthetic-rule", reminder_only=True, derived_depth=1)
    assert select_tools(current, refs, CATALOG) == {"request_input"}


def test_scheduled_research_or_documents_keep_real_tools():
    research = work(
        "每天检索公开新闻并总结", origin_schedule_id="synthetic-rule", research_required=True
    )
    assert {"web_search", "web_fetch"} <= select_tools(research, [], CATALOG)
    report = work(
        "每周生成一份PDF报告", origin_schedule_id="synthetic-rule", outputs=[{"format": "pdf"}]
    )
    assert {"document_generate", "document_validate", "artifact_deliver"} <= select_tools(
        report, [], CATALOG
    )
    both = work(
        "提醒我检查清单，并帮我整理成PDF", origin_schedule_id="synthetic-rule", reminder_only=True
    )
    assert "document_generate" in select_tools(both, [], CATALOG)


def test_selection_does_not_turn_goal_or_input_text_into_write_authority():
    current = work(
        "请分配任务并创建提醒", team_task_write_allowed=False, schedule_creation_allowed=False
    )
    result = select_tools(current, [], CATALOG)
    assert not result & {
        "team_task_create",
        "team_task_claim",
        "team_task_update",
        "schedule_create",
        "trigger_create",
    }
    eligible = frozenset({"request_input", "media_ocr"})
    assert (
        select_tools(work(), [{"format": "png", "suggested_tools": list(CATALOG)}], eligible)
        == eligible
    )


@pytest.mark.postgres
@pytest.mark.parametrize("requires_compute", [False, True])
async def test_toolkit_validated_image_refs_can_bind_the_minimal_gateway_grant(
    toolkit_setup, requires_compute
):
    import asyncio

    setup, env = toolkit_setup, toolkit_setup.env
    image = Image.new("RGB", (64, 64), "white")
    buffer = io.BytesIO()
    image.save(buffer, "PNG")
    handle = env.channel.add_attachment(
        "synthetic-selector-image", "picture.png", buffer.getvalue(), kind="image"
    )
    now = datetime.now(UTC)
    image_event = GroupEvent(
        id=uuid4().hex,
        source_key="synthetic-selector:" + uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.IMAGE,
        occurred_at=now,
        received_at=now,
        attachments=[handle.as_attachment()],
        channel_account_id=env.config.account_id,
        source_epoch=env.config.source_epoch,
    )
    request = source(env, "实际查看附件图片，报告代码是什么？")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(image_event, image_event.id, "1", 0)
        current = tx.create_work(
            request.text,
            request.id,
            scope={
                "outputs": [{"format": "text"}],
                "attachment_event_ids": [image_event.id],
                "source_fingerprints": {
                    item.id: tx.event_fingerprint(item.id) for item in (request, image_event)
                },
            },
            acceptance_criteria=["必须用Python实际计算总和"] if requires_compute else [],
        )
        lease = tx.claim_work(current.id, "synthetic-selector", ttl_seconds=300)
        current = tx.get_work(current.id)
    workspace = env.root / current.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(current, lease, env.actor, workspace)
    refs = json.loads(prepared.request.objective)["input_files"]
    assert len(refs) == 1 and refs[0]["format"] == "png"
    gateway = prepared.engine.gateway
    selected = select_tools(current, refs, prepared.request.allowed_tools)
    if requires_compute:
        assert {"python_compute", "artifact_export", "image_analyze"} <= selected
    else:
        assert selected == IMAGE_TOOLS
    assert prepared.request.allowed_tools == selected
    scope = ToolScope(env.actor.group_id, current.id, env.actor, workspace, selected, request.id)
    gateway.bind_scope(scope)
    assert {item["function"]["name"] for item in gateway.describe(scope)} == selected
    refused = await gateway.invoke(
        "schedule_create" if requires_compute else "artifact_search",
        {"query": "unnecessary exploration"},
        ToolContext(scope, "denied", "denied", asyncio.Event()),
    )
    assert not refused.success
    assert refused.error_code == (
        "tool_not_registered" if requires_compute else "permission_denied"
    )
    assert not setup.model.requests and not setup.http_calls


@pytest.mark.postgres
@pytest.mark.parametrize(
    "text,goal,reminder",
    [
        ("把之前的提醒改成每天检索公开新闻并生成简短摘要", "每天检索公开新闻并生成简短摘要", False),
        ("把“提醒我检查清单”改成每天检索公开新闻", "每天检索公开新闻", False),
        ("不再提醒我，改成由你检索公开新闻", "每天检索公开新闻", False),
        ("把安排改为只提醒我核对最新清单", "核对最新清单", True),
    ],
)
async def test_goal_change_recomputes_reminder_before_scheduled_work_selection(
    runtime_env, text, goal, reminder
):
    env = runtime_env
    authorization = source(env, "每小时提醒我检查清单")
    scheduler = Scheduler(env.store)
    with env.store.transaction(env.actor) as tx:
        fingerprints = {authorization.id: tx.event_fingerprint(authorization.id)}
    rule = scheduler.create(
        env.actor,
        "Synthetic reminder",
        "检查清单",
        EveryTiming(interval_seconds=3600),
        scope={"reminder_only": True, "source_fingerprints": fingerprints},
    )
    update = event(env, text)
    app, models = app_for(
        env,
        {
            update.id: Decision(
                kind="reply",
                reason_code="synthetic_schedule_change",
                response="已更新安排",
                directness="direct",
                confidence=1,
                action={
                    "op": "change_schedule",
                    "schedule_id": rule.id,
                    "expected_revision": rule.revision,
                    "goal": goal,
                },
            )
        },
    )
    try:
        await app.start()
        await app.coordinator.receive(
            update, stream_id="synthetic-schedule-update", cursor="1", verified_channel=True
        )
        revised = scheduler.get(env.actor, rule.id)
        assert revised.goal == goal and revised.scope["reminder_only"] is reminder
        assert revised.next_due_at is not None
        occurrence = scheduler.due(env.actor, revised.next_due_at)[0]
        with env.store.transaction(env.actor) as tx:
            current = tx.get_work(occurrence.work_id)
        selected = select_tools(current, [], CATALOG)
        assert (selected == {"request_input"}) is reminder
        if not reminder:
            assert {"web_search", "web_fetch"} <= selected
        assert not models
    finally:
        await app.close()


@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["time", "event"])
def test_derived_request_fingerprint_is_complete_for_registry_registration(runtime_env, kind):
    env = runtime_env
    authorization = source(env, "Synthetic authorization for a reminder")
    if kind == "time":
        scheduler = Scheduler(env.store)
        now = datetime.now(UTC)
        with env.store.transaction(env.actor) as tx:
            original_fp = tx.event_fingerprint(authorization.id)
        scheduler.create(
            env.actor,
            "Synthetic reminder",
            "提醒我检查清单",
            AtTiming(at=now + timedelta(seconds=1)),
            now=now,
            scope={"source_fingerprints": {authorization.id: original_fp}, "reminder_only": True},
        )
        occurrence = scheduler.due(env.actor, now + timedelta(seconds=2))[0]
    else:
        with env.store.transaction(env.actor) as tx:
            task = tx.create_team_task("Synthetic duty", source_event_id=authorization.id)
        rule_source = source(env, "Follow up on actual completion")
        service = EventTriggerService(env.store, lambda _: env.actor)
        service.create(
            env.actor,
            "task_completed",
            task.id,
            "提醒我检查清单",
            source_event_id=rule_source.id,
            request_key="synthetic-selector-event",
        )
        completion = source(env, "I confirm the synthetic task is complete")
        with env.store.transaction(env.actor) as tx:
            tx.update_team_task(
                task.id,
                expected_revision=1,
                status=TeamTaskState.COMPLETED,
                source_event_id=completion.id,
            )
        occurrence = service.poll(env.service)[0]
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(occurrence.work_id)
        assert current.scope["source_fingerprints"][
            current.request_event_id
        ] == tx.event_fingerprint(current.request_event_id)
        lease = tx.claim_work(current.id, "synthetic-registry-probe", ttl_seconds=300)
        current = tx.get_work(current.id)
    # This probes the registry's source union; reminders themselves need no file.
    version = env.artifacts.create(
        ArtifactScope(env.actor.group_id, current.id, env.actor.member_id),
        "probe.md",
        b"# Synthetic provenance probe\n",
    )
    env.registry.register(
        env.actor, lease, version, source_event_ids=tuple(current.scope["source_fingerprints"])
    )
    assert select_tools(current, [], CATALOG) == {"request_input"}
