"""Natural dialogue composed with scoped, transactional business operations."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb
from pydantic import JsonValue, ValidationError

from groupagent.config import AppConfig
from groupagent.control import ResolvedWorkSet, WorkSelector
from groupagent.coordinator import (
    CoordinationResult,
    Coordinator,
    Decision,
    DecisionProvider,
    ObserveOnlyProvider,
)
from groupagent.domain import (
    WORK_TRANSITIONS,
    ActorContext,
    DomainError,
    EventKind,
    GroupEvent,
    Role,
    TeamTaskState,
    WorkState,
)
from groupagent.identity import BotIdentity, control_request
from groupagent.memory import MemberService, MemoryService
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.runtime.artifacts import ArtifactRegistry
from groupagent.runtime.input_selection import canonicalize_input_selection
from groupagent.runtime.tool_selection import message_only_research_request
from groupagent.runtime.work_facts import WorkFacts
from groupagent.scheduling import Scheduler
from groupagent.scheduling.reminders import extract_reminder_body, is_time_only_reminder_change
from groupagent.security import redact_credentials
from groupagent.vision_policy import FACE_ID_RESPONSE, requests_face_identification

from .actions import (
    ACTION_ADAPTER,
    AssignTask,
    ChangeSchedule,
    ClaimTask,
    ConfirmName,
    ControlSchedule,
    CorrectMemory,
    CreateSchedule,
    Forget,
    OutputRequirement,
    Participation,
    Quiet,
    Remember,
    TaskBatch,
    UpdateTask,
)
from .commands import is_task_declaration
from .context import ContextBuilder, ContextQuery
from .image_requirements import is_public_image_work, normalize_image_quantity
from .intent import (
    main_request,
    new_work_intent,
    public_image_request_count,
    requests_speech_reply,
    task_write_request,
)
from .participation import fast_observe, outside_quotes, policy_intent, verified_human_addressee
from .policy import apply_quiet_barrier_in_tx, ingress_admission, interpret_policy
from .references import resolve_work_selection
from .work_queries import query_work_in_tx, render_work_query

ProviderFactory = Callable[[ActorContext, GroupEvent], DecisionProvider]
_ERRORS = {
    "feature_disabled": "这项能力目前已关闭，已有资料和任务会保留。",
    "permission_denied": "你目前没有这项操作的权限；可以处理自己的任务，其他分工请由负责人调整。",
    "ambiguous_reference": "有多个可能的对象，请说清楚是哪一项。",
    "not_found": "暂时没有找到对应记录，请补充任务或文件名称。",
    "version_conflict": "相关工作刚有变化，这次没有修改；请以最新状态为准。",
    "quota_exceeded": "这项操作超出了当前额度，我先保留现有安排。",
    "source_unavailable": "相关内容已不可用，我不会继续引用或处理它。",
    "unsupported_schedule": "目前支持一次性、按小时、每日或每周的安排。",
    "invalid_input": "这项操作还缺少清楚的时间或对象，请补充一下。",
    "selection_incomplete": "对象范围尚未完整确定，这次没有执行修改。",
}


def _strings(value: Any) -> list[str]:
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def _work_admission_text(
    required_tools: list[str],
    output_formats: list[str],
    *,
    research_required: bool,
    image_inputs: bool = False,
) -> str:
    if "web_image_import" in required_tools:
        return "我会上网找现成图片，核对清晰度和来源后发到这里。"
    if "python_compute" in required_tools:
        return "我会实际运行 Python 计算，再把结果发回来。"
    if "process_run" in required_tools:
        return "我会实际运行这段程序，再把结果发回来。"
    labels = {
        "docx": "Word",
        "pdf": "PDF",
        "pptx": "PPT",
        "md": "Markdown",
        "png": "图片",
        "jpeg": "图片",
    }
    files = "、".join(dict.fromkeys(labels[value] for value in output_formats if value in labels))
    if image_inputs and files:
        return f"我会读取所选图片，再整理成 {files} 发回来。"
    if image_inputs:
        return "我会先读取所选图片，再给出分析结果。"
    if research_required and files:
        return f"我会先查资料并核对来源，再整理成 {files}，检查后发回来。"
    if research_required:
        return "我会查找资料并核对来源，再把结论发回来。"
    if files:
        return f"我会整理成 {files}，检查内容后发回来。"
    return "这项工作已排队；有结果或需要补充资料时我会回复。"


def _scope_sources(scope: dict[str, Any]) -> set[str]:
    sources = set(_strings(scope.get("source_event_ids")))
    fingerprints = scope.get("source_fingerprints")
    if isinstance(fingerprints, dict):
        sources.update(fingerprints)
    source = scope.get("request_source_id")
    if isinstance(source, str):
        sources.add(source)
    return sources


class ConversationCoordinator(Coordinator):
    def __init__(
        self,
        config: AppConfig,
        store: PostgresStore,
        provider_factory: ProviderFactory,
        *,
        memory: MemoryService,
        members: MemberService,
        scheduler: Scheduler,
        artifacts: ArtifactRegistry | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(config, store, ObserveOnlyProvider())
        self.provider_factory, self.memory, self.members, self.scheduler = (
            provider_factory,
            memory,
            members,
            scheduler,
        )
        self.artifacts, self.clock = artifacts, clock or (lambda: datetime.now(UTC))
        self.identity = BotIdentity.from_runtime(config.runtime)
        self.execution_guard: Callable[..., None] = lambda *args: None
        self.refresh_roster: (
            Callable[[ActorContext, GroupEvent, dict[str, Any]], Awaitable[dict[str, Any] | None]]
            | None
        ) = None
        self.service_actor = ActorContext(
            group_id=config.group.internal_id,
            member_id="service:conversation",
            role=Role.MAINTAINER,
            policy_revision=config.group.policy_revision,
        )

    def current_mode(self) -> str:
        with self.store.transaction(self.service_actor) as tx:
            row = tx.conn.execute(
                "SELECT mode FROM ga_conversation_policy WHERE group_id=%s AND subject_id='*'",
                (tx.group_id,),
            ).fetchone()
        return row["mode"] if row and row["mode"] else self.config.group.participation_mode

    def accepts_event(self, event: GroupEvent) -> bool:
        if self.current_mode() != "disabled":
            return True
        if event.actor_id not in self.config.group.owner_ids:
            return False
        intent = policy_intent(
            event, {"role": "owner", "group_time_zone": self.config.group.time_zone}
        )
        return bool(intent and intent.subject == "group" and intent.operation == "participation")

    def _context(self, actor: ActorContext, event: GroupEvent) -> dict[str, Any]:
        query = ContextQuery.from_event(event, self.identity)
        if control_request(event.text, identity=self.identity) is not None:
            query = replace(
                query, new_request=False, continuation=True, resources=query.resources | {"works"}
            )
        return ContextBuilder(
            self.config,
            self.store,
            self.memory,
            self.members,
            self.scheduler,
            self.artifacts,
            self.clock,
        ).build(actor, event, query)

    def _fast_new_work(self, event: GroupEvent, context: dict) -> Decision | None:
        if (
            event.kind != EventKind.TEXT
            or event.reply_to
            or event.attachments
            or context.get("quoted_work_id")
            or context.get("_quoted_work_id")
            or context.get("role") not in {"owner", "member"}
            or context.get("read_only")
            or fast_observe(event, context) is not None
            or verified_human_addressee(event.text, context) is not None
            or policy_intent(event, context) is not None
        ):
            return None
        identity = BotIdentity.from_context(context)
        intent = new_work_intent(event.text, identity=identity)
        if intent is None:
            return None
        # Only self-contained requests qualify. Missing unrelated history grants
        # no input references; the shared final transaction still asserts the
        # original complete snapshot, role and source below.
        addressed = identity.strip_address(event.text) != event.text.strip()
        return Decision(
            kind="start_work",
            reason_code="explicit_new_work_admission",
            goal=event.text,
            acceptance_criteria=[
                "完成原始请求中的全部内容与约束，以真实执行、产物验证和必需投递回执结案。"
            ],
            outputs=list(intent.outputs),
            work_capabilities=(
                ["public_images"]
                if intent.required_tool == "web_image_import"
                else ["compute"]
                if intent.required_tool in {"python_compute", "process_run"}
                else ["documents"]
                if any(
                    item.get("format") in {"md", "docx", "pdf", "pptx"} for item in intent.outputs
                )
                else []
            ),
            public_image_count=public_image_request_count(event.text, identity=identity)
            if intent.required_tool == "web_image_import"
            else None,
            directness="direct" if addressed else "open",
            confidence=1,
        )

    def _fast_roster_reply(self, event: GroupEvent, context: dict) -> Decision | None:
        status = context.get("roster_status")
        if not isinstance(status, dict) or re.search(
            r"生成|制作|整理成|分配|分工|写入|文件|PDF|Word|PPT", event.text, re.I
        ):
            return None
        if not status.get("fresh"):
            text = (
                "这次群资料刷新还没有成功，暂时只能看到已有记录，不能据此判断你是否设置了群昵称。"
            )
        elif not re.search(r"群成员|名单|群里|有哪些|有谁|所有人", event.text) and re.search(
            r"我的|我.{0,6}(?:昵称|设置)|群昵称|再看看|现在", event.text
        ):
            member = next(
                (m for m in context["members"] if m["member_id"] == context["requester_member_id"]),
                None,
            )
            if member and member["name_source"] == "room_display_name":
                text = f"现在能看见了，你的群昵称是「{member['name']}」。"
            else:
                text = "刚刷新了群资料，微信接口仍未返回你的群昵称；这不代表你没有设置。"
        else:
            members = [m for m in context["members"] if m["membership_state"] != "left"]
            names = [m["name"] for m in members if m["name_source"] == "room_display_name"]
            text = f"刚刷新了群资料，当前可读取到 {len(members)} 位成员。"
            if names:
                text += "群昵称：" + "、".join(names[:30]) + "。"
            unknown = len(members) - len(names)
            if unknown:
                text += f"其中 {unknown} 位的群昵称尚未由微信接口返回。"
        return Decision(
            kind="reply",
            reason_code="refreshed_roster",
            response=text,
            directness="direct",
            confidence=1,
        )

    def _fast_visual_boundary(self, event: GroupEvent, context: dict) -> Decision | None:
        images = any(part.get("kind") == "image" for part in event.attachments)
        images = images or any(
            any(part.get("kind") == "image" for part in row.get("attachments", []))
            for row in context.get("attachment_sources", [])
        )
        if event.kind in {
            EventKind.TEXT,
            EventKind.QUOTE,
            EventKind.IMAGE,
        } and requests_face_identification(event.text, image_context=images):
            return Decision(
                kind="reply",
                reason_code="face_identification_unavailable",
                response=FACE_ID_RESPONSE,
                directness="direct",
                confidence=1,
            )
        return None

    def _fast_identity_reply(self, event: GroupEvent, context: dict) -> Decision | None:
        """Answer only a standalone question about this configured bot's name."""
        if (
            event.kind != EventKind.TEXT
            or event.reply_to
            or event.attachments
            or len(event.text) > 200
            or fast_observe(event, context) is not None
        ):
            return None
        raw = event.text.strip()
        if outside_quotes(raw) != raw or self.identity.strip_address(raw) == raw:
            return None
        main = main_request(raw, identity=self.identity).strip(" ?？。!！")
        if (
            re.fullmatch(
                r"(?:(?:告诉|告知)我)?"
                r"(?:(?:你|您)(?:叫(?:什么|啥)(?:名字)?|是谁)|(?:你|您)的?名字(?:是什么|是啥|叫什么)"
                r"|what(?:'s| is) your name|who are you)"
                r"(?:[，,]\s*(?:请用|用)?(?:一句话|一句|一行)(?:回答|回复)(?:即可|就好)?)?",
                main,
                re.I,
            )
            is None
        ):
            return None
        return Decision(
            kind="reply",
            reason_code="bot_identity_query",
            response=f"我叫{self.identity.name}。",
            directness="direct",
            confidence=1,
        )

    def _fast_fact_reply(
        self, event: GroupEvent, context: dict, actor: ActorContext, *, tx: UnitOfWork | None = None
    ) -> Decision | None:
        """Read one exact, source-bound groupwide record; never infer a fact."""
        if (
            event.kind != EventKind.TEXT
            or event.reply_to
            or event.attachments
            or len(event.text) > 200
            or context.get("context_incomplete")
            or context.get("_topic_id")
        ):
            return None
        raw = event.text.strip()
        if outside_quotes(raw) != raw or self.identity.strip_address(raw) == raw:
            return None
        main = main_request(raw, identity=self.identity)
        main = re.sub(r"^(?:(?:用一句话(?:回答|回复|告诉我))[，,：:\s]*|(?:告诉|告知)我)", "", main)
        main = re.sub(r"^(?:(?:我|你|您)?(?:刚才|之前|此前|已经)?(?:记住|记下|记录)的)", "", main)
        match = re.fullmatch(
            r"([^，,。；;：:\s!?！？]{1,50}?)(?:是什么|是啥|是多少|有多少|多少)"
            r"(?:[，,?？]\s*(?:请用|用)?一句话(?:回答|回复)(?:即可|就好)?)?[?？。!！]*",
            main,
        )
        if match is None:
            return None
        predicate = match[1]
        if re.search(
            r"^(?:我|你|您|他|她|本人|谁)|更正|更新|修改|删除|记住|生成|执行|解释|翻译|并|且|顺便|另外|密钥|密码|口令|凭据|token|password|secret|api[_ -]?key",
            predicate,
            re.I,
        ):
            return None
        if any(
            isinstance(member.get("name"), str) and member["name"] + "的" in predicate
            for member in context.get("members", [])
            if isinstance(member, dict)
        ):
            return None
        hits = [
            item
            for item in context.get("memory", [])
            if item.get("kind") in {"fact", "decision"} and item.get("predicate") == predicate
        ]
        if len(hits) != 1:
            return None
        hit = hits[0]
        value = hit.get("value")
        if value is None or not isinstance(value, (str, int, float, bool)):
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        display = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        if (
            not display
            or len(display) > 100
            or not display.isprintable()
            or display != display.strip()
        ):
            return None
        if redact_credentials(predicate + display) != predicate + display:
            return None
        snapshot = context["_snapshot"]
        original = next((item for item in snapshot["items"] if item["id"] == hit["id"]), None)
        sources = set(hit.get("source_event_ids", []))
        if (
            original is None
            or original.get("incomplete")
            or hit.get("topic_id") is not None
            or hit.get("context_excerpt_truncated")
            or hit.get("source_refs_truncated")
            or not sources
            or not sources.issubset(snapshot["source_fingerprints"])
            or sources != {ref["event_id"] for ref in original["source_refs"]}
            or any(ref["state"] != "active" for ref in original["source_refs"])
            or any(
                original.get(key) != hit.get(key)
                for key in ("predicate", "value", "revision", "kind")
            )
        ):
            return None
        if tx is None:
            with self.store.transaction(actor) as owned:
                self.memory.prepare_in_tx(owned)
                self.memory.assert_snapshot_in_tx(owned, snapshot)
                return self._fast_fact_reply(event, context, actor, tx=owned)
        rows = tx.conn.execute(
            "SELECT id,kind,subject_id,revision,value,source_event_ids FROM ga_memory_facts "
            "WHERE group_id=%s AND topic_id IS NULL AND state='active' AND predicate=%s ORDER BY id LIMIT 2",
            (actor.group_id, predicate),
        ).fetchall()
        if len(rows) != 1:
            return None
        current = rows[0]
        if (
            current["subject_id"] not in {None, actor.member_id, actor.group_id}
            or current["id"] != hit["id"]
            or current["kind"] != hit["kind"]
            or current["revision"] != hit["revision"]
            or set(current["source_event_ids"]) != sources
            or json.dumps(current["value"], ensure_ascii=False)
            != json.dumps(value, ensure_ascii=False)
        ):
            return None
        context["_exact_fact_reply"] = True
        return Decision(
            kind="reply",
            reason_code="exact_known_fact",
            response=f"我记下的{predicate}是{display}。",
            directness="direct",
            confidence=1,
        )

    def _resolve_work_set(self, actor, event, decision, context) -> ResolvedWorkSet:
        readonly = decision.kind == "query_work"
        selector = decision.work_selector
        if selector is None:
            selector = WorkSelector(
                basis="ids",
                ids=(decision.target_work_id or "",),
                requester_scope="any" if readonly or actor.role == Role.OWNER else "self",
            )
        if not readonly:
            target = {
                "cancel": WorkState.CANCELLED,
                "pause": WorkState.PAUSED,
                "resume": WorkState.QUEUED,
                "retry": WorkState.QUEUED,
            }[decision.control or "pause"]
            eligible = (
                {WorkState.PAUSED}
                if decision.control == "resume"
                else {WorkState.FAILED}
                if decision.control == "retry"
                else {
                    state
                    for state, transitions in WORK_TRANSITIONS.items()
                    if target in transitions
                }
            )
            if selector.states and not set(selector.states).issubset(eligible):
                raise DomainError(
                    "invalid_transition", "The selected states cannot accept this command"
                )
            selector = selector.model_copy(
                update={"states": selector.states or tuple(sorted(eligible))}
            )
        resolved = resolve_work_selection(
            actor,
            event,
            selector,
            context.get("_control_works", context.get("_works", [])),
            focus_id=context.get("_focus_work_ids") or context.get("_focus_work_id"),
            quoted_id=context.get("_quoted_work_id"),
            candidates_complete=context.get("_control_works_complete", True),
            readonly=readonly,
        )
        if (
            decision.work_selector is None
            and resolved.refs[0].revision != decision.expected_revision
        ):
            raise DomainError("version_conflict", "The exact work reference is stale")
        return resolved

    def _fast_control(self, event: GroupEvent, context: dict) -> Decision | None:
        if event.kind not in {EventKind.TEXT, EventKind.QUOTE}:
            return None
        command = control_request(event.text, identity=BotIdentity.from_context(context))
        if command is None:
            return None
        selector = WorkSelector(
            basis="quote" if event.reply_to else "focus",
            requester_scope="any" if context.get("role") == "owner" else "self",
        )
        # A short control has no independent definition of 'this work'. Both
        # fast and model paths use the same source-bound reference resolver.
        decision = Decision(
            kind="control_work",
            reason_code="explicit_local_control",
            work_selector=selector,
            control="pause" if command in {"暂停", "先停一下", "停一下"} else "cancel",
            directness="direct",
            confidence=1,
        )
        try:
            self._resolve_work_set(self.actor(event), event, decision, context)
        except DomainError:
            return Decision(
                kind="clarify",
                reason_code="ambiguous_control",
                response="你想停止哪一项工作？可以引用它或说出名称。",
                directness="direct",
                confidence=1,
            )
        return decision

    def _gate(
        self, tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict
    ) -> Decision:
        observed = fast_observe(event, context)
        if observed is not None:
            return observed
        if verified_human_addressee(event.text, context) is not None:
            decision = decision.model_copy(update={"directness": "other"})
        write = decision.action is not None or decision.kind in {
            "start_work",
            "update_work",
            "control_work",
        }
        if event.kind == EventKind.FORWARDED and write:
            return Decision(kind="observe", reason_code="forwarded_data_not_authority")
        if write and any(
            item.get("kind") == "audio_transcription" and item.get("needs_clarification")
            for item in event.attachments
        ):
            return Decision(
                kind="clarify",
                reason_code="uncertain_audio_action",
                response="这段语音里的姓名、数字或时间还不够确定，请用文字补充关键内容。",
                directness="direct",
                confidence=1,
            )
        declaration = is_task_declaration(
            decision.action, tx.actor.role, tx.actor.member_id, context.get("_tasks", [])
        )
        if decision.directness in {"other", "none"} and not declaration:
            return Decision(kind="observe", reason_code="not_addressed_to_agent")
        if write and decision.confidence < 0.85:
            return Decision(
                kind="clarify",
                reason_code="low_confidence_write",
                response="你希望我具体处理哪项任务？请补充一下对象或时间。",
                directness="direct",
                confidence=decision.confidence,
            )
        if tx.actor.role in {Role.OBSERVER, Role.MAINTAINER} and write:
            return Decision(
                kind="reply",
                reason_code="read_only_scope",
                response="你当前可以查询信息，暂时不能执行工具或修改任务。",
                directness="direct",
                confidence=1,
            )
        now = self.clock()
        if decision.kind == "control_work" and decision.control in {"resume", "retry"}:
            mode_row = tx.conn.execute(
                "SELECT mode FROM ga_conversation_policy WHERE group_id=%s AND subject_id='*'",
                (tx.group_id,),
            ).fetchone()
            if mode_row and mode_row["mode"] in {"paused", "disabled"}:
                return Decision(
                    kind="reply",
                    reason_code="group_paused",
                    response="本群处理仍处于暂停状态，需要负责人先恢复全群处理。",
                    directness="direct",
                    confidence=1,
                )
        if not self.config.features.natural_participation and decision.directness in {
            "proactive",
            "open",
        }:
            return Decision(kind="observe", reason_code="natural_participation_disabled")
        is_policy_command = decision.action and decision.action.get("op") in {
            "quiet",
            "participation",
        }
        if not is_policy_command and decision.kind != "control_work":
            admission = ingress_admission(
                tx,
                event,
                self.identity,
                time_zone=self.config.group.time_zone,
                quiet_hours=self.config.group.quiet_hours,
                now=now,
                default_mode=self.config.group.participation_mode,
            )
            if not admission.allowed:
                return Decision(kind="observe", reason_code=admission.reason_code)
        if decision.directness == "proactive" and decision.kind in {
            "reply",
            "clarify",
            "start_work",
        }:
            count = tx.conn.execute(
                "SELECT count(*) AS n FROM ga_participation_log WHERE group_id=%s AND proactive AND created_at>%s::timestamptz-interval '10 minutes'",
                (tx.group_id, now),
            ).fetchone()
            if count and count["n"] >= self.config.conversation.proactive_messages_per_10_minutes:
                return Decision(kind="observe", reason_code="proactive_quota")
        return decision

    def _inputs(
        self, tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict
    ) -> list[str]:
        snapshot = context["_snapshot"]
        allowed = set(snapshot["source_fingerprints"])
        selected = list(decision.input_event_ids)
        for identifier in (event.id, event.reply_to):
            if identifier and identifier in allowed:
                source = tx.get_event(identifier)
                if source.kind in {EventKind.IMAGE, EventKind.AUDIO, EventKind.FILE}:
                    selected.append(identifier)
        selected = list(dict.fromkeys(selected))
        if len(selected) + len(decision.input_versions) > self.config.limits.input_files_per_work:
            raise DomainError("quota_exceeded", "Too many input attachment events")
        if not set(selected).issubset(allowed):
            raise DomainError("permission_denied", "Attachment was not in the request context")
        for identifier in selected:
            if tx.get_event(identifier).content_state != "active":
                raise DomainError("source_unavailable", "Attachment event is unavailable")
        retained = set(snapshot.get("artifact_retained_source_ids", []))
        artifact_request_sources: set[str] = set()
        if decision.input_versions and self.artifacts is None:
            raise DomainError("not_found", "Artifact registry is unavailable")
        if self.artifacts:
            known_versions = {row["version_id"] for row in context.get("files", [])}
            for version_id in decision.input_versions:
                if version_id not in known_versions:
                    raise DomainError(
                        "permission_denied", "Input file was not in the request context"
                    )
                fingerprints, expired = self.artifacts.consume_in_tx(tx, version_id)
                provenance = self.artifacts._row(tx, version_id)["body"].get("provenance", {})
                primary_source = (
                    provenance.get("source_event_id") if isinstance(provenance, dict) else None
                )
                if isinstance(primary_source, str) and primary_source in fingerprints:
                    artifact_request_sources.add(primary_source)
                snapshot["source_fingerprints"].update(fingerprints)
                snapshot.setdefault("source_task_revisions", {}).update(
                    self.artifacts.task_sources_in_tx(tx, version_id)
                )
                retained.update(expired)
        snapshot["artifact_retained_source_ids"] = sorted(retained)
        # A generated artifact's original request may be selected twice: once
        # as its immutable version, once as a purported channel attachment.
        # Keep that request as authorized provenance, without asking the media
        # importer to download a nonexistent attachment. Only the registered
        # primary source qualifies; other dependencies or malformed media do not.
        return [
            identifier
            for identifier in selected
            if not (
                identifier in artifact_request_sources
                and (source := tx.get_event(identifier)).kind in {EventKind.TEXT, EventKind.QUOTE}
                and not source.attachments
            )
        ]

    def _canonical_inputs(
        self, tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict
    ) -> tuple[Decision, list[str]]:
        # First prove every original reference was authorized. Canonicalization
        # never becomes a way to turn an unapproved ID into an approved file.
        channel_ids = self._inputs(tx, event, decision, context)
        if self.artifacts is None or not (channel_ids or decision.input_versions):
            return decision, channel_ids
        versions, channels = canonicalize_input_selection(
            tx, self.artifacts, event, decision.input_versions, channel_ids
        )
        if versions == decision.input_versions and channels == channel_ids:
            return decision, channel_ids
        known = {row["version_id"] for row in context.get("files", [])}
        for identifier in versions:
            if identifier not in known:
                context.setdefault("files", []).append({"version_id": identifier})
        normalized = decision.model_copy(
            update={"input_versions": versions, "input_event_ids": channels}
        )
        result = self._inputs(tx, event, normalized, context)
        tx._audit(
            "input_selection_canonicalized",
            event.id,
            original_versions=len(decision.input_versions),
            original_channel_events=len(channel_ids),
            versions=len(versions),
            channel_events=len(result),
        )
        return normalized, result

    def _apply_action(
        self, tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict
    ) -> tuple[dict, str]:
        action = ACTION_ADAPTER.validate_python(decision.action)
        if event.kind not in {EventKind.TEXT, EventKind.QUOTE, EventKind.AUDIO}:
            raise DomainError(
                "permission_denied", "Data-only messages cannot commit business actions"
            )
        if isinstance(action, TaskBatch):
            results, replies = [], []
            for index, part in enumerate(action.actions):
                identifier = (
                    "action-part:" + hashlib.sha256(f"{event.id}:{index}".encode()).hexdigest()
                )
                derived = event.model_copy(
                    update={
                        "id": identifier,
                        "source_key": identifier,
                        "kind": EventKind.QUOTE,
                        "text": "",
                        "reply_to": event.id,
                        "platform_message_id": None,
                        "attachments": [
                            {"kind": "action_part", "source_event_id": event.id, "index": index}
                        ],
                    }
                )
                tx.ingest_event(derived, identifier, "1", 0)
                context["_snapshot"]["source_fingerprints"][identifier] = tx.event_fingerprint(
                    identifier
                )
                part_result, reply = self._apply_action(
                    tx,
                    derived,
                    decision.model_copy(update={"action": part.model_dump(mode="json")}),
                    context,
                )
                observed = CoordinationResult(
                    event_id=identifier,
                    decision=Decision(kind="observe", reason_code="atomic_action_part"),
                )
                tx.record_decision(
                    identifier,
                    {
                        "kind": "observe",
                        "result": observed.model_dump(mode="json"),
                        "source_event_ids": [event.id],
                    },
                )
                results.append(part_result)
                replies.append(reply)
            return {"actions": results}, "\n".join(replies)
        if isinstance(action, ClaimTask):
            if action.task_id:
                task = tx.claim_team_task(
                    action.task_id,
                    expected_revision=action.expected_revision or 0,
                    source_event_id=event.id,
                )
            else:
                exact = [
                    task
                    for task in tx.find_team_tasks(action.title, limit=100)
                    if task.title == action.title
                    and task.status not in {TeamTaskState.COMPLETED, TeamTaskState.CANCELLED}
                ]
                if len(exact) > 1:
                    raise DomainError("ambiguous_reference", "Multiple matching human tasks")
                if exact:
                    task = exact[0]
                    if task.owner_id not in {None, tx.actor.member_id}:
                        raise DomainError(
                            "permission_denied", "This task already belongs to another member"
                        )
                    if task.owner_id is None:
                        task = tx.claim_team_task(
                            task.id, expected_revision=task.revision, source_event_id=event.id
                        )
                else:
                    task = tx.create_team_task(
                        action.title, due_at=action.due_at, source_event_id=event.id
                    )
            if action.due_note:
                task = tx.update_team_task(
                    task.id,
                    expected_revision=task.revision,
                    progress=(task.progress + "\n" if task.progress else "")
                    + "截止安排："
                    + action.due_note,
                    source_event_id=event.id,
                    append_source_fields={"progress"},
                )
            return {
                "task_id": task.id,
                "revision": task.revision,
            }, f"已记下，你负责「{task.title}」。"
        if isinstance(action, AssignTask):
            member_id = context["_aliases"].get(action.member_id)
            if not member_id:
                raise DomainError("ambiguous_reference", "Assignee was not in the allowed roster")
            member = self.members.in_tx(tx).resolve_unique(tx.actor, action.member_name)
            if member.member_id != member_id or member.membership_state == "left":
                raise DomainError("ambiguous_reference", "Assignee name and identity do not agree")
            if tx.actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Only the owner assigns another member")
            if action.task_id:
                task = tx.update_team_task(
                    action.task_id,
                    expected_revision=action.expected_revision or 0,
                    owner_id=member_id,
                    source_event_id=event.id,
                )
            else:
                task = tx.create_team_task(
                    action.title, owner_id=member_id, due_at=action.due_at, source_event_id=event.id
                )
            if action.due_note:
                task = tx.update_team_task(
                    task.id,
                    expected_revision=task.revision,
                    progress=(task.progress + "\n" if task.progress else "")
                    + "截止安排："
                    + action.due_note,
                    source_event_id=event.id,
                    append_source_fields={"progress"},
                )
            return {
                "task_id": task.id,
                "revision": task.revision,
            }, f"已按你的安排，将「{task.title}」分配给{member.display_name}。"
        if isinstance(action, UpdateTask):
            options = {
                key: getattr(action, key)
                for key in ("title", "progress", "evidence_refs", "due_at")
                if key in action.model_fields_set and getattr(action, key) is not None
            }
            if action.clear_due:
                options["due_at"] = None
            if action.status is not None:
                options["status"] = TeamTaskState(action.status)
            if action.due_note:
                previous = tx.get_team_task(action.task_id)
                progress = options.get("progress", previous.progress) or ""
                options["progress"] = (
                    (progress + "\n" if progress else "") + "截止安排：" + action.due_note
                )
                if action.due_at is None:
                    options["due_at"] = None
            task = tx.update_team_task(
                action.task_id,
                expected_revision=action.expected_revision,
                source_event_id=event.id,
                append_source_fields={"progress"}
                if action.due_note and action.progress is None
                else set(),
                **options,
            )
            return {
                "task_id": task.id,
                "revision": task.revision,
            }, f"「{task.title}」的进展已更新。"
        if isinstance(action, CreateSchedule):
            if not self.config.features.durable_schedules:
                raise DomainError("feature_disabled", "Scheduling is disabled")
            outside = self.identity.strip_address(outside_quotes(event.text))
            reminder_only = bool(
                re.search(r"(?:提醒|通知)(?:我|我们|大家|一下|：|:)", outside)
            ) and not (
                re.search(
                    r"(?:你|帮我|替我|"
                    + self.identity.name_pattern
                    + r").{0,8}(?:实际|执行|生成|制作|搜索|整理成)",
                    outside,
                    re.I,
                )
                or self.identity.additional_request(outside)
            )
            body = (
                extract_reminder_body(event.text, identity=self.identity) if reminder_only else None
            )
            scope = {
                "source_event_ids": list(context["_snapshot"]["source_fingerprints"]),
                "source_fingerprints": context["_snapshot"]["source_fingerprints"],
                "topic_id": context["_topic_id"],
                "reminder_only": reminder_only,
            }
            if body is not None:
                scope.update(reminder_body_literal=True, reminder_body_source_event_id=event.id)
            rule = self.scheduler.in_tx(tx).create(
                tx.actor,
                action.title,
                body if body is not None else action.goal,
                action.timing,
                now=self.clock(),
                time_zone=self.config.group.time_zone,
                valid_until=action.valid_until,
                request_key="chat:" + event.id,
                budget=self.config.execution.model_dump(mode="json"),
                scope=scope,
            )
            time = (
                rule.next_due_at.astimezone(ZoneInfo(rule.time_zone)).strftime("%Y-%m-%d %H:%M")
                if rule.next_due_at
                else "待定"
            )
            return {
                "schedule_id": rule.id,
                "revision": rule.revision,
            }, f"安排好了，下一次是{time}（{rule.time_zone}）；随时可以让我修改或取消。"
        if isinstance(action, ChangeSchedule):
            scheduler = self.scheduler.in_tx(tx)
            previous_schedule = scheduler.get(tx.actor, action.schedule_id)
            values = {
                key: getattr(action, key)
                for key in ("timing", "goal", "valid_until")
                if key in action.model_fields_set
            }
            replacement_body = extract_reminder_body(
                event.text, replacement=True, identity=self.identity
            )
            if replacement_body is not None:
                values["goal"] = replacement_body
            elif previous_schedule.scope.get(
                "reminder_body_literal"
            ) is True and is_time_only_reminder_change(event.text, identity=self.identity):
                # A timing-only request cannot replace its original literal
                # body with a paraphrased goal supplied by the router.
                values.pop("goal", None)
            rule = scheduler.update(
                tx.actor,
                action.schedule_id,
                expected_revision=action.expected_revision,
                now=self.clock(),
                **values,
            )
            scope = {
                **rule.scope,
                "source_event_ids": list(context["_snapshot"]["source_fingerprints"]),
                "source_fingerprints": context["_snapshot"]["source_fingerprints"],
            }
            if replacement_body is not None:
                scope.update(
                    reminder_only=True,
                    reminder_body_literal=True,
                    reminder_body_source_event_id=event.id,
                )
            elif scope.get("reminder_body_literal") is True:
                body_source = scope.get("reminder_body_source_event_id")
                previous_sources = previous_schedule.scope.get("source_fingerprints", {})
                if (
                    not isinstance(body_source, str)
                    or not isinstance(previous_sources, dict)
                    or body_source not in previous_sources
                ):
                    raise DomainError("scope_violation", "Literal reminder source is unavailable")
                scope["source_fingerprints"] = {
                    **context["_snapshot"]["source_fingerprints"],
                    body_source: previous_sources[body_source],
                }
                scope["source_event_ids"] = list(scope["source_fingerprints"])
            if rule.goal != previous_schedule.goal and replacement_body is None:
                # A goal replacement cannot inherit the old reminder hint.
                # Only the current authorized request/goal may re-establish it.
                current_request = re.sub(
                    r"“[^”]*”|『[^』]*』|「[^」]*」|\"[^\"]*\"",
                    "<quoted data>",
                    main_request(event.text, identity=self.identity),
                )
                reminder_text = current_request + "\n" + rule.goal
                scope["reminder_only"] = bool(
                    re.search(r"(?:提醒|通知)(?:我|我们|大家|一下|：|:)", rule.goal)
                    or re.search(
                        r"(?:^|改成|改为|调整为|换成|换为)(?:只|仅|继续|还是)?"
                        r"(?:提醒|通知)(?:我|我们|大家|一下|：|:)",
                        current_request,
                    )
                ) and not bool(
                    re.search(
                        r"(?:不要|不用|不再|取消).{0,5}(?:提醒|通知)"
                        r"|(?:你|帮我|替我|机器人).{0,8}(?:实际|执行|生成|制作|搜索|检索|整理成)"
                        r"|(?:并|另外|同时|顺便).{0,5}(?:帮我|替我|由你|你来|"
                        + self.identity.name_pattern
                        + r")",
                        reminder_text,
                        re.I,
                    )
                )
            tx.conn.execute(
                "UPDATE ga_schedules SET scope=%s WHERE group_id=%s AND id=%s",
                (Jsonb(scope), tx.group_id, rule.id),
            )
            return {"schedule_id": rule.id, "revision": rule.revision}, "已更新这项定时安排。"
        if isinstance(action, ControlSchedule):
            method = getattr(self.scheduler.in_tx(tx), action.command)
            rule = method(tx.actor, action.schedule_id, expected_revision=action.expected_revision)
            return {"schedule_id": rule.id, "revision": rule.revision}, "这项定时安排已" + {
                "pause": "暂停",
                "resume": "恢复",
                "cancel": "取消",
            }[action.command] + "。"
        if isinstance(action, Remember):
            if len(json.dumps(action.value, ensure_ascii=False)) > 2000:
                raise DomainError(
                    "quota_exceeded", "Long-term facts must be concise, not whole chat archives"
                )
            fact = self.memory.in_tx(tx).record_fact(
                tx.actor,
                action.predicate,
                action.value,
                source_event_ids=[event.id],
                explicit=True,
                kind=action.kind,
                topic_id=context["_topic_id"],
                request_key="chat:" + event.id,
            )
            return {
                "fact_id": fact.id,
                "revision": fact.revision,
            }, "记下了，后续会把它作为长期记录；有变化随时告诉我。"
        if isinstance(action, CorrectMemory):
            fact = self.memory.in_tx(tx).correct_fact(
                tx.actor,
                action.fact_id,
                expected_revision=action.expected_revision,
                value=action.value,
                source_event_ids=[event.id],
                explicit=True,
            )
            return {
                "fact_id": fact.id,
                "revision": fact.revision,
            }, "已更正，旧记录不会继续当作当前事实使用。"
        if isinstance(action, Forget):
            if event.id in action.event_ids:
                raise DomainError("invalid_input", "Please identify the earlier source to forget")
            result = self.memory.in_tx(tx).forget(
                tx.actor,
                event_ids=action.event_ids,
                fact_ids=action.fact_ids,
                task_ids=action.task_ids,
                now=self.clock(),
            )
            return {
                "removed_events": len(result.event_ids),
                "removed_facts": len(result.fact_ids),
            }, "已从后续检索中移除相关内容；备份副本会按保留期清理，已发出的群消息不会自动撤回。"
        if isinstance(action, (Quiet, Participation)):
            subject = (
                "*"
                if isinstance(action, Participation) or action.scope == "group"
                else tx.actor.member_id
            )
            if subject == "*" and tx.actor.role != Role.OWNER:
                raise DomainError(
                    "permission_denied", "Only the owner changes group-wide participation"
                )
            if (
                isinstance(action, Quiet)
                and action.until is not None
                and action.until <= self.clock()
            ):
                raise DomainError("invalid_input", "Quiet period must end in the future")
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (tx.group_id, subject),
            )
            if isinstance(action, Quiet):
                tx.conn.execute(
                    "UPDATE ga_conversation_policy SET quiet=%s,quiet_until=%s,quiet_hours=%s,revision=revision+1 WHERE group_id=%s AND subject_id=%s",
                    (action.enabled, action.until, action.hours or [], tx.group_id, subject),
                )
                if action.enabled:
                    apply_quiet_barrier_in_tx(tx, event, subject)
                text = (
                    "我先保持安静；你明确叫我时，我仍会回应。"
                    if action.enabled
                    else "我恢复参与讨论了。"
                )
            else:
                intended = interpret_policy(
                    event, tx.actor, self.identity, time_zone=self.config.group.time_zone
                )
                if (
                    intended is None
                    or intended.operation != "participation"
                    or intended.transition != action.mode
                ):
                    raise DomainError(
                        "invalid_input", "A whole-group mode needs an explicit policy request"
                    )
                tx.conn.execute(
                    "UPDATE ga_conversation_policy SET mode=%s,revision=revision+1 WHERE group_id=%s AND subject_id='*'",
                    (action.mode, tx.group_id),
                )
                if action.mode in {"paused", "disabled"}:
                    for work in tx.list_work(limit=1000):
                        if work.state in {
                            WorkState.QUEUED,
                            WorkState.RUNNING,
                            WorkState.VALIDATING,
                            WorkState.DELIVERING,
                            WorkState.WAITING_EXTERNAL,
                        }:
                            tx.transition_work(
                                work.id,
                                WorkState.PAUSED,
                                expected_revision=work.revision,
                                reason="group_paused",
                            )
                elif action.mode in {"natural", "on_request"}:
                    for work in tx.list_work(state=WorkState.PAUSED, limit=1000):
                        if work.pause_reason == "group_paused":
                            with tx.conn.transaction():
                                try:
                                    tx.transition_work(
                                        work.id,
                                        WorkState.QUEUED,
                                        expected_revision=work.revision,
                                        reason="user_request",
                                    )
                                except DomainError:
                                    pass
                text = "已更新群内参与方式。"
            tx._audit("participation_policy_changed", subject)
            return {"policy_subject": subject}, text
        if isinstance(action, ConfirmName):
            member_id = context["_aliases"].get(action.member_id)
            if not member_id:
                raise DomainError("ambiguous_reference", "Member alias is unavailable")
            member = self.members.in_tx(tx).confirm_name(
                tx.actor, member_id, action.name, expected_revision=action.expected_revision
            )
            return {
                "member_id": member.member_id,
                "revision": member.revision,
            }, "已保存这位成员的姓名例外，群昵称仍优先使用。"
        raise DomainError("invalid_input", "Unknown conversation action")

    def _policy_turn(self, tx: UnitOfWork, event: GroupEvent) -> CoordinationResult | None:
        intent = interpret_policy(
            event, tx.actor, self.identity, time_zone=self.config.group.time_zone
        )
        if intent is None:
            return None
        recipient_context = {
            **self.identity.context(),
            "requester_member_id": tx.actor.member_id,
            "members": [
                {
                    "member_id": member.member_id,
                    "name": member.display_name,
                    "name_source": member.name_source,
                    "membership_state": member.membership_state,
                }
                for member in self.members.in_tx(tx).list(tx.actor)
            ],
        }
        if verified_human_addressee(event.text, recipient_context) is not None:
            return None
        if tx.actor.role not in {Role.OWNER, Role.MEMBER}:
            decision = Decision(
                kind="reply",
                reason_code="read_only_scope",
                response="当前角色只能查询，不能修改参与设置。",
                directness="direct",
                confidence=1,
            )
            outcome = None
        else:
            proposed = Decision(
                kind="reply",
                reason_code="participation_command",
                response="调整参与设置。",
                action=intent.action(),
                directness="direct",
                confidence=1,
            )
            try:
                with tx.conn.transaction():
                    outcome, text = self._apply_action(tx, event, proposed, {})
                # Silence is a committed command, not a conversational promise.
                # Activating it intentionally does not generate an acknowledgement.
                decision = (
                    Decision(kind="observe", reason_code="quiet_enabled", confidence=1)
                    if intent.operation == "quiet" and intent.transition == "pause_proactive"
                    else Decision(
                        kind="reply",
                        reason_code="participation_command_applied",
                        response=text,
                        directness="direct",
                        confidence=1,
                    )
                )
            except (DomainError, ValidationError) as error:
                code = error.code if isinstance(error, DomainError) else "invalid_input"
                decision = Decision(
                    kind="reply",
                    reason_code=code,
                    response=_ERRORS.get(code, "参与设置暂未更新。"),
                    directness="direct",
                    confidence=1,
                )
                outcome = None
        delivery = tx.enqueue_reply(event.id, decision.response) if decision.response else None
        result = CoordinationResult(
            event_id=event.id,
            decision=decision,
            delivery_id=delivery.id if delivery else None,
            action_result=outcome,
        )
        tx.record_decision(
            event.id,
            {
                "kind": decision.kind,
                "result": result.model_dump(mode="json"),
                "source_event_ids": [event.id],
                "source_fingerprints": {event.id: tx.event_fingerprint(event.id)},
            },
        )
        return result

    @staticmethod
    def _model_aliases(value: Any, context: dict[str, Any], field: str | None = None) -> Any:
        """Keep stable channel member IDs on the host side of the model boundary."""
        identity_fields = {
            "actor_id",
            "member_id",
            "owner_id",
            "requester_id",
            "creator_id",
            "created_by",
            "author_id",
            "subject_id",
            "requester_member_id",
        }
        inverse = {identifier: alias for alias, identifier in context.get("_aliases", {}).items()}
        if isinstance(value, str):
            if field not in identity_fields:
                return value
            aliases = context.setdefault("_aliases", {})
            if value in aliases or value == "*":
                return value
            if value not in inverse:
                alias = "member_" + str(len(aliases) + 1)
                while alias in aliases:
                    alias = "member_" + str(int(alias.removeprefix("member_")) + 1)
                aliases[alias] = value
                inverse[value] = alias
            return inverse[value]
        if isinstance(value, list):
            return [ConversationCoordinator._model_aliases(item, context, field) for item in value]
        if isinstance(value, dict):
            return {
                key: ConversationCoordinator._model_aliases(item, context, key)
                for key, item in value.items()
            }
        return value

    def _bind_agent_tools(
        self, actor: ActorContext, event: GroupEvent, context: dict[str, Any]
    ) -> None:
        async def read_facts(selector: WorkSelector, include_evidence: bool) -> dict[str, Any]:
            if context.get("_agent_context_revoked"):
                raise DomainError("source_unavailable", "This turn consumed a withdrawn source")
            candidate = Decision(
                kind="query_work",
                reason_code="agent_fact_read",
                work_selector=selector,
                directness="direct",
                confidence=1,
            )
            with self.store.transaction(actor, trace_id=event.id) as tx:
                self.memory.prepare_in_tx(tx)
                self.execution_guard(tx)
                self.memory.assert_snapshot_in_tx(tx, context["_snapshot"])
                if tx.get_event(event.id).content_state != "active":
                    raise DomainError("source_unavailable", "The human request was withdrawn")
                observed = query_work_in_tx(tx, event, candidate, context, WorkFacts(self.store))
                rows = observed["facts"]
                context["_last_work_fact_read"] = observed
                context["_work_fact_source_ids"] = sorted(
                    set(context.get("_work_fact_source_ids", []))
                    | WorkFacts.source_dependencies_in_tx(tx, tuple(row["id"] for row in rows))
                )
                for row in rows:
                    source_id = row["request_event_id"]
                    context["_snapshot"]["source_fingerprints"][source_id] = tx.event_fingerprint(
                        source_id
                    )
                # A read can reveal a valid reference that was not in the small
                # initial prompt. It grants visibility, never mutation permission.
                for name in ("_works", "_control_works"):
                    current = {row["id"]: row for row in context.get(name, [])}
                    current.update({row["id"]: row for row in rows})
                    context[name] = list(current.values())
                public = self._model_aliases(observed, context)
                current = {row["id"]: row for row in context.get("works", [])}
                current.update({row["id"]: row for row in public["facts"]})
                context["works"] = list(current.values())
                if include_evidence:
                    public["inspection"] = {
                        "scope": "recorded_execution_validation_artifact_and_delivery_evidence",
                        "performed": True,
                        "work_resumed": False,
                        "artifact_contents_opened": False,
                    }
                return public

        async def apply_action(candidate: Decision, tool_call_id: str) -> dict[str, Any]:
            if context.get("_agent_context_revoked"):
                return {"status": "rejected", "code": "source_unavailable", "committed": False}
            try:
                result = self._apply_decision(
                    actor, event, candidate, context, tool_call_id=tool_call_id
                )
            except DomainError as error:
                return {"status": "rejected", "code": error.code, "committed": False}
            context["_agent_applied"] = True
            accepted = bool(result.work_id or result.action_result is not None)
            # Successful reads never enter this function. A prior closed turn is
            # not evidence that a different candidate was committed.
            if result.duplicate:
                with self.store.transaction(actor, trace_id=event.id) as tx:
                    prior: dict[str, Any] = tx.get_decision(event.id) or {}
                digest = hashlib.sha256(candidate.model_dump_json().encode()).hexdigest()
                accepted = (
                    any(
                        receipt.get("candidate_sha256") == digest
                        for receipt in prior.get("action_receipts", [])
                    )
                    and accepted
                )
            return {
                "status": "committed" if accepted else "rejected",
                "committed": accepted,
                "replayed": result.duplicate,
                "tool_call_id": tool_call_id,
                "host_message": result.decision.response,
                "result": self._model_aliases(result.model_dump(mode="json"), context),
            }

        context["_read_work_facts"] = read_facts
        context["_apply_business_action"] = apply_action

    def _finish_agent_response_in_tx(
        self,
        tx: UnitOfWork,
        event: GroupEvent,
        body: dict[str, Any],
        *,
        decision: Decision | None = None,
        context: dict[str, Any] | None = None,
    ) -> CoordinationResult:
        """Close a committed turn exactly once, including after interrupted inference."""
        previous = CoordinationResult.model_validate(body["result"])
        if not body.get("agent_pending_response"):
            return previous.model_copy(update={"duplicate": True})
        if body.get("agent_context_revoked"):
            # The running model still has its pre-forget transcript. Only the
            # source-free committed host acknowledgement may cross this boundary.
            decision = None
        self.memory.prepare_in_tx(tx)
        self.execution_guard(tx)
        snapshot = context["_snapshot"] if context is not None else body.get("agent_snapshot", {})
        try:
            self.memory.assert_snapshot_in_tx(tx, snapshot)
            if tx.get_event(event.id).content_state != "active":
                raise DomainError("source_unavailable", "The request was withdrawn")
        except DomainError as error:
            # Committed actions remain facts. Revoked context cannot authorize a
            # delayed reply, even if inference had already produced its text.
            final = Decision(kind="observe", reason_code=error.code, confidence=1)
        else:
            if decision is not None and decision.kind in {"observe", "ignore"}:
                final = decision
            else:
                text = decision.response if decision is not None else None
                if text is None:
                    acknowledgements = [
                        receipt.get("result", {}).get("decision", {}).get("response")
                        for receipt in body.get("action_receipts", [])
                    ]
                    text = "\n".join(dict.fromkeys(value for value in acknowledgements if value))
                final = (
                    previous.decision.model_copy(update={"response": text})
                    if text and previous.decision.kind not in {"ignore", "observe"}
                    else Decision(kind="observe", reason_code="agent_turn_silent", confidence=1)
                )
        admission = ingress_admission(
            tx,
            event,
            self.identity,
            time_zone=self.config.group.time_zone,
            quiet_hours=self.config.group.quiet_hours,
            now=self.clock(),
            default_mode=self.config.group.participation_mode,
        )
        if not admission.allowed:
            final = Decision(kind="observe", reason_code=admission.reason_code, confidence=1)
        delivery_id = previous.delivery_id
        if final.response and final.kind not in {"ignore", "observe"}:
            delivery = tx.enqueue_reply(
                event.id,
                final.response,
                read_only_authorized=tx.actor.role.value in self.config.group.read_only_qa_roles,
            )
            delivery_id = delivery.id
            tx.conn.execute(
                "INSERT INTO ga_participation_log(group_id,event_id,proactive,reason,created_at,invocation_root_event_id) "
                "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    tx.group_id,
                    event.id,
                    final.directness == "proactive",
                    final.reason_code,
                    self.clock(),
                    admission.invocation_root_event_id,
                ),
            )
        controlled = list(
            dict.fromkeys(
                identifier
                for receipt in body.get("action_receipts", [])
                for identifier in receipt.get("result", {}).get("controlled_work_ids", [])
            )
        )
        associated = list(
            dict.fromkeys(
                identifier
                for receipt in body.get("action_receipts", [])
                for identifier in [receipt.get("result", {}).get("work_id")]
                if isinstance(identifier, str) and identifier
            )
        )
        associated = list(dict.fromkeys([*associated, *controlled]))
        action_result = previous.action_result
        if associated:
            associated_values: list[JsonValue] = [identifier for identifier in associated]
            action_result = {**(action_result or {}), "work_ids": associated_values}
        associated_work_id = associated[0] if len(associated) == 1 else previous.work_id
        result = previous.model_copy(
            update={
                "decision": final,
                "delivery_id": delivery_id,
                "controlled_work_ids": controlled,
                "work_id": associated_work_id,
                "action_result": action_result,
            }
        )
        body = {
            **body,
            "kind": final.kind,
            "result": result.model_dump(mode="json"),
            "agent_pending_response": False,
        }
        if context is not None:
            body["source_event_ids"] = sorted(
                set(body.get("source_event_ids", []))
                | set(context.get("_work_fact_source_ids", []))
                | set(context["_snapshot"]["source_fingerprints"])
            )
            body["source_fingerprints"] = {
                **body.get("source_fingerprints", {}),
                **context["_snapshot"]["source_fingerprints"],
            }
        tx.conn.execute(
            "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
            (Jsonb(body), tx.group_id, event.id),
        )
        return result

    def _finish_agent_response(
        self,
        actor: ActorContext,
        event: GroupEvent,
        *,
        decision: Decision | None = None,
        context: dict[str, Any] | None = None,
    ) -> CoordinationResult:
        with self.store.transaction(actor, trace_id=event.id) as tx:
            body = tx.get_decision(event.id)
            if body is None:
                raise DomainError("not_found", "No committed agent action can be finalized")
            return self._finish_agent_response_in_tx(
                tx, event, body, decision=decision, context=context
            )

    async def process(self, event: GroupEvent) -> CoordinationResult:
        self.execution_guard()
        actor = self.actor(event)
        with self.store.transaction(actor, trace_id=event.id) as tx:
            event = tx.get_event(event.id)
            if event.actor_id != actor.member_id:
                raise DomainError("permission_denied", "Canonical event identity differs")
            if getattr(event, "content_state", "active") != "active":
                return CoordinationResult(
                    event_id=event.id,
                    decision=Decision(kind="ignore", reason_code="source_unavailable"),
                    duplicate=True,
                )
            previous = tx.get_decision(event.id)
            if previous:
                if previous.get("agent_pending_response"):
                    return self._finish_agent_response_in_tx(tx, event, previous)
                return CoordinationResult.model_validate(previous["result"]).model_copy(
                    update={"duplicate": True}
                )
            echo = bool(
                event.platform_message_id
                and (
                    tx.is_bot_message(event.platform_message_id)
                    or tx.conn.execute(
                        "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND platform_message_id=%s AND state='accepted' LIMIT 1",
                        (actor.group_id, event.platform_message_id),
                    ).fetchone()
                )
            )
            if echo and event.kind != EventKind.RECALL:
                # A durable native receipt is enough to discard our own text;
                # it needs no history, model call or generated-reply context.
                self.execution_guard(tx)
                result = CoordinationResult(
                    event_id=event.id,
                    decision=Decision(kind="ignore", reason_code="verified_bot_receipt"),
                )
                tx.record_decision(
                    event.id, {"kind": "ignore", "result": result.model_dump(mode="json")}
                )
                return result
            if self.config.group.bot_member_id == event.actor_id:
                if not self.config.group.shared_account:
                    result = CoordinationResult(
                        event_id=event.id,
                        decision=Decision(kind="ignore", reason_code="unshared_bot_account"),
                    )
                    tx.record_decision(
                        event.id, {"kind": "ignore", "result": result.model_dump(mode="json")}
                    )
                    return result
                if (
                    control_request(event.text, identity=self.identity) is None
                    and tx.conn.execute(
                        "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND state IN ('submitted','unknown') LIMIT 1",
                        (tx.group_id,),
                    ).fetchone()
                ):
                    raise DomainError(
                        "echo_reconciliation_pending",
                        "Awaiting send identity reconciliation",
                        retryable=True,
                    )
            policy_result = self._policy_turn(tx, event)
            if policy_result is not None:
                self.execution_guard(tx)
                return policy_result
            if event.kind not in {EventKind.SYSTEM, EventKind.MEMBERSHIP, EventKind.RECALL}:
                admission = ingress_admission(
                    tx,
                    event,
                    self.identity,
                    time_zone=self.config.group.time_zone,
                    quiet_hours=self.config.group.quiet_hours,
                    now=self.clock(),
                    default_mode=self.config.group.participation_mode,
                )
                if not admission.allowed:
                    result = CoordinationResult(
                        event_id=event.id,
                        decision=Decision(kind="observe", reason_code=admission.reason_code),
                    )
                    tx.record_decision(
                        event.id, {"kind": "observe", "result": result.model_dump(mode="json")}
                    )
                    return result
        if event.kind == EventKind.RECALL:
            self.memory.recall(self.service_actor, event.id, now=self.clock())
        context = self._context(actor, event)
        if self.refresh_roster is not None and not echo:
            roster_status = await self.refresh_roster(actor, event, context)
            if roster_status is not None:
                context = self._context(actor, event)
                context["roster_status"] = roster_status
        fast_admitted = False
        if echo:
            decision = Decision(kind="ignore", reason_code="verified_bot_receipt")
        elif (event.kind == EventKind.AUDIO and not self.config.features.audio_transcription) or (
            event.kind == EventKind.IMAGE and not self.config.features.image_understanding
        ):
            decision = (
                Decision(
                    kind="reply",
                    reason_code="feature_disabled",
                    response="这类媒体的理解功能目前已关闭，可以发文字说明。",
                    directness="direct",
                    confidence=1,
                )
                if re.search(r"请|帮|看看|分析|识别|转写", event.text)
                else Decision(kind="observe", reason_code="feature_disabled")
            )
        elif event.kind == EventKind.UNSUPPORTED_VIDEO:
            requested = bool(re.search(r"分析|看看|内容|总结|识别", event.text))
            decision = (
                Decision(
                    kind="reply",
                    reason_code="unsupported_video",
                    response="暂时不支持视频分析，可以发截图或文字说明。",
                    directness="direct",
                    confidence=1,
                )
                if requested
                else Decision(kind="observe", reason_code="unsupported_video")
            )
        elif event.kind in {EventKind.SYSTEM, EventKind.MEMBERSHIP, EventKind.RECALL}:
            decision = Decision(kind="observe", reason_code="non_dialogue_event")
        elif (
            actor.role in {Role.OBSERVER, Role.MAINTAINER}
            and actor.role.value not in self.config.group.read_only_qa_roles
        ):
            decision = Decision(kind="observe", reason_code="read_only_qa_not_authorized")
        else:
            if (
                self.current_mode() in {"paused", "disabled"}
                and policy_intent(
                    event,
                    {"role": actor.role.value, "group_time_zone": self.config.group.time_zone},
                )
                is None
            ):
                decision = self._fast_control(event, context) or Decision(
                    kind="observe", reason_code="group_paused"
                )
            else:
                provider = self.provider_factory(actor, event)
                immediate = self._fast_control(event, context)
                if immediate is None:
                    immediate = self._fast_roster_reply(event, context)
                if immediate is None:
                    immediate = self._fast_visual_boundary(event, context)
                if immediate is None:
                    immediate = self._fast_identity_reply(event, context)
                if immediate is None:
                    immediate = self._fast_fact_reply(event, context, actor)
                if getattr(provider, "uses_agent_kernel", False):
                    self._bind_agent_tools(actor, event, context)
                else:
                    if immediate is None:
                        immediate = self._fast_new_work(event, context)
                        fast_admitted = immediate is not None
                try:
                    decision = immediate or await provider.decide(event, context)
                except Exception:
                    if context.get("_agent_applied"):
                        return self._finish_agent_response(actor, event, context=context)
                    raise
        if context.get("_agent_applied"):
            return self._finish_agent_response(actor, event, decision=decision, context=context)
        return self._apply_decision(actor, event, decision, context, fast_admitted=fast_admitted)

    def _apply_decision(
        self,
        actor: ActorContext,
        event: GroupEvent,
        decision: Decision,
        context: dict[str, Any],
        *,
        fast_admitted: bool = False,
        tool_call_id: str | None = None,
    ) -> CoordinationResult:
        """The single typed write boundary, also used by foreground graph tools.

        Tool actions persist a receipt before returning to the model. They leave
        delivery pending, so no database transaction spans model inference.
        """
        with self.store.transaction(actor, trace_id=event.id) as tx:
            self.memory.prepare_in_tx(tx)
            self.execution_guard(tx)
            previous: dict[str, Any] | None = tx.get_decision(event.id)
            candidate_digest = hashlib.sha256(decision.model_dump_json().encode()).hexdigest()
            if previous:
                if tool_call_id and previous.get("agent_pending_response"):
                    for receipt in previous.get("action_receipts", []):
                        if receipt.get("tool_call_id") == tool_call_id:
                            if receipt.get("candidate_sha256") != candidate_digest:
                                raise DomainError(
                                    "invalid_input", "A tool call ID cannot change its action"
                                )
                            return CoordinationResult.model_validate(receipt["result"]).model_copy(
                                update={"duplicate": True}
                            )
                        if receipt.get("candidate_sha256") == candidate_digest:
                            # Bind a second call ID to the same receipt as well;
                            # it must never be reusable for a different action.
                            previous["action_receipts"] = [
                                *previous["action_receipts"],
                                {**receipt, "tool_call_id": tool_call_id},
                            ]
                            tx.conn.execute(
                                "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
                                (Jsonb(previous), actor.group_id, event.id),
                            )
                            return CoordinationResult.model_validate(receipt["result"]).model_copy(
                                update={"duplicate": True}
                            )
                else:
                    return CoordinationResult.model_validate(previous["result"]).model_copy(
                        update={"duplicate": True}
                    )
            self.memory.assert_snapshot_in_tx(tx, context["_snapshot"])
            if (
                context.get("_exact_fact_reply")
                and self._fast_fact_reply(event, context, actor, tx=tx) != decision
            ):
                raise DomainError(
                    "memory_changed", "Exact fact changed before its reply was committed"
                )
            if event.platform_message_id and (
                tx.is_bot_message(event.platform_message_id)
                or tx.conn.execute(
                    "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND platform_message_id=%s AND state='accepted' LIMIT 1",
                    (actor.group_id, event.platform_message_id),
                ).fetchone()
            ):
                decision = Decision(kind="ignore", reason_code="verified_bot_receipt")
            decision = self._gate(tx, event, decision, context)
            work_id = delivery_id = None
            controlled_work_ids: list[str] = []
            action_result = None
            if (
                tool_call_id is None
                and decision.kind == "reply"
                and context.get("_last_work_fact_read")
            ):
                observed = context["_last_work_fact_read"]
                action_result = {
                    "work_facts": observed,
                    "work_ids": [row["id"] for row in observed["facts"]],
                    "states": [row["state"] for row in observed["facts"]],
                }
                identifiers = action_result["work_ids"]
                work_id = identifiers[0] if len(identifiers) == 1 else None
            original_snapshot = deepcopy(context["_snapshot"])
            try:
                # Savepoint lets a rejected candidate produce an honest response
                # without committing any partial business mutation.
                with tx.conn.transaction():
                    if decision.action is not None:
                        action_payload = decision.action
                        silent_declaration = decision.directness in {
                            "other",
                            "none",
                        } and is_task_declaration(
                            action_payload, actor.role, actor.member_id, context.get("_tasks", [])
                        )
                        action_result, text = self._apply_action(tx, event, decision, context)
                        if action_payload.get("op") == "quiet" and action_payload.get("enabled"):
                            decision = Decision(
                                kind="observe", reason_code="quiet_enabled", confidence=1
                            )
                        elif silent_declaration:
                            decision = Decision(
                                kind="observe",
                                reason_code="task_declaration_recorded",
                                directness=decision.directness,
                                confidence=decision.confidence,
                            )
                        else:
                            decision = decision.model_copy(update={"response": text})
                        if action_payload.get("op") == "forget":
                            context["_snapshot"]["source_fingerprints"] = {
                                event.id: tx.event_fingerprint(event.id)
                            }
                            context["_snapshot"]["source_task_revisions"] = {}
                            context["_work_fact_source_ids"] = []
                        else:
                            task_map = context["_snapshot"].get("source_task_revisions", {})
                            task_ids = set(task_map)
                            if action_result.get("task_id"):
                                task_ids.add(action_result["task_id"])
                            for part in action_result.get("actions", []):
                                if part.get("task_id"):
                                    task_ids.add(part["task_id"])
                            context["_snapshot"]["source_task_revisions"] = {
                                identifier: tx.get_team_task(identifier).revision
                                for identifier in task_ids
                            }
                    elif decision.kind == "start_work":
                        planner_outputs: list[JsonValue] = [value for value in decision.outputs]
                        message_only = bool(
                            event.kind == EventKind.TEXT
                            and not event.reply_to
                            and not event.attachments
                            and not decision.input_versions
                            and not decision.input_event_ids
                            and message_only_research_request(event.text, identity=self.identity)
                        )
                        outputs = [
                            OutputRequirement.model_validate(value)
                            for value in (
                                [{"format": "text"}] if message_only else decision.outputs
                            )
                        ]
                        formats = {item.format for item in outputs}
                        if (
                            not self.config.features.document_editing
                            and formats & {"md", "docx", "pdf", "pptx"}
                        ) or (
                            not self.config.features.speech_reply_on_request
                            and formats & {"wav", "ogg"}
                        ):
                            raise DomainError(
                                "feature_disabled", "Requested output capability is disabled"
                            )
                        snapshot = context["_snapshot"]
                        decision, attachment_ids = self._canonical_inputs(
                            tx, event, decision, context
                        )
                        topic_id = decision.topic_id or context["_topic_id"]
                        if topic_id is not None and topic_id not in {
                            topic["id"] for topic in context["topics"]
                        }:
                            raise DomainError(
                                "permission_denied", "Topic was not in the public request context"
                            )
                        if topic_id is None:
                            topic = self.memory.in_tx(tx).create_topic(
                                actor, event.text.strip()[:100] or "附件讨论"
                            )
                            topic_id = topic.id
                            tx.conn.execute(
                                "INSERT INTO ga_topic_origins(group_id,topic_id,source_event_ids) VALUES(%s,%s,%s)",
                                (actor.group_id, topic_id, [event.id]),
                            )
                        self.memory.in_tx(tx).link_event(
                            actor, event.id, topic_id, evidence_event_ids=[event.id]
                        )
                        request_text = str(context["model_event"].get("text", event.text))
                        speech_requested = requests_speech_reply(request_text)
                        if speech_requested and not self.config.features.speech_reply_on_request:
                            raise DomainError("feature_disabled", "Speech output is disabled")
                        delivery_artifacts = not bool(
                            re.search(
                                r"(?:不要|不用|先别|暂时别|暂不|别).{0,6}(?:发送|发到群|发群|发出来|发给)",
                                request_text,
                            )
                        )
                        quantity = normalize_image_quantity(
                            tx,
                            event,
                            requester_id=actor.member_id,
                            authorized=snapshot["source_fingerprints"],
                            count=decision.public_image_count,
                            evidence=decision.public_image_count_evidence,
                            image_work="public_images" in decision.work_capabilities
                            or decision.public_image_count is not None,
                            identity=self.identity,
                        )
                        public_images = quantity.count
                        # The semantic plan describes required execution evidence.
                        # Mentioning search in a negative clause or quoted example
                        # cannot manufacture an extra research obligation.
                        research_required = public_images is not None or bool(
                            set(decision.work_capabilities) & {"public_research", "public_images"}
                        )
                        if public_images and not self.config.features.public_image_retrieval:
                            raise DomainError(
                                "feature_disabled", "Public image retrieval is disabled"
                            )
                        direct_fetch = bool(
                            research_required
                            and re.match(
                                r"(?:真正|实际|现在|直接)?(?:读取|打开|查看|访问).{0,20}https?://",
                                main_request(request_text, identity=self.identity),
                            )
                        )
                        if research_required and not self.config.features.web_research:
                            raise DomainError("feature_disabled", "Web research is disabled")
                        required_tools = []
                        if "public_images" in decision.work_capabilities or public_images:
                            required_tools.append("web_image_import")
                        main = main_request(request_text, identity=self.identity)
                        if re.match(
                            r"(?:真正|实际|现在|直接)?(?:用|使用)(?:受控的?)?\s*[Pp]ython", main
                        ):
                            required_tools.append("python_compute")
                        elif re.match(
                            r"(?:真正|实际|现在|直接)?(?:用|使用)(?:受控的?)?\s*(?:[Ss]hell|进程)",
                            main,
                        ):
                            required_tools.append("process_run")
                        admitted_intent = (
                            new_work_intent(event.text, identity=self.identity)
                            if fast_admitted
                            else None
                        )
                        if (
                            admitted_intent
                            and admitted_intent.required_tool
                            and admitted_intent.required_tool not in required_tools
                        ):
                            required_tools.append(admitted_intent.required_tool)
                        if speech_requested:
                            required_tools.append("speech_synthesize")
                        if direct_fetch:
                            required_tools.append("web_fetch")
                        if research_required and re.match(
                            r"(?:联网)?(?:检索|搜索|上网.{0,3}查)", main
                        ):
                            required_tools.append("web_search")
                        team_write = task_write_request(request_text, identity=self.identity)
                        team_targets = [
                            row["id"]
                            for row in context["_tasks"]
                            if actor.role == Role.OWNER or row["owner_id"] == actor.member_id
                        ]
                        completion_allowed = bool(
                            re.search(
                                r"(?:标记|标成|记为|更新为|确认).{0,12}(?:完成|做完)|(?:我|我们).{0,10}(?:已经|已).{0,12}(?:完成|做完)",
                                request_text,
                            )
                        )
                        retained = sorted(
                            {
                                ref["event_id"]
                                for item in snapshot["items"]
                                if item["kind"] in {"fact", "decision", "team_task"}
                                for ref in item["source_refs"]
                                if ref["state"] == "expired"
                            }
                            | set(snapshot.get("artifact_retained_source_ids", []))
                            | set(snapshot.get("retained_context_source_ids", []))
                        )
                        work = tx.create_work(
                            event.text if message_only else decision.goal or "",
                            event.id,
                            acceptance_criteria=[
                                "完成原始请求中的全部说明、事实及来源要求，以有证据的文字消息交付，不增加原文未要求的附件。"
                            ]
                            if message_only
                            else decision.acceptance_criteria,
                            input_versions=decision.input_versions,
                            scope={
                                "allowed_group_ids": [actor.group_id],
                                "delivery_target": "origin_group",
                                "external_account_changes": False,
                                "source_fingerprints": snapshot["source_fingerprints"],
                                "source_task_revisions": snapshot.get("source_task_revisions", {}),
                                "context_artifact_versions": snapshot.get(
                                    "context_artifact_versions", []
                                ),
                                "source_event_ids": list(snapshot["source_fingerprints"]),
                                "attachment_event_ids": [str(value) for value in attachment_ids],
                                "retained_fact_source_ids": retained,
                                "outputs": [item.model_dump(mode="json") for item in outputs],
                                "research_message_only": message_only,
                                "planner_outputs": planner_outputs,
                                "admission_path": "deterministic_current_request"
                                if fast_admitted
                                else "provider_candidate",
                                "topic_id": topic_id,
                                "speech_requested": speech_requested,
                                "delivery_artifacts": delivery_artifacts,
                                "research_required": research_required,
                                **quantity.fields(),
                                "planner_public_image_count": decision.public_image_count,
                                "work_capabilities": list(decision.work_capabilities),
                                "required_tools": [str(name) for name in required_tools],
                                "team_task_write_allowed": team_write,
                                "team_task_authorization_event_id": event.id,
                                "team_task_target_ids": [
                                    str(identifier) for identifier in team_targets
                                ]
                                if team_write
                                else [],
                                "team_task_completion_ids": [
                                    str(identifier) for identifier in team_targets
                                ]
                                if team_write and completion_allowed
                                else [],
                                "schedule_creation_allowed": not bool(
                                    re.search(
                                        r"(?:不(?:要|用|必)?|别|无需).{0,3}(?:创建|设置|安排)?.{0,2}(?:提醒|定时|跟进)",
                                        request_text,
                                    )
                                ),
                                "derived_depth": 0,
                            },
                            budget=self.config.execution.model_dump(mode="json"),
                            required_checks=["result"]
                            + [
                                "artifact:" + item.format
                                for item in outputs
                                if item.format != "text"
                            ],
                        )
                        work_id = work.id
                        decision = decision.model_copy(
                            update={
                                "response": _work_admission_text(
                                    required_tools,
                                    [item.format for item in outputs],
                                    research_required=research_required,
                                    image_inputs=any(
                                        tx.get_event(identifier).kind == EventKind.IMAGE
                                        for identifier in attachment_ids
                                    ),
                                )
                            }
                        )
                    elif decision.kind == "update_work":
                        decision, attachment_ids = self._canonical_inputs(
                            tx, event, decision, context
                        )
                        changed_outputs = [
                            OutputRequirement.model_validate(value).model_dump(mode="json")
                            for value in decision.outputs
                        ]
                        previous_work = tx.get_work(decision.target_work_id or "")
                        quantity = normalize_image_quantity(
                            tx,
                            event,
                            requester_id=actor.member_id,
                            authorized=context["_snapshot"]["source_fingerprints"],
                            count=decision.public_image_count,
                            evidence=decision.public_image_count_evidence,
                            previous=previous_work,
                            image_work=is_public_image_work(previous_work)
                            or "public_images" in decision.work_capabilities,
                            identity=self.identity,
                        )
                        quantity_sources = dict(context["_snapshot"]["source_fingerprints"])
                        if quantity.evidence is not None:
                            quantity_sources[quantity.evidence.event_id] = tx.event_fingerprint(
                                quantity.evidence.event_id
                            )
                        work = tx.revise_work(
                            decision.target_work_id or "",
                            decision.goal or "",
                            expected_revision=decision.expected_revision or 0,
                            acceptance_criteria=decision.acceptance_criteria or None,
                            input_versions=decision.input_versions or None,
                            source_fingerprints=quantity_sources,
                            source_task_revisions=context["_snapshot"].get(
                                "source_task_revisions", {}
                            ),
                            context_artifact_versions=context["_snapshot"].get(
                                "context_artifact_versions", []
                            ),
                            retained_fact_source_ids=context["_snapshot"].get(
                                "artifact_retained_source_ids"
                            )
                            or None,
                            output_requirements=changed_outputs or None,
                            required_checks=(
                                ["result"]
                                + [
                                    "artifact:" + str(item["format"])
                                    for item in changed_outputs
                                    if item["format"] != "text"
                                ]
                            )
                            if changed_outputs
                            else None,
                        )
                        work_id = work.id
                        if (
                            decision.work_capabilities
                            or is_public_image_work(previous_work)
                            or decision.public_image_count is not None
                            or decision.public_image_count_evidence is not None
                        ):
                            scope = dict(work.scope)
                            if decision.work_capabilities:
                                scope["work_capabilities"] = list(decision.work_capabilities)
                            scope.update(quantity.fields())
                            if decision.public_image_count is not None:
                                scope["planner_public_image_count"] = decision.public_image_count
                            if set(decision.work_capabilities) & {
                                "public_research",
                                "public_images",
                            }:
                                scope["research_required"] = True
                            tx.conn.execute(
                                "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
                                (Jsonb(scope), tx.group_id, work.id),
                            )
                        if attachment_ids:
                            scope = dict(work.scope)
                            scope["attachment_event_ids"] = list(
                                dict.fromkeys(
                                    _strings(scope.get("attachment_event_ids")) + attachment_ids
                                )
                            )
                            tx.conn.execute(
                                "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
                                (Jsonb(scope), tx.group_id, work.id),
                            )
                        if work.state == WorkState.WAITING_INPUT:
                            work = tx.transition_work(
                                work.id,
                                WorkState.QUEUED,
                                expected_revision=work.revision,
                                reason="input_received",
                            )
                        decision = decision.model_copy(
                            update={"response": "已更新同一项工作的要求，旧结果不会覆盖新目标。"}
                        )
                    elif decision.kind == "query_work":
                        # Compatibility for persisted/scripted Decision callers.
                        # The live agent reads via read_work_facts and supplies
                        # its own final text, never this deterministic renderer.
                        observed = query_work_in_tx(
                            tx, event, decision, context, WorkFacts(self.store)
                        )
                        current_facts = observed["facts"]
                        context["_work_fact_source_ids"] = sorted(
                            set(context.get("_work_fact_source_ids", []))
                            | WorkFacts.source_dependencies_in_tx(
                                tx, tuple(row["id"] for row in current_facts)
                            )
                        )
                        for fact in current_facts:
                            source_id = fact["request_event_id"]
                            context["_snapshot"]["source_fingerprints"][source_id] = (
                                tx.event_fingerprint(source_id)
                            )
                        identifiers = [fact["id"] for fact in current_facts]
                        action_result = {
                            "work_ids": identifiers,
                            "states": [fact["state"] for fact in current_facts],
                            "work_facts": observed,
                        }
                        work_id = identifiers[0] if len(identifiers) == 1 else None
                        decision = Decision(
                            kind="reply",
                            reason_code="work_status_query",
                            response=render_work_query(observed),
                            directness="direct",
                            confidence=1,
                        )
                    elif decision.kind == "control_work":
                        resolved = self._resolve_work_set(actor, event, decision, context)
                        selected = []
                        for ref in resolved.refs:
                            row = tx._work_row(ref.id)
                            if row["revision"] != ref.revision or row["state"] != ref.state.value:
                                raise DomainError(
                                    "version_conflict", "A selected work changed before the command"
                                )
                            work = tx.get_work(ref.id)
                            tx._check_work_source(row)
                            source = tx.get_event(work.request_event_id)
                            context["_snapshot"]["source_fingerprints"][source.id] = (
                                tx.event_fingerprint(source.id)
                            )
                            selected.append(work)
                        target = {
                            "cancel": WorkState.CANCELLED,
                            "pause": WorkState.PAUSED,
                            "resume": WorkState.QUEUED,
                            "retry": WorkState.QUEUED,
                        }[decision.control or "pause"]
                        for work in selected:
                            if (
                                decision.control in {"resume", "retry"}
                                and work.policy_revision != actor.policy_revision
                            ):
                                # The user's current scoped command reauthorizes the same
                                # immutable goal under the current policy; it resets nothing.
                                tx.conn.execute(
                                    "UPDATE ga_work_orders SET policy_revision=%s WHERE group_id=%s AND id=%s AND revision=%s",
                                    (
                                        actor.policy_revision,
                                        actor.group_id,
                                        work.id,
                                        work.revision,
                                    ),
                                )
                            tx.transition_work(
                                work.id,
                                target,
                                expected_revision=work.revision,
                                reason="retry" if decision.control == "retry" else "user_request",
                            )
                            controlled_work_ids.append(work.id)
                        action_result = {
                            "work_ids": controlled_work_ids,
                            "control": decision.control,
                            "selected_states": [item.state.value for item in selected],
                        }
                        work_id = selected[0].id if len(selected) == 1 else None
                        verb = {
                            "cancel": "取消",
                            "pause": "暂停",
                            "resume": "恢复排队",
                            "retry": "安排重试",
                        }[decision.control or "pause"]
                        text = (
                            f"已{verb}这项工作。"
                            if len(selected) == 1
                            else f"已{verb}{len(selected)}项匹配的工作。"
                        )
                        decision = decision.model_copy(update={"response": text})
            except (DomainError, ValidationError) as error:
                context["_snapshot"] = original_snapshot
                code = error.code if isinstance(error, DomainError) else "invalid_input"
                decision = Decision(
                    kind="clarify"
                    if code in {"ambiguous_reference", "not_found", "invalid_input"}
                    else "reply",
                    reason_code=code,
                    response=_ERRORS.get(code, "这项操作暂时无法完成，现有记录已保留。"),
                    directness="direct",
                    confidence=1,
                )
                work_id = None
                action_result = None
                controlled_work_ids = []
            if decision.response and decision.kind not in {"ignore", "observe"}:
                admission = ingress_admission(
                    tx,
                    event,
                    self.identity,
                    time_zone=self.config.group.time_zone,
                    quiet_hours=self.config.group.quiet_hours,
                    now=self.clock(),
                    default_mode=self.config.group.participation_mode,
                )
                if not admission.allowed:
                    decision = (
                        decision.model_copy(update={"response": None})
                        if decision.kind not in {"reply", "clarify"}
                        else Decision(kind="observe", reason_code=admission.reason_code)
                    )
            if (
                tool_call_id is None
                and decision.response
                and decision.kind not in {"ignore", "observe"}
            ):
                delivery = tx.enqueue_reply(
                    event.id,
                    decision.response,
                    read_only_authorized=actor.role.value in self.config.group.read_only_qa_roles,
                )
                delivery_id = delivery.id
                tx.conn.execute(
                    "INSERT INTO ga_participation_log(group_id,event_id,proactive,reason,created_at,invocation_root_event_id) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        actor.group_id,
                        event.id,
                        decision.directness == "proactive",
                        decision.reason_code,
                        self.clock(),
                        admission.invocation_root_event_id,
                    ),
                )
            if (
                event.kind in {EventKind.IMAGE, EventKind.AUDIO, EventKind.FILE}
                and decision.kind == "observe"
                and decision.directness != "other"
            ):
                # A silent attachment inside an existing invoked conversation
                # carries that same session; it neither wakes a new one nor
                # changes the user's quiet policy or starts execution.
                media_admission = ingress_admission(
                    tx,
                    event,
                    self.identity,
                    time_zone=self.config.group.time_zone,
                    quiet_hours=self.config.group.quiet_hours,
                    now=self.clock(),
                    default_mode=self.config.group.participation_mode,
                )
                if media_admission.allowed and media_admission.invocation_root_event_id:
                    tx.conn.execute(
                        "INSERT INTO ga_participation_log(group_id,event_id,proactive,reason,created_at,invocation_root_event_id) "
                        "VALUES(%s,%s,false,'invocation_attachment',%s,%s) ON CONFLICT DO NOTHING",
                        (
                            actor.group_id,
                            event.id,
                            self.clock(),
                            media_admission.invocation_root_event_id,
                        ),
                    )
            result = CoordinationResult(
                event_id=event.id,
                decision=decision,
                work_id=work_id,
                delivery_id=delivery_id,
                action_result=action_result,
                controlled_work_ids=controlled_work_ids,
            )
            privacy_action = decision.action is not None and decision.action.get("op") == "forget"
            if privacy_action:
                # Forget can redact this open turn. Never rehydrate a stale
                # receipt/transcript after the privacy transaction has done so.
                canonical: dict[str, Any] | None = tx.get_decision(event.id)
                previous = {**canonical, "action_receipts": []} if canonical is not None else None
                context["_agent_context_revoked"] = True
            lineage = previous if previous and not privacy_action else {}
            body: dict[str, Any] = {
                "kind": decision.kind,
                "result": result.model_dump(mode="json"),
                # Revocation lineage is metadata, not an attachment/tool grant.
                "source_event_ids": sorted(
                    set(context["_snapshot"]["source_fingerprints"])
                    | set(context.get("_work_fact_source_ids", []))
                    | set(lineage.get("source_event_ids", []))
                ),
                "source_fingerprints": {
                    **lineage.get("source_fingerprints", {}),
                    **context["_snapshot"]["source_fingerprints"],
                },
                "source_task_revisions": {
                    **lineage.get("source_task_revisions", {}),
                    **context["_snapshot"].get("source_task_revisions", {}),
                },
            }
            if tool_call_id is not None:
                # Refresh witnesses changed by our own accepted mutation. A later
                # tool still revalidates all sources against concurrent changes.
                snapshot = context["_snapshot"]
                from groupagent.memory.common import memory_lock

                snapshot["privacy_epoch"] = memory_lock(tx)
                for index, witness in enumerate(snapshot.get("task_queries", [])):
                    _, refreshed = self.memory.task_query_in_tx(
                        tx,
                        witness["query"],
                        topic_id=witness["topic_id"],
                        limit=witness["limit"],
                        public_only=witness["public_only"],
                        through_sequence=witness.get("through_sequence"),
                    )
                    snapshot["task_queries"][index] = refreshed
                body["agent_snapshot"] = {
                    key: value for key, value in snapshot.items() if key != "items"
                }
                body["action_receipts"] = [
                    *(previous.get("action_receipts", []) if previous else []),
                    {
                        "tool_call_id": tool_call_id,
                        "candidate_sha256": candidate_digest,
                        "result": result.model_dump(mode="json"),
                    },
                ]
                body["agent_pending_response"] = decision.reason_code != "quiet_enabled"
                body["agent_context_revoked"] = bool(context.get("_agent_context_revoked"))
                if previous:
                    tx.conn.execute(
                        "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
                        (Jsonb(body), actor.group_id, event.id),
                    )
                else:
                    tx.record_decision(event.id, body)
            else:
                tx.record_decision(event.id, body)
            return result
