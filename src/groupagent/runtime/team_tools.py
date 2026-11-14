"""Human TeamTask tools with canonical authorization and bounded source scope.

Read data does not grant write authority. Only control-plane Work.scope flags do.
Internal action events contain no fabricated human quotation or platform ID.

Control-plane scope contract:
- team_task_write_allowed must be exactly True; derived work gets read tools only.
- team_task_authorization_event_id defaults to request_event_id. After clarification,
  bind the requester's new canonical source and include its fingerprint.
- team_task_target_ids authorizes existing task edits/claims; tasks created by this
  same goal may also be adjusted. Merely reading a task never adds authorization.
- team_task_completion_ids requires explicit requester/Owner recognition before a
  transition to completed. An AI artifact or model success assertion is insufficient.
- team_task_effects records minimal actual receipts for the parent validator.

Private sources are omitted from public task reads. Consumed task revisions and
source fingerprints are retained for forget/recall. A trusted precondition can
request input with success=False and NO_EFFECT; it never pretends a write occurred.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal

from psycopg.types.json import Jsonb
from pydantic import AwareDatetime, Field, JsonValue

from groupagent.domain import (
    ActorContext,
    DomainError,
    EventKind,
    ExecutionLease,
    GroupEvent,
    Role,
    TeamTask,
    TeamTaskState,
    WorkOrder,
)
from groupagent.memory import MemberService, MemoryService
from groupagent.memory.common import source_rows
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure, ToolInputRequired

_HUMAN_ORIGINS = {
    "verified_channel_metadata",
    "agentwechat_authenticated_message_metadata",
    "authenticated_audio_transcription",
}


class TeamMembersInput(ToolData):
    query: str = Field(default="", max_length=100)
    limit: int = Field(default=50, ge=1, le=200, strict=True)


class TeamTasksInput(ToolData):
    query: str = Field(default="", max_length=200)
    owner_id: str | None = Field(default=None, min_length=1, max_length=200)
    status: Literal["open", "in_progress", "completed", "cancelled"] | None = None
    limit: int = Field(default=20, ge=1, le=50, strict=True)


class TeamTaskCreateInput(ToolData):
    title: str = Field(min_length=1, max_length=300)
    owner_id: str | None = Field(default=None, min_length=1, max_length=200)
    owner_name: str | None = Field(default=None, min_length=1, max_length=200)
    due_at: AwareDatetime | None = None


class TeamTaskClaimInput(ToolData):
    task_id: str = Field(min_length=1, max_length=200)
    expected_revision: int = Field(ge=1, strict=True)


class TeamTaskUpdateInput(TeamTaskClaimInput):
    title: str | None = Field(default=None, min_length=1, max_length=300)
    progress: str | None = Field(default=None, max_length=3000)
    status: Literal["open", "in_progress", "completed", "cancelled"] | None = None
    owner_id: str | None = Field(default=None, min_length=1, max_length=200)
    owner_name: str | None = Field(default=None, min_length=1, max_length=200)
    due_at: AwareDatetime | None = None


class TeamListOutput(ToolData):
    items: tuple[dict[str, JsonValue], ...]
    incomplete: bool = False
    untrusted_content: Literal[True] = True


class TeamTaskReceipt(ToolData):
    task_id: str
    revision: int
    owner_id: str | None
    status: str
    assignment_kind: str
    operation: str
    changed: bool
    source_event_id: str
    goal_revision: int
    replayed: bool = False


def _ids(scope: dict[str, JsonValue], name: str, *, maximum: int = 100) -> set[str]:
    value = scope.get(name, [])
    if (
        not isinstance(value, list)
        or len(value) > maximum
        or any(not isinstance(item, str) for item in value)
    ):
        raise DomainError("scope_violation", "Trusted task grant must contain bounded task IDs")
    return {str(item) for item in value}


def _public_sources(
    tx: UnitOfWork, identifiers: list[str], *, allow_expired: bool = False
) -> set[str]:
    """Include known quote ancestry so an internal event cannot hide a private origin."""
    known: set[str] = set()
    pending = list(identifiers)
    while pending:
        identifier = pending.pop()
        if identifier in known:
            continue
        if len(known) >= 500:
            raise DomainError("quota_exceeded", "Task evidence exceeds its scope bound")
        source_rows(
            tx, [identifier], shared=True, allow_expired=allow_expired, require_complete=True
        )
        value = tx.get_event(identifier)
        known.add(identifier)
        if value.kind == EventKind.QUOTE and value.reply_to:
            exists = tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND id=%s", (tx.group_id, value.reply_to)
            ).fetchone()
            if exists:
                pending.append(value.reply_to)
            elif value.trusted_actor_origin == "internal_work_action":
                raise DomainError(
                    "source_unavailable", "Internal task authority source disappeared"
                )
    return known


def register_team_tools(
    gateway: ToolGateway,
    store: PostgresStore,
    members: MemberService,
    memory: MemoryService,
    work: WorkOrder,
    lease: ExecutionLease,
    actor: ActorContext,
) -> None:
    if (work.group_id, work.id, work.requester_id, work.goal_revision) != (
        actor.group_id,
        lease.work_id,
        actor.member_id,
        lease.goal_revision,
    ) or lease.group_id != actor.group_id:
        raise DomainError("scope_violation", "Team tools require the exact requester work lease")

    def guard(
        tx: UnitOfWork, context: ToolContext, *, write: bool
    ) -> tuple[WorkOrder, GroupEvent | None]:
        if (
            context.actor != actor
            or context.group_id != work.group_id
            or context.work_id != work.id
        ):
            raise DomainError("scope_violation", "Team context cannot replace trusted authority")
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Team operation was cancelled")
        memory.prepare_in_tx(tx)
        tx.assert_lease(lease)
        current = tx.get_work(work.id)
        tx.assert_task_sources(
            current.scope.get("source_task_revisions", {}), require_current=False
        )
        known = current.scope.get("source_fingerprints", {})
        retained = _ids(current.scope, "retained_fact_source_ids", maximum=500)
        if not isinstance(known, dict) or len(known) > 500:
            raise DomainError("scope_violation", "Work evidence is not a bounded fingerprint map")
        for identifier, digest in known.items():
            _public_sources(tx, [identifier], allow_expired=identifier in retained)
            if tx.event_fingerprint(identifier) != digest:
                raise DomainError("source_unavailable", "Work evidence changed before task access")
        _public_sources(tx, [current.request_event_id])
        if not write:
            return current, None
        tx._writer()
        if (
            current.scope.get("team_task_write_allowed") is not True
            or type(current.scope.get("derived_depth", 0)) is not int
            or current.scope.get("derived_depth", 0) != 0
            or current.scope.get("origin_schedule_id")
            or current.scope.get("origin_trigger_rule_id")
        ):
            raise DomainError(
                "permission_denied", "This accepted goal does not authorize human task writes"
            )
        authorization = current.scope.get(
            "team_task_authorization_event_id", current.request_event_id
        )
        if not isinstance(authorization, str) or (
            authorization != current.request_event_id and authorization not in known
        ):
            raise DomainError("scope_violation", "Task authority must be a bound canonical source")
        _public_sources(tx, [authorization])
        source = tx.get_event(authorization)
        if (
            source.actor_id != actor.member_id
            or source.kind not in {EventKind.TEXT, EventKind.QUOTE}
            or source.trusted_actor_origin not in _HUMAN_ORIGINS
            or (source.platform_message_id and tx.is_bot_message(source.platform_message_id))
        ):
            raise DomainError(
                "permission_denied", "File/OCR/other-member/bot data cannot authorize task writes"
            )
        if any(
            item.get("kind") == "audio_transcription"
            and (item.get("needs_clarification") or item.get("uncertainty"))
            for item in source.attachments
        ):
            raise ToolInputRequired(
                "语音中的姓名、时间或完成状态还不确定。请用文字补充关键内容后再登记真人任务。",
                required_inputs=("准确的负责人及任务要求",),
            )
        return current, source

    def assignee(tx: UnitOfWork, owner_id: str | None, owner_name: str | None) -> str:
        service = members.in_tx(tx)
        try:
            if owner_name is not None:
                selected = service.resolve_unique(actor, owner_name)
                if owner_id is not None and owner_id != selected.member_id:
                    raise DomainError("ambiguous_reference", "Name and member ID differ")
            else:
                selected = service.get(actor, owner_id or actor.member_id)
            if selected.membership_state == "left":
                raise DomainError("not_found", "The assignee left this group")
        except DomainError as error:
            if error.code in {"ambiguous_reference", "not_found"}:
                raise ToolInputRequired(
                    "请用当前群昵称或唯一成员标识明确负责人；目前无法唯一对应到本群成员。",
                    required_inputs=("负责人群昵称或唯一标识",),
                ) from None
            raise
        if selected.member_id != actor.member_id and actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only Owner may assign another member")
        return selected.member_id

    def task_sources(tx: UnitOfWork, task: TeamTask) -> set[str]:
        if task.content_state != "active":
            raise DomainError("source_unavailable", "Forgotten tasks are unavailable")
        return _public_sources(tx, list(task.source_event_ids), allow_expired=True)

    def authorize_target(tx: UnitOfWork, current: WorkOrder, task: TeamTask) -> None:
        task_sources(tx, task)
        permitted = task.id in _ids(current.scope, "team_task_target_ids")
        if not permitted and task.source_event_id:
            source = tx.get_event(task.source_event_id)
            permitted = source.trusted_actor_origin == "internal_work_action" and any(
                item.get("kind") == "work_action"
                and item.get("internal") is True
                and item.get("work_id") == current.id
                and item.get("goal_revision") == current.goal_revision
                and item.get("operation") == "team_task_create"
                for item in source.attachments
            )
        if not permitted:
            raise ToolInputRequired(
                "请明确本次要认领或修改的真人任务，再继续登记。",
                required_inputs=("真人任务名称或编号",),
            )

    def consume(
        tx: UnitOfWork,
        current: WorkOrder,
        tasks: list[TeamTask],
        extra_sources: tuple[str, ...] = (),
        effect: TeamTaskReceipt | None = None,
    ) -> None:
        scope: dict[str, Any] = dict(current.scope)
        fingerprints = dict(scope.get("source_fingerprints", {}))
        revisions = dict(scope.get("source_task_revisions", {}))
        retained = set(scope.get("retained_fact_source_ids", []))
        identifiers = set(extra_sources)
        for task in tasks:
            identifiers.update(task_sources(tx, task))
            revisions[task.id] = task.revision
        for identifier in identifiers:
            fingerprints[identifier] = tx.event_fingerprint(identifier)
            if tx.get_event(identifier).content_state == "expired":
                retained.add(identifier)
        if len(fingerprints) > 500 or len(revisions) > 100:
            raise DomainError("quota_exceeded", "Consumed task scope exceeds its bound")
        scope.update(
            source_fingerprints=fingerprints,
            source_event_ids=sorted(fingerprints),
            retained_fact_source_ids=sorted(retained),
            source_task_revisions=revisions,
        )
        if effect:
            effects = list(scope.get("team_task_effects", []))
            if len(effects) >= 100:
                raise DomainError("quota_exceeded", "Too many task effects in one work")
            effects.append(effect.model_dump(mode="json"))
            scope["team_task_effects"] = effects
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
            (Jsonb(scope), actor.group_id, current.id),
        )

    def write(
        name: str,
        arguments: ToolData,
        context: ToolContext,
        action: Callable[[UnitOfWork, WorkOrder, str], tuple[TeamTask, bool]],
    ) -> TeamTaskReceipt:
        fingerprint = hashlib.sha256(
            json.dumps(
                [name, lease.goal_revision, arguments.model_dump(mode="json")],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with store.transaction(actor) as tx:
            current, authorization = guard(tx, context, write=True)
            assert authorization is not None
            old = tx.conn.execute(
                "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
                "AND action='tool_team_receipt' AND details->>'idempotency_key'=%s ORDER BY id DESC LIMIT 1",
                (actor.group_id, current.id, context.idempotency_key),
            ).fetchone()
            if old:
                if old["details"]["fingerprint"] != fingerprint:
                    raise DomainError(
                        "idempotency_conflict", "Task key was reused with different arguments"
                    )
                result = TeamTaskReceipt.model_validate(old["details"]["receipt"])
                task_sources(tx, tx.get_team_task(result.task_id))
                return result.model_copy(update={"replayed": True})
            key = (
                "work-action:"
                + hashlib.sha256(
                    json.dumps([actor.group_id, current.id, context.idempotency_key]).encode()
                ).hexdigest()
            )
            now = datetime.now(UTC)
            source = GroupEvent(
                id=key,
                source_key=key,
                group_id=actor.group_id,
                actor_id=actor.member_id,
                kind=EventKind.QUOTE,
                text="",
                occurred_at=now,
                received_at=now,
                reply_to=authorization.id,
                trusted_actor_origin="internal_work_action",
                policy_revision=actor.policy_revision,
                attachments=[
                    {
                        "kind": "work_action",
                        "internal": True,
                        "work_id": current.id,
                        "goal_revision": current.goal_revision,
                        "operation": name,
                        "authorization_event_id": authorization.id,
                    }
                ],
            )
            ingested = tx.ingest_event(source, key, "1", 0).event
            task, changed = action(tx, current, ingested.id)
            result = TeamTaskReceipt(
                task_id=task.id,
                revision=task.revision,
                owner_id=task.owner_id,
                status=task.status,
                assignment_kind=task.assignment_kind,
                operation=name,
                changed=changed,
                source_event_id=ingested.id,
                goal_revision=current.goal_revision,
            )
            consume(tx, current, [task], (authorization.id, ingested.id), effect=result)
            input_fingerprints = current.scope.get("source_fingerprints", {})
            input_tasks = current.scope.get("source_task_revisions", {})
            assert isinstance(input_fingerprints, dict) and isinstance(input_tasks, dict)
            input_sources: list[JsonValue] = [
                identifier for identifier in sorted({authorization.id, *input_fingerprints})
            ]
            tx.record_decision(
                ingested.id,
                {
                    "kind": "observe",
                    # Data provenance is separate from authorization. A file or
                    # prior task actually consumed by this Work may influence
                    # generated duties, but never supplies the write grant.
                    "source_event_ids": input_sources,
                    "source_fingerprints": {
                        **input_fingerprints,
                        authorization.id: tx.event_fingerprint(authorization.id),
                    },
                    "source_task_revisions": {
                        **input_tasks,
                        task.id: task.revision,
                    },
                    "result": {
                        "event_id": ingested.id,
                        "decision": {"kind": "observe", "reason_code": "internal_work_action"},
                        "work_id": None,
                        "delivery_id": None,
                        "duplicate": False,
                    },
                },
            )
            tx.assert_lease(lease)
            if context.cancelled.is_set():
                raise DomainError("cancelled", "Task action was cancelled before commit")
            tx._audit(
                "tool_team_receipt",
                current.id,
                idempotency_key=context.idempotency_key,
                fingerprint=fingerprint,
                receipt=result.model_dump(mode="json"),
            )
            return result

    async def mutate(
        name: str, arguments: ToolData, context: ToolContext, action: Callable
    ) -> TeamTaskReceipt:
        try:
            return await asyncio.to_thread(write, name, arguments, context, action)
        except ToolInputRequired:
            raise  # The DB context has rolled back all business effects.
        except DomainError as error:
            raise ToolFailure(error.code, error.message, no_effect=True) from None

    async def create(arguments: TeamTaskCreateInput, context: ToolContext) -> TeamTaskReceipt:
        def action(tx, current, source_id):
            owner = assignee(tx, arguments.owner_id, arguments.owner_name)
            return tx.create_team_task(
                arguments.title, owner_id=owner, due_at=arguments.due_at, source_event_id=source_id
            ), True

        return await mutate("team_task_create", arguments, context, action)

    async def claim(arguments: TeamTaskClaimInput, context: ToolContext) -> TeamTaskReceipt:
        def action(tx, current, source_id):
            original = tx.get_team_task(arguments.task_id)
            if original.owner_id not in {None, actor.member_id}:
                raise DomainError(
                    "permission_denied", "Only an unassigned task may be newly claimed"
                )
            authorize_target(tx, current, original)
            result = tx.claim_team_task(
                original.id,
                expected_revision=arguments.expected_revision,
                source_event_id=source_id,
            )
            return result, result.revision != original.revision

        return await mutate("team_task_claim", arguments, context, action)

    async def update(arguments: TeamTaskUpdateInput, context: ToolContext) -> TeamTaskReceipt:
        def action(tx, current, source_id):
            original = tx.get_team_task(arguments.task_id)
            if original.owner_id != actor.member_id and actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Only the task owner or Owner may update it")
            authorize_target(tx, current, original)
            if original.revision != arguments.expected_revision:
                raise DomainError("version_conflict", "Task revision changed")
            if (
                arguments.status == "completed"
                and original.status != TeamTaskState.COMPLETED
                and original.id not in _ids(current.scope, "team_task_completion_ids")
            ):
                raise ToolInputRequired(
                    "请确认这项真人任务确已完成并指出对应任务；AI产物完成不等于成员已完成。",
                    required_inputs=("真人任务的明确完成认可",),
                )
            options: dict[str, Any] = {}
            for field in ("title", "progress", "status", "due_at"):
                value = getattr(arguments, field)
                if (
                    field in arguments.model_fields_set
                    and (value is not None or field == "due_at")
                    and value != getattr(original, field)
                ):
                    options[field] = TeamTaskState(value) if field == "status" else value
            if arguments.owner_id is not None or arguments.owner_name is not None:
                owner = assignee(tx, arguments.owner_id, arguments.owner_name)
                if owner != original.owner_id:
                    options["owner_id"] = owner
            if not options:
                return original, False
            result = tx.update_team_task(
                original.id,
                expected_revision=arguments.expected_revision,
                source_event_id=source_id,
                **options,
            )
            return result, True

        return await mutate("team_task_update", arguments, context, action)

    async def roster(arguments: TeamMembersInput, context: ToolContext) -> TeamListOutput:
        def read():
            with store.transaction(actor) as tx:
                guard(tx, context, write=False)
                matched = [
                    member
                    for member in members.in_tx(tx).list(actor)
                    if arguments.query.casefold() in member.display_name.casefold()
                ]
                items: tuple[dict[str, JsonValue], ...] = tuple(
                    {
                        "member_id": member.member_id,
                        "display_name": member.display_name[:200],
                        "name_source": member.name_source,
                        "membership_state": member.membership_state,
                        "name_truncated": len(member.display_name) > 200,
                        "revision": member.revision,
                    }
                    for member in matched[: arguments.limit]
                )
                return TeamListOutput(items=items, incomplete=len(matched) > arguments.limit)

        return await asyncio.to_thread(read)

    async def tasks(arguments: TeamTasksInput, context: ToolContext) -> TeamListOutput:
        def read():
            with store.transaction(actor) as tx:
                current, _ = guard(tx, context, write=False)
                candidates = tx.find_team_tasks(
                    arguments.query,
                    owner_id=arguments.owner_id,
                    status=TeamTaskState(arguments.status) if arguments.status else None,
                    limit=arguments.limit * 5,
                )
                selected: list[TeamTask] = []
                items: list[dict[str, JsonValue]] = []
                for task in candidates:
                    try:
                        task_sources(tx, task)
                    except DomainError as error:
                        if error.code in {
                            "source_unavailable",
                            "permission_denied",
                            "not_found",
                            "scope_violation",
                        }:
                            continue
                        raise
                    selected.append(task)
                    owner_name = None
                    if task.owner_id:
                        try:
                            owner_name = (
                                members.in_tx(tx).get(actor, task.owner_id).display_name[:200]
                            )
                        except DomainError:
                            pass
                    items.append(
                        {
                            "task_id": task.id,
                            "kind": "human_team_task",
                            "title": task.title,
                            "owner_id": task.owner_id,
                            "owner_name": owner_name,
                            "status": task.status.value,
                            "progress": task.progress,
                            "due_at": task.due_at.isoformat() if task.due_at else None,
                            "revision": task.revision,
                            "provenance_kind": task.provenance_kind,
                        }
                    )
                    if len(items) >= arguments.limit:
                        break
                consume(tx, current, selected)
                return TeamListOutput(
                    items=tuple(items), incomplete=len(candidates) >= arguments.limit
                )

        return await asyncio.to_thread(read)

    for name, input_model, handler, description in (
        (
            "team_members",
            TeamMembersInput,
            roster,
            "Read this group's roster. Group nicknames take precedence; names do not grant authority.",
        ),
        (
            "team_tasks",
            TeamTasksInput,
            tasks,
            "Read public human TeamTasks with actual status/revision. Human tasks are different from agent WorkOrders.",
        ),
    ):
        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=input_model,
                output_model=TeamListOutput,
                handler=handler,
                risk_class="read_only",
                idempotency_mode=IdempotencyMode.READ_ONLY,
                resource_scopes=("group", "work", "team_tasks"),
                timeout_seconds=15,
                max_output_bytes=256_000,
            )
        )
    if (
        work.scope.get("team_task_write_allowed") is not True
        or type(work.scope.get("derived_depth", 0)) is not int
        or work.scope.get("derived_depth", 0) != 0
        or work.scope.get("origin_schedule_id")
        or work.scope.get("origin_trigger_rule_id")
    ):
        return
    for write_name, write_input_model, write_handler, write_description in (
        (
            "team_task_create",
            TeamTaskCreateInput,
            create,
            "Create an authorized human task; Member assigns self, Owner may assign a uniquely identified group member. The task starts open.",
        ),
        (
            "team_task_claim",
            TeamTaskClaimInput,
            claim,
            "Claim only a specifically authorized unassigned human task for the current requester, with an exact revision.",
        ),
        (
            "team_task_update",
            TeamTaskUpdateInput,
            update,
            "Update only an authorized human task with an exact revision. Completed requires explicit human approval; never infer it from an AI artifact.",
        ),
    ):
        gateway.register(
            ToolDefinition(
                name=write_name,
                description=write_description,
                input_model=write_input_model,
                output_model=TeamTaskReceipt,
                handler=write_handler,
                idempotency_mode=IdempotencyMode.MANAGED_TRANSACTION,
                resource_scopes=("group", "work", "team_tasks"),
                may_request_input=True,
                timeout_seconds=15,
                max_input_bytes=20_000,
                max_output_bytes=8000,
            )
        )
