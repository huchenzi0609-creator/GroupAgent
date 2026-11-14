"""Natural dialogue composed with scoped, transactional business operations."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb
from pydantic import JsonValue, ValidationError

from groupagent.channels.models import AgentWechatConfig, stable_key
from groupagent.config import AppConfig
from groupagent.coordinator import (
    CoordinationResult,
    Coordinator,
    Decision,
    DecisionProvider,
    ObserveOnlyProvider,
)
from groupagent.domain import (
    ActorContext,
    DomainError,
    EventKind,
    GroupEvent,
    Role,
    TeamTaskState,
    WorkState,
)
from groupagent.memory import MemberService, MemoryService
from groupagent.memory.retrieval import visible_events
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.runtime.artifacts import ArtifactRegistry
from groupagent.scheduling import Scheduler

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
from .intent import (
    explicitly_requires_execution,
    main_request,
    requires_research,
    task_write_request,
)
from .participation import fast_observe, policy_intent
from .quiet import is_quiet

ProviderFactory = Callable[[ActorContext, GroupEvent], DecisionProvider]
_ERRORS = {
    "feature_disabled": "这项能力目前已关闭，已有资料和任务会保留。",
    "permission_denied": "你目前没有这项操作的权限；可以处理自己的任务，其他分工请由负责人调整。",
    "ambiguous_reference": "有多个可能的对象，请说清楚是哪一项。",
    "not_found": "暂时没有找到对应记录，请补充任务或文件名称。",
    "version_conflict": "这项记录刚有更新，请以最新记录为准。",
    "quota_exceeded": "这项操作超出了当前额度，我先保留现有安排。",
    "source_unavailable": "相关内容已不可用，我不会继续引用或处理它。",
    "unsupported_schedule": "目前支持一次性、按小时、每日或每周的安排。",
    "invalid_input": "这项操作还缺少清楚的时间或对象，请补充一下。",
}


def _strings(value: Any) -> list[str]:
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


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
        self.execution_guard: Callable[..., None] = lambda *args: None
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
        topic_id = None
        quoted_work_id = None
        quoted_work_source = None
        if event.reply_to:
            with self.store.transaction(actor) as tx:
                quoted_message_id = None
                direct_work = tx.conn.execute(
                    "SELECT id FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
                    (tx.group_id, event.reply_to),
                ).fetchone()
                if direct_work:
                    quoted_work_id = direct_work["id"]
                    quoted_work_source = event.reply_to
                try:
                    quoted = tx.get_event(event.reply_to)
                except DomainError as error:
                    if error.code != "not_found":
                        raise
                else:
                    quoted_message_id = quoted.platform_message_id
                if (
                    quoted_message_id is None
                    and event.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
                    and event.channel_account_id == self.config.runtime.channel_account_id
                ):
                    binding = AgentWechatConfig(
                        base_url=self.config.runtime.channel_base_url,
                        account_id=self.config.runtime.channel_account_id,
                        external_group_id=self.config.group.external_group_id,
                        internal_group_id=actor.group_id,
                        source_epoch=self.config.runtime.source_epoch,
                    )
                    for part in event.attachments:
                        mid = part.get("server_id")
                        if (
                            part.get("kind") == "quote"
                            and isinstance(mid, str)
                            and event.reply_to
                            == str(uuid5(NAMESPACE_URL, stable_key(binding, mid, server=True)))
                        ):
                            quoted_message_id = mid
                            break
                if quoted_message_id:
                    found = tx.conn.execute(
                        "SELECT COALESCE(d.work_id,n.work_id,w.id) AS work_id FROM ga_deliveries d "
                        "LEFT JOIN ga_delivery_parts p ON p.group_id=d.group_id AND p.intent_id=d.id "
                        "LEFT JOIN ga_progress_notices n ON n.group_id=d.group_id AND n.intent_id=d.id "
                        "LEFT JOIN ga_work_orders w ON w.group_id=d.group_id AND w.request_event_id=d.request_event_id "
                        "WHERE d.group_id=%s AND ((d.state='accepted' AND d.platform_message_id=%s) "
                        "OR (p.state='accepted' AND p.platform_message_id=%s)) LIMIT 1",
                        (tx.group_id, quoted_message_id, quoted_message_id),
                    ).fetchone()
                    if found and found["work_id"]:
                        linked_work = tx.get_work(found["work_id"])
                        if not linked_work.scope.get("privacy_redacted"):
                            quoted_work_id = linked_work.id
                            quoted_work_source = linked_work.request_event_id
                            candidate_topic = linked_work.scope.get("topic_id")
                            if isinstance(candidate_topic, str):
                                topic_id = candidate_topic
        current_topics = self.memory.topics_for_event(actor, event.id)
        if any(topic.visibility == "participants" for topic in current_topics):
            raise DomainError("permission_denied", "A group reply cannot expose a restricted topic")
        if len(current_topics) == 1:
            topic_id = current_topics[0].id
        if event.reply_to:
            try:
                linked = self.memory.topics_for_event(actor, event.reply_to)
                if any(topic.visibility == "participants" for topic in linked):
                    raise DomainError(
                        "permission_denied", "A group reply cannot expose a restricted quote"
                    )
                if len(linked) == 1:
                    topic_id = linked[0].id
            except DomainError as error:
                if error.code == "permission_denied":
                    raise
        with self.store.transaction(actor) as tx:
            self.memory.prepare_in_tx(tx)
            if tx.get_event(event.id) != event:
                raise DomainError(
                    "memory_changed",
                    "Canonical message changed before context construction",
                    retryable=True,
                )
            recent = visible_events(
                tx, topic_id=topic_id, limit=24, newest_first=True, public_only=True
            )
            canonical_recent = {row["id"]: tx.get_event(row["id"]) for row in recent}
            current_sources = [event.id] + [row["id"] for row in recent]
            if quoted_work_source and self.memory.in_tx(tx).source_available(
                actor, quoted_work_source
            ):
                current_sources.append(quoted_work_source)
            if event.reply_to and self.memory.in_tx(tx).source_available(actor, event.reply_to):
                current_sources.append(event.reply_to)
            snapshot = self.memory.snapshot_in_tx(
                tx,
                "",
                topic_id=topic_id,
                limit=30,
                source_event_ids=list(dict.fromkeys(current_sources)),
            )
            # Query meaningful nouns as well as recent context; full question
            # strings must not be treated as an exact database phrase.
            terms = re.findall(
                r"[A-Za-z][A-Za-z0-9_-]{1,30}|主题|截止|分工|负责人|进度|参考文献|方案|结论|决定|报告|资料|提醒",
                event.text,
            )
            hits = {(item["kind"], item["id"]): item for item in snapshot["items"]}
            for term in list(dict.fromkeys(terms))[:3]:
                extra = self.memory.snapshot_in_tx(tx, term, topic_id=topic_id, limit=10)
                snapshot["source_fingerprints"].update(extra["source_fingerprints"])
                hits.update({(item["kind"], item["id"]): item for item in extra["items"]})
            snapshot["items"] = list(hits.values())[:50]
            private_topics = {
                row["id"]
                for row in tx.conn.execute(
                    "SELECT id FROM ga_memory_topics WHERE group_id=%s AND visibility='participants'",
                    (actor.group_id,),
                ).fetchall()
            }
            private_events = {
                row["event_id"]
                for row in tx.conn.execute(
                    "SELECT r.event_id FROM ga_topic_events r JOIN ga_memory_topics t ON t.group_id=r.group_id AND t.id=r.topic_id WHERE r.group_id=%s AND t.visibility='participants'",
                    (actor.group_id,),
                ).fetchall()
            }
            if (
                event.id in private_events
                or event.reply_to in private_events
                or quoted_work_source in private_events
            ):
                raise DomainError(
                    "permission_denied", "Restricted evidence cannot enter a group reply"
                )
            snapshot["items"] = [
                item
                for item in snapshot["items"]
                if item.get("topic_id") not in private_topics
                and not any(ref["event_id"] in private_events for ref in item["source_refs"])
            ]
            admitted_sources = set(current_sources) - private_events
            admitted_task_ids: set[str] = set()
            bounded_items = []
            context_incomplete = False
            for item in snapshot["items"]:
                closure = {ref["event_id"] for ref in item["source_refs"]}
                logical = set(item.get("source_task_revisions", {}))
                if len(admitted_sources | closure) > 350 or len(admitted_task_ids | logical) > 100:
                    context_incomplete = True
                    continue
                admitted_sources.update(closure)
                admitted_task_ids.update(logical)
                bounded_items.append(item)
            snapshot["items"] = bounded_items
            for item in snapshot["items"]:
                if item["kind"] in {"fact", "decision"}:
                    fact = tx.conn.execute(
                        "SELECT revision,predicate,value FROM ga_memory_facts WHERE group_id=%s AND id=%s AND state='active'",
                        (tx.group_id, item["id"]),
                    ).fetchone()
                    if fact:
                        item.update(fact)
            consumed = set(current_sources) | {
                ref["event_id"] for item in snapshot["items"] for ref in item["source_refs"]
            }
            snapshot["source_fingerprints"] = {
                key: value
                for key, value in snapshot["source_fingerprints"].items()
                if key in consumed and key not in private_events
            }
            member_rows = self.members.in_tx(tx).list(actor, include_left=True)
            member_ids = sorted(
                {member.member_id for member in member_rows}
                | {event.actor_id}
                | {row["actor_id"] for row in recent}
            )
            aliases = {
                f"member_{index + 1}": member_id for index, member_id in enumerate(member_ids)
            }
            inverse = {value: key for key, value in aliases.items()}
            works = [
                w.model_dump(mode="json")
                for w in tx.list_work(limit=30, active_first=True, newest_first=True)
                if not w.scope.get("privacy_redacted")
                and w.request_event_id not in private_events
                and not _scope_sources(w.scope) & private_events
                and tx.get_event(w.request_event_id).content_state == "active"
            ]
            control_works = list(works)
            follow_up = bool(
                re.search(
                    r"刚才|刚刚|那份|那项|之前|上次|继续|进度|状态|暂停|停止|取消|恢复|重试|哪些工作|改成|改短|修改",
                    event.text,
                )
            )
            if topic_id is None and quoted_work_id is None:
                if re.search(r"新建|独立工作|单独做|单独生成", event.text):
                    works = []
                else:
                    works = [
                        row
                        for row in works
                        if row["state"] not in {"completed", "cancelled", "failed"} or follow_up
                    ][:10]
            topic_events = (
                {
                    row["event_id"]
                    for row in tx.conn.execute(
                        "SELECT event_id FROM ga_topic_events WHERE group_id=%s AND topic_id=%s",
                        (actor.group_id, topic_id),
                    ).fetchall()
                }
                if topic_id
                else None
            )
            tasks = [
                task.model_dump(mode="json")
                for task in tx.list_team_tasks(limit=100)
                if task.content_state == "active"
                and not set(task.source_event_ids) & private_events
                and not any(
                    tx.get_event(identifier).content_state in {"forgotten", "recalled"}
                    for identifier in task.source_event_ids
                )
                and (topic_events is None or bool(set(task.source_event_ids) & topic_events))
            ]
            rules = [
                rule.model_dump(mode="json")
                for rule in self.scheduler.in_tx(tx).list(actor)
                if not rule.scope.get("privacy_redacted")
                and not _scope_sources(rule.scope) & private_events
                and rule.scope.get("topic_id") not in private_topics
                and not any(
                    tx.get_event(source).content_state in {"forgotten", "recalled"}
                    for source in _scope_sources(rule.scope)
                )
            ]
            topics = tx.conn.execute(
                "SELECT t.id,t.title,t.visibility,o.source_event_ids FROM ga_memory_topics t LEFT JOIN ga_topic_origins o ON o.group_id=t.group_id AND o.topic_id=t.id WHERE t.group_id=%s AND t.visibility='group' ORDER BY t.created_at DESC LIMIT 30",
                (actor.group_id,),
            ).fetchall()
            topics = [
                topic
                for topic in topics
                if (
                    topic["id"] == topic_id
                    or topic["id"] in {row["scope"].get("topic_id") for row in works}
                )
                and not any(
                    getattr(tx.get_event(identifier), "content_state", "active") != "active"
                    or any(
                        part.get("reason") in {"forgotten", "recalled"}
                        for part in tx.get_event(identifier).attachments
                    )
                    for identifier in (topic["source_event_ids"] or [])
                )
            ]
            bounded_topics = []
            for topic in topics:
                if (
                    len(set(snapshot["source_fingerprints"]) | set(topic["source_event_ids"] or []))
                    > 400
                ):
                    context_incomplete = True
                    continue
                bounded_topics.append(topic)
                for identifier in topic["source_event_ids"] or []:
                    snapshot["source_fingerprints"][identifier] = tx.event_fingerprint(identifier)
            topics = bounded_topics
            if topic_id and topic_id not in {topic["id"] for topic in topics}:
                topics.append(
                    {
                        "id": topic_id,
                        "title": "当前讨论",
                        "visibility": "group",
                        "source_event_ids": [event.id],
                    }
                )
            visible_topic_ids = {topic["id"] for topic in topics}
            works = [
                work
                for work in works
                if (
                    work["scope"].get("topic_id") is None
                    or work["scope"].get("topic_id") in visible_topic_ids
                )
                and (topic_id is None or work["scope"].get("topic_id") == topic_id)
            ]
            retained_context = set()
            admitted_rows: dict[str, list[dict[str, Any]]] = {"tasks": [], "works": [], "rules": []}
            for kind, rows in (("tasks", tasks), ("works", works), ("rules", rules)):
                for row in rows:
                    closure = _scope_sources(row.get("scope", {}))
                    closure.update(_strings(row.get("source_event_ids")))
                    source = row.get("request_event_id")
                    if isinstance(source, str):
                        closure.add(source)
                    logical = (
                        {row["id"]}
                        if kind == "tasks"
                        else set(row.get("scope", {}).get("source_task_revisions", {}))
                    )
                    if (
                        len(set(snapshot["source_fingerprints"]) | closure) > 400
                        or len(admitted_task_ids | logical) > 100
                    ):
                        context_incomplete = True
                        continue
                    if any(
                        tx.get_event(identifier).content_state in {"forgotten", "recalled"}
                        for identifier in closure
                    ):
                        continue
                    snapshot["source_fingerprints"].update(
                        {identifier: tx.event_fingerprint(identifier) for identifier in closure}
                    )
                    admitted_rows[kind].append(row)
                    admitted_task_ids.update(logical)
            tasks, works, rules = (
                admitted_rows["tasks"],
                admitted_rows["works"],
                admitted_rows["rules"],
            )
            for task in tasks:
                for identifier in task["source_event_ids"]:
                    snapshot["source_fingerprints"][identifier] = tx.event_fingerprint(identifier)
                    if tx.get_event(identifier).content_state == "expired":
                        retained_context.add(identifier)
            for row in works + rules:
                sources = _scope_sources(row["scope"])
                original = row.get("request_event_id")
                if isinstance(original, str):
                    sources.add(original)
                for identifier in sources:
                    snapshot["source_fingerprints"][identifier] = tx.event_fingerprint(identifier)
            snapshot["retained_context_source_ids"] = sorted(retained_context)
            task_ids = set()
            for item in snapshot["items"]:
                task_ids.update(item.get("source_task_revisions", {}))
            task_ids.update(task["id"] for task in tasks)
            for row in works + rules:
                task_ids.update(row.get("scope", {}).get("source_task_revisions", {}))
            snapshot["source_task_revisions"] = {
                identifier: tx.get_team_task(identifier).revision for identifier in task_ids
            }
            policies = tx.conn.execute(
                "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s)",
                (actor.group_id, actor.member_id),
            ).fetchall()

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

        def alias_value(value, field_name=None):
            if isinstance(value, str):
                return inverse.get(value, value) if field_name in identity_fields else value
            if isinstance(value, list):
                return [alias_value(item, field_name) for item in value]
            if isinstance(value, dict):
                return {key: alias_value(item, key) for key, item in value.items()}
            return value

        memory_items = []
        for item in snapshot["items"]:
            text = item["text"]
            memory_items.append(
                {
                    key: value
                    for key, value in {
                        **item,
                        "text": text[:2500],
                        "context_excerpt_truncated": len(text) > 2500,
                        "source_event_ids": [ref["event_id"] for ref in item["source_refs"][:20]],
                        "source_refs_truncated": len(item["source_refs"]) > 20,
                    }.items()
                    if key
                    in {
                        "kind",
                        "id",
                        "text",
                        "topic_id",
                        "revision",
                        "predicate",
                        "value",
                        "context_excerpt_truncated",
                        "source_event_ids",
                        "source_refs_truncated",
                    }
                }
            )
        public_works = []
        for row in works:
            projected = {
                key: row[key]
                for key in (
                    "id",
                    "requester_id",
                    "request_event_id",
                    "goal",
                    "state",
                    "revision",
                    "goal_revision",
                    "input_versions",
                    "acceptance_criteria",
                )
            }
            projected["goal"] = str(projected["goal"])[:3000]
            projected["acceptance_criteria"] = [
                str(value)[:500] for value in projected["acceptance_criteria"]
            ]
            projected["scope"] = {
                key: value
                for key, value in row["scope"].items()
                if key in {"topic_id", "outputs", "delivery_artifacts", "speech_requested"}
            }
            question = row["scope"].get("_runtime_input_request")
            if (
                isinstance(question, dict)
                and row["state"] == "waiting_input"
                and question.get("goal_revision") == row["goal_revision"]
            ):
                projected["waiting_question"] = question.get("question")
            public_works.append(projected)
        public_tasks = [
            {
                key: row[key]
                for key in (
                    "id",
                    "title",
                    "owner_id",
                    "created_by",
                    "status",
                    "progress",
                    "due_at",
                    "revision",
                )
                if key in row
            }
            for row in tasks
        ]
        public_rules = [
            {
                key: row[key]
                for key in (
                    "id",
                    "creator_id",
                    "title",
                    "goal",
                    "state",
                    "timing",
                    "time_zone",
                    "next_due_at",
                    "valid_until",
                    "revision",
                )
            }
            for row in rules
        ]
        payload = {
            "now": self.clock().astimezone(ZoneInfo(self.config.group.time_zone)).isoformat(),
            "group_time_zone": self.config.group.time_zone,
            "role": actor.role.value,
            "read_only": actor.role in {Role.OBSERVER, Role.MAINTAINER},
            "capabilities": self.config.features.model_dump(mode="json"),
            "requester_member_id": inverse[actor.member_id],
            "members": [
                {
                    "member_id": member.member_id,
                    "name": member.display_name,
                    "name_source": member.name_source,
                    "membership_state": member.membership_state,
                    "revision": member.revision,
                }
                for member in member_rows
            ],
            "works": public_works,
            "tasks": public_tasks,
            "schedules": public_rules,
            "topics": [{key: row[key] for key in ("id", "title", "visibility")} for row in topics],
            "memory": memory_items,
            "summaries_are_lossy": True,
            "context_incomplete": context_incomplete,
            "quoted_work_id": quoted_work_id
            if quoted_work_id in {w["id"] for w in works}
            else None,
            "recent_messages": [
                {
                    "id": row["id"],
                    "actor_id": row["actor_id"],
                    "kind": row["kind"],
                    "occurred_at": tx_event.occurred_at.isoformat(),
                    "text": tx_event.text[:4000],
                    "reply_to": tx_event.reply_to,
                    "attachments": tx_event.attachments,
                }
                for row in reversed(recent)
                for tx_event in [canonical_recent[row["id"]]]
            ],
            "model_event": {
                "id": event.id,
                "actor_id": event.actor_id,
                "kind": event.kind.value,
                "text": event.text,
                "occurred_at": event.occurred_at.astimezone(
                    ZoneInfo(self.config.group.time_zone)
                ).isoformat(),
                "reply_to": event.reply_to,
                "attachments": event.attachments,
            },
        }
        payload = alias_value(payload)
        payload.update(
            {
                "_snapshot": snapshot,
                "_aliases": aliases,
                "_policies": policies,
                "_topic_id": topic_id,
                "_works": works,
                "_control_works": control_works,
                "_tasks": tasks,
                "_rules": rules,
                "_quoted_work_id": quoted_work_id,
            }
        )
        if self.artifacts:
            candidates = self.artifacts.list(actor, limit=30, public_context=True)
            if quoted_work_id:
                bound_files = self.artifacts.list(
                    actor, limit=20, public_context=True, work_id=quoted_work_id
                )
                by_version = {row["version_id"]: row for row in bound_files + candidates}
                candidates = list(by_version.values())
            files = []
            with self.store.transaction(actor) as tx:
                self.memory.prepare_in_tx(tx)
                self.memory.assert_snapshot_in_tx(tx, snapshot)
                retained = set(snapshot.get("artifact_retained_source_ids", []))
                metadata_versions = []
                for candidate in candidates:
                    fingerprints, expired = self.artifacts.consume_in_tx(
                        tx, candidate["version_id"]
                    )
                    file_tasks = self.artifacts.task_sources_in_tx(tx, candidate["version_id"])
                    if (
                        len(set(snapshot["source_fingerprints"]) | set(fingerprints)) > 400
                        or len(set(snapshot["source_task_revisions"]) | set(file_tasks)) > 100
                    ):
                        payload["context_incomplete"] = True
                        continue
                    snapshot["source_fingerprints"].update(fingerprints)
                    snapshot["source_task_revisions"].update(file_tasks)
                    retained.update(expired)
                    metadata_versions.append(candidate["version_id"])
                    files.append(candidate)
                snapshot["artifact_retained_source_ids"] = sorted(retained)
                snapshot["context_artifact_versions"] = metadata_versions
            payload["files"] = files

        # Bound the actual serialized model context as well as provenance fanout.
        # Drop complete optional entries; the canonical database remains intact.
        def public_bytes():
            return len(
                json.dumps(
                    {key: value for key, value in payload.items() if not key.startswith("_")},
                    ensure_ascii=False,
                    default=str,
                ).encode()
            )

        for key, minimum in (
            ("memory", 0),
            ("recent_messages", 2),
            ("files", 0),
            ("works", 1),
            ("schedules", 1),
            ("tasks", 1),
        ):
            while public_bytes() > 90_000 and len(payload.get(key, [])) > minimum:
                payload[key].pop(0 if key == "recent_messages" else -1)
                payload["context_incomplete"] = True
        return payload

    def _fast_control(self, event: GroupEvent, context: dict) -> Decision | None:
        if event.kind not in {EventKind.TEXT, EventKind.QUOTE}:
            return None
        text = re.sub(r"^\s*(?:/组长|@?机器人组长|@?组长)[，,:：\s]*", "", event.text).strip()
        match = re.fullmatch(
            r"(停止|取消|暂停|先停一下|停一下|别做了)(?:这个任务|这项任务|刚才的任务|这个工作|任务)?[。！!\s]*",
            text,
        )
        if not match:
            return None
        works = [
            work
            for work in context.get("_control_works", context["_works"])
            if (work["requester_id"] == event.actor_id or context["role"] == "owner")
            and work["state"] not in {"completed", "failed", "cancelled"}
        ]
        if event.reply_to:
            quoted = [
                work
                for work in works
                if work["request_event_id"] == event.reply_to
                or work["id"] == context.get("_quoted_work_id")
            ]
            # An explicit quote never falls back to an unrelated work owned by
            # the speaker, even when only one such work is running.
            works = quoted
        if len(works) != 1:
            return Decision(
                kind="clarify",
                reason_code="ambiguous_control",
                response="你想停哪一项任务？告诉我任务名称即可。",
                directness="direct",
                confidence=1,
            )
        work = works[0]
        return Decision(
            kind="control_work",
            reason_code="explicit_local_control",
            target_work_id=work["id"],
            expected_revision=work["revision"],
            control="pause" if match[1] in {"暂停", "先停一下", "停一下"} else "cancel",
            directness="direct",
            confidence=1,
        )

    def _gate(
        self, tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict
    ) -> Decision:
        observed = fast_observe(event, context)
        if observed is not None:
            return observed
        write = decision.action is not None or decision.kind in {
            "start_work",
            "update_work",
            "control_work",
        }
        if event.kind == EventKind.FORWARDED and write:
            return Decision(kind="observe", reason_code="forwarded_data_not_authority")
        if (
            decision.kind == "reply"
            and decision.action is None
            and decision.directness in {"direct", "open"}
        ):
            requested_execution = event.kind in {
                EventKind.TEXT,
                EventKind.QUOTE,
            } and explicitly_requires_execution(event.text)
            unsupported = bool(re.search(r"(?:无法|不能|不支持|没有权限)", decision.response or ""))
            if requested_execution and not unsupported:
                formats: list[dict[str, JsonValue]] = []
                for pattern, format_name in (
                    (r"Word|docx", "docx"),
                    (r"PPT|pptx|幻灯片", "pptx"),
                    (r"PDF|pdf", "pdf"),
                    (r"Markdown|markdown|\.md", "md"),
                ):
                    if re.search(pattern, event.text):
                        formats.append({"format": format_name})
                decision = Decision(
                    kind="start_work",
                    reason_code="actual_execution_required",
                    goal=event.text,
                    directness=decision.directness,
                    confidence=decision.confidence,
                    outputs=formats,
                )
                write = True
            elif re.search(
                r"(?:已经|已)(?:成功)?(?:执行|运行|生成并发送|发送邮件|发邮件|付款|支付|上传到|提交到)",
                decision.response or "",
            ):
                decision = Decision(
                    kind="reply",
                    reason_code="unverified_effect_claim",
                    response="目前没有这项操作已完成的执行凭证，我不能把它说成已经完成。",
                    directness=decision.directness,
                    confidence=1,
                )
        if decision.action and any(
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
        owner_assignment = (
            tx.actor.role == Role.OWNER
            and decision.action
            and decision.action.get("op") in {"assign_task", "task_batch"}
        )
        if decision.directness in {"other", "none"} and not owner_assignment:
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
        policies = tx.conn.execute(
            "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s) FOR UPDATE",
            (tx.group_id, tx.actor.member_id),
        ).fetchall()
        is_policy_command = decision.action and decision.action.get("op") in {
            "quiet",
            "participation",
        }
        if not is_policy_command and decision.kind != "control_work":
            mode = next(
                (p["mode"] for p in policies if p["subject_id"] == "*"),
                self.config.group.participation_mode,
            )
            if mode in {"paused", "disabled"}:
                return Decision(kind="observe", reason_code="group_paused")
            if mode == "on_request" and decision.directness not in {"direct", "open"}:
                return Decision(kind="observe", reason_code="on_request_mode")
            quiet = is_quiet(
                policies, now, self.config.group.time_zone, self.config.group.quiet_hours
            )
            if quiet and decision.directness not in {"direct"}:
                return Decision(kind="observe", reason_code="quiet_policy")
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
                snapshot["source_fingerprints"].update(fingerprints)
                snapshot.setdefault("source_task_revisions", {}).update(
                    self.artifacts.task_sources_in_tx(tx, version_id)
                )
                retained.update(expired)
        snapshot["artifact_retained_source_ids"] = sorted(retained)
        return selected

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
            rule = self.scheduler.in_tx(tx).create(
                tx.actor,
                action.title,
                action.goal,
                action.timing,
                now=self.clock(),
                time_zone=self.config.group.time_zone,
                valid_until=action.valid_until,
                request_key="chat:" + event.id,
                budget=self.config.execution.model_dump(mode="json"),
                scope={
                    "source_event_ids": list(context["_snapshot"]["source_fingerprints"]),
                    "source_fingerprints": context["_snapshot"]["source_fingerprints"],
                    "topic_id": context["_topic_id"],
                    "reminder_only": bool(
                        re.search(r"(?:提醒|通知)(?:我|我们|大家|一下|：|:)", event.text)
                    )
                    and not bool(
                        re.search(
                            r"(?:你|帮我|替我|机器人).{0,8}(?:实际|执行|生成|制作|搜索|整理成)",
                            event.text,
                        )
                    ),
                },
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
            if rule.goal != previous_schedule.goal:
                # A goal replacement cannot inherit the old reminder hint.
                # Only the current authorized request/goal may re-establish it.
                current_request = re.sub(
                    r"“[^”]*”|『[^』]*』|「[^」]*」|\"[^\"]*\"",
                    "<quoted data>",
                    main_request(event.text),
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
                        r"|(?:并|另外|同时|顺便).{0,5}(?:帮我|替我|由你|你来|机器人|组长)",
                        reminder_text,
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
                text = (
                    "我先保持安静；你明确叫我时，我仍会回应。"
                    if action.enabled
                    else "我恢复参与讨论了。"
                )
            else:
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
        if event.kind == EventKind.RECALL:
            self.memory.recall(self.service_actor, event.id, now=self.clock())
        context = self._context(actor, event)
        if self.config.group.bot_member_id == event.actor_id and not echo:
            if not self.config.group.shared_account:
                echo = True
            elif self._fast_control(event, context) is None:
                with self.store.transaction(actor) as tx:
                    pending = tx.conn.execute(
                        "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND state IN ('submitted','unknown') LIMIT 1",
                        (actor.group_id,),
                    ).fetchone()
                if pending:
                    raise DomainError(
                        "echo_reconciliation_pending",
                        "Same-account input awaits send identity reconciliation",
                        retryable=True,
                    )
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
                decision = self._fast_control(event, context) or await self.provider_factory(
                    actor, event
                ).decide(event, context)
        with self.store.transaction(actor, trace_id=event.id) as tx:
            self.memory.prepare_in_tx(tx)
            self.execution_guard(tx)
            previous = tx.get_decision(event.id)
            if previous:
                return CoordinationResult.model_validate(previous["result"]).model_copy(
                    update={"duplicate": True}
                )
            self.memory.assert_snapshot_in_tx(tx, context["_snapshot"])
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
            action_result = None
            original_snapshot = deepcopy(context["_snapshot"])
            try:
                # Savepoint lets a rejected candidate produce an honest response
                # without committing any partial business mutation.
                with tx.conn.transaction():
                    if decision.action is not None:
                        action_payload = decision.action
                        action_result, text = self._apply_action(tx, event, decision, context)
                        decision = decision.model_copy(update={"kind": "reply", "response": text})
                        if action_payload.get("op") == "forget":
                            context["_snapshot"]["source_fingerprints"] = {
                                event.id: tx.event_fingerprint(event.id)
                            }
                            context["_snapshot"]["source_task_revisions"] = {}
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
                        outputs = [
                            OutputRequirement.model_validate(value) for value in decision.outputs
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
                        attachment_ids = self._inputs(tx, event, decision, context)
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
                        speech_requested = bool(
                            re.search(
                                r"语音(?:回复|回答)|朗读|读出来|读给我|念给我|说给我听",
                                request_text,
                            )
                        ) and not bool(
                            re.search(r"(?:不要|不用|别).{0,5}(?:语音|朗读)", request_text)
                        )
                        if speech_requested and not self.config.features.speech_reply_on_request:
                            raise DomainError("feature_disabled", "Speech output is disabled")
                        delivery_artifacts = not bool(
                            re.search(
                                r"(?:不要|不用|先别|暂时别|暂不|别).{0,6}(?:发送|发到群|发群|发出来|发给)",
                                request_text,
                            )
                        )
                        research_required = requires_research(request_text)
                        direct_fetch = bool(
                            re.match(
                                r"(?:真正|实际|现在|直接)?(?:读取|打开|查看|访问).{0,20}https?://",
                                main_request(request_text),
                            )
                        )
                        research_required = research_required or direct_fetch
                        if research_required and not self.config.features.web_research:
                            raise DomainError("feature_disabled", "Web research is disabled")
                        required_tools = []
                        main = main_request(request_text)
                        if re.match(
                            r"(?:真正|实际|现在|直接)?(?:用|使用)(?:受控的?)?\s*[Pp]ython", main
                        ):
                            required_tools.append("python_compute")
                        elif re.match(
                            r"(?:真正|实际|现在|直接)?(?:用|使用)(?:受控的?)?\s*(?:[Ss]hell|进程)",
                            main,
                        ):
                            required_tools.append("process_run")
                        if speech_requested:
                            required_tools.append("speech_synthesize")
                        if direct_fetch:
                            required_tools.append("web_fetch")
                        if re.match(r"(?:联网)?(?:检索|搜索|上网.{0,3}查)", main):
                            required_tools.append("web_search")
                        team_write = task_write_request(request_text)
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
                            decision.goal or "",
                            event.id,
                            acceptance_criteria=decision.acceptance_criteria,
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
                                "topic_id": topic_id,
                                "speech_requested": speech_requested,
                                "delivery_artifacts": delivery_artifacts,
                                "research_required": research_required,
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
                            update={"response": "收到，我会处理这项工作，完成后把结果发回来。"}
                        )
                    elif decision.kind == "update_work":
                        attachment_ids = self._inputs(tx, event, decision, context)
                        changed_outputs = [
                            OutputRequirement.model_validate(value).model_dump(mode="json")
                            for value in decision.outputs
                        ]
                        work = tx.revise_work(
                            decision.target_work_id or "",
                            decision.goal or "",
                            expected_revision=decision.expected_revision or 0,
                            acceptance_criteria=decision.acceptance_criteria or None,
                            input_versions=decision.input_versions or None,
                            source_fingerprints=context["_snapshot"]["source_fingerprints"],
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
                    elif decision.kind == "control_work":
                        target = {
                            "cancel": WorkState.CANCELLED,
                            "pause": WorkState.PAUSED,
                            "resume": WorkState.QUEUED,
                            "retry": WorkState.QUEUED,
                        }[decision.control or "pause"]
                        work = tx.transition_work(
                            decision.target_work_id or "",
                            target,
                            expected_revision=decision.expected_revision or 0,
                            reason="retry" if decision.control == "retry" else "user_request",
                        )
                        work_id = work.id
                        decision = decision.model_copy(
                            update={
                                "response": {
                                    "pause": "已暂停后续执行。已发出的内容会保留记录。",
                                    "cancel": "已停止后续执行。已发出的内容无法自动撤回。",
                                    "resume": "已恢复排队，仍遵守原来的范围和预算。",
                                    "retry": "已安排重试，原有费用和执行记录会保留。",
                                }[decision.control or "pause"]
                            }
                        )
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
            if decision.response and decision.kind not in {"ignore", "observe"}:
                delivery = tx.enqueue_reply(
                    event.id,
                    decision.response,
                    read_only_authorized=actor.role.value in self.config.group.read_only_qa_roles,
                )
                delivery_id = delivery.id
                tx.conn.execute(
                    "INSERT INTO ga_participation_log(group_id,event_id,proactive,reason,created_at) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        actor.group_id,
                        event.id,
                        decision.directness == "proactive",
                        decision.reason_code,
                        self.clock(),
                    ),
                )
            result = CoordinationResult(
                event_id=event.id,
                decision=decision,
                work_id=work_id,
                delivery_id=delivery_id,
                action_result=action_result,
            )
            tx.record_decision(
                event.id,
                {
                    "kind": decision.kind,
                    "result": result.model_dump(mode="json"),
                    "source_event_ids": list(context["_snapshot"]["source_fingerprints"]),
                    "source_fingerprints": context["_snapshot"]["source_fingerprints"],
                    "source_task_revisions": context["_snapshot"].get("source_task_revisions", {}),
                },
            )
            return result
