"""Select a turn's context without turning the group's archive into its agenda.

The private work index is a lookup surface, not consumed evidence. Only rows
actually shown to the interpreter enter the one evidence snapshot. The control
plane hydrates a selected index reference before applying a mutation.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from groupagent.channels.models import AgentWechatConfig, stable_key
from groupagent.config import AppConfig
from groupagent.coordinator import CoordinationResult
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity
from groupagent.memory import MemberService, MemoryService
from groupagent.memory.causality import after_boundary_sql, causal_sources
from groupagent.memory.common import source_rows
from groupagent.memory.retrieval import visible_events
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.runtime.artifacts import ArtifactRegistry
from groupagent.runtime.work_facts import WorkFacts, visible_work_index_in_tx
from groupagent.runtime.work_status import work_status_detail
from groupagent.scheduling import Scheduler

from .intent import main_request, new_work_intent
from .participation import outside_quotes
from .references import authored_identifier
from .work_queries import compact_work_overview

_LIVE = frozenset(
    {"queued", "running", "waiting_input", "waiting_external", "validating", "delivering"}
)
_IDENTITY_FIELDS = frozenset(
    {
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
)
_STATE_WORDS = {
    "paused": "暂停|挂起|paused",
    "failed": "失败|failed",
    "completed": "完成|已结束|completed",
    "cancelled": "取消|cancelled",
    "running": "运行中|执行中|正在做|running",
    "waiting_input": "等.{0,6}(?:资料|补充|输入)|waiting_input",
}


def scope_sources(scope: Mapping[str, Any]) -> set[str]:
    values = scope.get("source_event_ids", [])
    result = (
        {value for value in values if isinstance(value, str)} if isinstance(values, list) else set()
    )
    fingerprints = scope.get("source_fingerprints", {})
    if isinstance(fingerprints, dict):
        result.update(fingerprints)
    if isinstance(scope.get("request_source_id"), str):
        result.add(scope["request_source_id"])
    return result


def _task_ids(scope: Mapping[str, Any]) -> set[str]:
    revisions = scope.get("source_task_revisions", {})
    return set(revisions) if isinstance(revisions, dict) else set()


@dataclass(frozen=True)
class ContextQuery:
    """A bounded read plan for an event or an explicit tool request."""

    new_request: bool = False
    continuation: bool = False
    resources: frozenset[str] = frozenset()
    states: frozenset[str] = frozenset()
    memory_terms: tuple[str, ...] = ()
    fact_predicate: str | None = None
    roster: bool = False
    operations: bool = False
    model_led: bool = False
    preview: bool = False

    @classmethod
    def for_agent(cls, event: GroupEvent) -> ContextQuery:
        # The model can request additional resources. Start with the delivery
        # timeline and references that are structurally present in this event.
        resources = {"tasks", "schedules"}
        if event.reply_to or event.attachments:
            resources.add("files")
        return cls(resources=frozenset(resources), model_led=True, preview=True)

    @classmethod
    def from_event(cls, event: GroupEvent, identity: BotIdentity) -> ContextQuery:
        text = outside_quotes(event.text)
        main = main_request(text, identity=identity)
        continuation = bool(
            re.search(
                r"刚才|刚刚|上次|之前|这份|那份|这项|那项|这个|那个|这张|那张|这些|那些|继续|接着|停止|取消|暂停|恢复|重试|改成|改短|修改|进度|状态|原因",
                main,
            )
        )
        new_request = not event.reply_to and (
            new_work_intent(event.text, identity=identity) is not None
            or bool(re.search(r"新建|独立工作|单独(?:做|生成)|重新发起", main))
            or bool(re.match(r"(?:搜索|检索|查找|找|生成|制作|分析|计算|解释|翻译|介绍)", main))
            and not continuation
        )
        resources: set[str] = set()
        arrangements = bool(
            re.search(
                r"(?:已有|现有|当前|之前|各项|所有).{0,8}(?:安排|分工|任务|工作|进度)|工作.{0,4}(?:进度|清单)|资料目录",
                main,
            )
        )
        state_query = bool(re.search(r"的|所有|哪些|列表|清单|处于|已经|已被", main))
        state_text = re.sub(r"^(?:暂停|停止|取消|恢复|重试)\s*", "", main)
        states = frozenset(
            state
            for state, words in _STATE_WORDS.items()
            if state_query and re.search(words, state_text, re.I)
        )
        if not new_request and (
            continuation
            or states
            or arrangements
            or re.search(r"工作|任务.{0,5}(?:停|取消|恢复)|停.{0,5}任务", main)
        ):
            resources.add("works")
        if arrangements or re.search(
            r"分工|负责人|认领|我(?:来|负责|接手|承担)|由我|任务.{0,5}(?:进展|完成|更新)|我.{0,6}(?:完成|做完)",
            main,
        ):
            resources.add("tasks")
        if arrangements or re.search(r"提醒|日程|定时|已有安排|现有安排", main):
            resources.add("schedules")
        if re.search(
            r"附件|(?:这|那)(?:[0-9一二两三四五六七八九十百]+\s*)?(?:份|张)|"
            r"(?:这些|那些)(?:文件|图片|照片|图像|截图|表情包|图)|原稿|草稿|版本|资料目录|"
            r"(?:已有|现有|之前|上次|刚才|已经|已).{0,16}(?:文件|报告|图片|照片|图像|截图|表情包)|"
            r"(?:文件|资料).{0,5}(?:目录|清单)",
            main,
        ):
            resources.add("files")
        operations = bool(
            re.search(
                r"(?:你|工作|任务|执行).{0,12}(?:超时|预算|上限|限制|暂停|失败)|(?:超时|预算).{0,10}(?:多少|原因|问题)",
                main,
            )
        )
        if operations and re.search(r"上限|配置|限制|设置|机制", main) and not event.reply_to:
            resources.discard("works")
        recall = bool(
            re.search(
                r"记忆|记住的|记下的|记录的|记得|上次|之前|历史|昨天.{0,8}(?:讨论|说)|忘记|更正记录",
                main,
            )
        )
        question = re.fullmatch(
            r"(.{1,60}?)(?:是什么|是啥|是多少|有多少|多少)"
            r"(?:[，,?？]\s*(?:请用|用)?一句话(?:回答|回复)(?:即可|就好)?)?[?？。!！\s]*",
            main,
        )
        terms: list[str] = []
        fact_predicate = None
        if (recall or question) and not operations:
            noun = question[1] if question else main
            noun = re.sub(
                r"^(?:(?:我|你)?(?:刚才|上次|之前|此前|已经)?(?:记住|记下|记录|提到|说过|讨论)(?:的|了)?|(?:告诉|告知)我|用一句话回答[，,：:]?)+",
                "",
                noun,
            ).strip()
            if question and noun:
                fact_predicate = noun
            if recall:
                terms = re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,30}", noun)
                if not terms and noun:
                    terms = [noun[:100]]
        return cls(
            new_request=new_request,
            continuation=continuation and not new_request,
            resources=frozenset(resources),
            states=states,
            memory_terms=tuple(dict.fromkeys(terms))[:2],
            fact_predicate=fact_predicate,
            roster=bool(re.search(r"群成员|群昵称|名单|群里.{0,5}(?:有谁|有哪些)|我的昵称", main)),
            operations=operations,
        )


@dataclass(frozen=True)
class ContextFocus:
    kind: str
    work_ids: tuple[str, ...] = ()
    source_event_ids: tuple[str, ...] = ()

    @property
    def work_id(self) -> str | None:
        return self.work_ids[0] if len(self.work_ids) == 1 else None


def result_reference(
    body: Mapping[str, Any], source_event_id: str, requester_id: str
) -> dict[str, Any] | None:
    """Read controller-owned result references, never IDs written in bot prose."""
    if body.get("privacy_redacted"):
        return None
    try:
        result = CoordinationResult.model_validate(body.get("result"))
    except ValidationError:
        return None
    if result.event_id != source_event_id or result.decision.kind in {"ignore", "observe"}:
        return None
    identifiers = [result.work_id] if result.work_id else []
    identifiers.extend(result.controlled_work_ids)
    action_ids = (result.action_result or {}).get("work_ids", [])
    if isinstance(action_ids, list):
        identifiers.extend(value for value in action_ids if isinstance(value, str) and value)
    identifiers = list(dict.fromkeys(identifiers))
    if not identifiers or len(identifiers) > 1000:
        return None
    return {
        "source_event_id": source_event_id,
        "requester_id": requester_id,
        "work_ids": identifiers,
    }


def select_focus(
    event: GroupEvent,
    query: ContextQuery,
    works: Sequence[Mapping[str, Any]],
    recent: Sequence[Mapping[str, Any]],
    *,
    quoted_work_id: str | None = None,
    quoted_reference: Mapping[str, Any] | None = None,
    identity: BotIdentity = DEFAULT_IDENTITY,
) -> ContextFocus:
    """Retain a typed discourse set; only an explicit new request resets its agenda."""
    if query.new_request:
        return ContextFocus("new_request")
    available = {row["id"] for row in works}
    if quoted_work_id and quoted_work_id in available:
        return ContextFocus("quote", (quoted_work_id,))
    if quoted_reference is not None:
        identifiers = tuple(
            value for value in quoted_reference.get("work_ids", ()) if value in available
        )
        sources = (quoted_reference["source_event_id"],) if identifiers else ()
        return ContextFocus("quote" if identifiers else "unavailable", identifiers, sources)
    if query.model_led:
        # Preserve the candidate references in recent messages and the work
        # overview. A lexical guess about the current goal cannot discard them.
        return ContextFocus("discussion")
    previous = [row for row in recent if row.get("id") != event.id]
    addressed = identity.directly_addressed(outside_quotes(event.text))
    quoted = next((row for row in previous if row.get("id") == event.reply_to), None)
    if (
        quoted is not None
        and quoted.get("speaker", "human") == "human"
        and quoted.get("actor_id") != event.actor_id
    ):
        return ContextFocus("human_discussion")
    for row in reversed(previous):
        if (
            row.get("speaker", "human") == "human"
            and row.get("actor_id") != event.actor_id
            and not addressed
        ):
            return ContextFocus("human_discussion")
        reference = row.get("work_reference")
        if isinstance(reference, Mapping) and reference.get("requester_id") == event.actor_id:
            identifiers = tuple(
                value for value in reference.get("work_ids", ()) if value in available
            )
            sources = (reference["source_event_id"],) if identifiers else ()
            kind = (
                "continuation" if query.continuation or "works" in query.resources else "discourse"
            )
            return ContextFocus(kind if identifiers else "unavailable", identifiers, sources)
        if (
            row.get("speaker", "human") == "human"
            and row.get("actor_id") == event.actor_id
            and (row.get("new_request") or row.get("focus_reset"))
        ):
            return ContextFocus("discussion")
    if query.states:
        return ContextFocus("query")
    if not query.continuation:
        return ContextFocus("discussion")
    own_sources = {
        row.get("id")
        for row in previous
        if row.get("actor_id") == event.actor_id and row.get("speaker", "human") == "human"
    }
    candidates = [
        row
        for row in works
        if row.get("requester_id") == event.actor_id
        and row.get("state") in _LIVE
        and row.get("request_event_id") in own_sources
    ]
    if len(candidates) == 1:
        return ContextFocus("continuation", (str(candidates[0]["id"]),))
    return ContextFocus("ambiguous" if candidates else "discussion")


@dataclass
class _Evidence:
    tx: UnitOfWork
    epoch: int
    through_sequence: int
    entries: dict[str, tuple[dict[str, str], dict[str, int], set[str]]] = field(
        default_factory=dict
    )
    incomplete: bool = False

    def add(
        self,
        key: str,
        sources: set[str],
        tasks: set[str] | None = None,
        *,
        retained: set[str] | None = None,
    ) -> bool:
        tasks, retained = tasks or set(), retained or set()
        sources = causal_sources(self.tx, sources)
        known_sources = {source for entry in self.entries.values() for source in entry[0]}
        known_tasks = {task for entry in self.entries.values() for task in entry[1]}
        if len(known_sources | sources) > 400 or len(known_tasks | tasks) > 100:
            self.incomplete = True
            return False
        if (
            sources
            and self.tx.conn.execute(
                "SELECT 1 FROM ga_events e WHERE e.group_id=%s AND e.id=ANY(%s) AND "
                + after_boundary_sql("e")
                + " LIMIT 1",
                (self.tx.group_id, sorted(sources), self.through_sequence, self.through_sequence),
            ).fetchone()
        ):
            return False
        try:
            if sources:
                source_rows(
                    self.tx,
                    sorted(sources),
                    shared=True,
                    allow_expired=True,
                    require_complete=key != "event",
                )
            expired = {
                source for source in sources if self.tx.get_event(source).content_state == "expired"
            }
            if not expired <= retained:
                return False
            fingerprints = {source: self.tx.event_fingerprint(source) for source in sources}
            revisions = {task: self.tx.get_team_task(task).revision for task in tasks}
        except DomainError as error:
            if error.code not in {
                "source_unavailable",
                "permission_denied",
                "scope_violation",
                "not_found",
            }:
                raise
            return False
        self.entries[key] = (fingerprints, revisions, expired)
        return True

    def snapshot(
        self,
        actor: ActorContext,
        topic_id: str | None,
        items: list[dict],
        keys: set[str],
        input_ids: list[str],
    ) -> dict[str, Any]:
        selected = [entry for key, entry in self.entries.items() if key in keys]
        return {
            "group_id": actor.group_id,
            "member_id": actor.member_id,
            "policy_revision": actor.policy_revision,
            "topic_id": topic_id,
            "privacy_epoch": self.epoch,
            "through_sequence": self.through_sequence,
            "items": items,
            "source_fingerprints": {
                source: digest for entry in selected for source, digest in entry[0].items()
            },
            "source_task_revisions": {
                task: revision for entry in selected for task, revision in entry[1].items()
            },
            "input_event_ids": input_ids,
            "summaries_are_lossy": True,
            "retained_context_source_ids": sorted(
                {source for entry in selected for source in entry[2]}
            ),
            "artifact_retained_source_ids": sorted(
                {
                    source
                    for key, entry in self.entries.items()
                    if key in keys and key.startswith("files:")
                    for source in entry[2]
                }
            ),
        }


class ContextBuilder:
    def __init__(
        self,
        config: AppConfig,
        store: PostgresStore,
        memory: MemoryService,
        members: MemberService,
        scheduler: Scheduler,
        artifacts: ArtifactRegistry | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config, self.store, self.memory, self.members, self.scheduler = (
            config,
            store,
            memory,
            members,
            scheduler,
        )
        self.artifacts, self.clock = artifacts, clock or (lambda: datetime.now(UTC))
        self.identity = BotIdentity.from_runtime(config.runtime)

    def _quote_message_id(self, tx: UnitOfWork, event: GroupEvent) -> str | None:
        if not event.reply_to:
            return None
        message_id = None
        try:
            message_id = tx.get_event(event.reply_to).platform_message_id
        except DomainError as error:
            if error.code != "not_found":
                raise
        if (
            message_id is None
            and event.kind == EventKind.QUOTE
            and tx.is_bot_message(event.reply_to)
        ):
            # A channel can supply the verified native receipt directly,
            # before its inbound echo is normalized into an event row.
            return event.reply_to
        if (
            message_id is None
            and event.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
            and event.channel_account_id == self.config.runtime.channel_account_id
        ):
            binding = AgentWechatConfig(
                base_url=self.config.runtime.channel_base_url,
                account_id=self.config.runtime.channel_account_id,
                external_group_id=self.config.group.external_group_id,
                internal_group_id=tx.group_id,
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
                    message_id = mid
                    break
        return message_id

    def _quoted_work(self, tx: UnitOfWork, event: GroupEvent) -> str | None:
        if not event.reply_to:
            return None
        direct = tx.conn.execute(
            "SELECT id FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
            (tx.group_id, event.reply_to),
        ).fetchone()
        if direct:
            return direct["id"]
        message_id = self._quote_message_id(tx, event)
        if not message_id:
            return None
        row = tx.conn.execute(
            "SELECT COALESCE(d.work_id,n.work_id,w.id) AS work_id FROM ga_deliveries d "
            "LEFT JOIN ga_delivery_parts p ON p.group_id=d.group_id AND p.intent_id=d.id "
            "LEFT JOIN ga_progress_notices n ON n.group_id=d.group_id AND n.intent_id=d.id "
            "LEFT JOIN ga_work_orders w ON w.group_id=d.group_id AND w.request_event_id=d.request_event_id "
            "WHERE d.group_id=%s AND ((d.state='accepted' AND d.platform_message_id=%s) "
            "OR (p.state='accepted' AND p.platform_message_id=%s)) LIMIT 1",
            (tx.group_id, message_id, message_id),
        ).fetchone()
        return row["work_id"] if row else None

    def _receipt_reference(
        self, tx: UnitOfWork, message_id: str, private_events: set[str], through_sequence: int
    ) -> dict[str, Any] | None:
        row = tx.conn.execute(
            "SELECT e.id,e.actor_id,c.body FROM ga_deliveries d "
            "LEFT JOIN ga_delivery_parts p ON p.group_id=d.group_id AND p.intent_id=d.id "
            "LEFT JOIN ga_progress_notices n ON n.group_id=d.group_id AND n.intent_id=d.id "
            "LEFT JOIN ga_work_orders w ON w.group_id=d.group_id AND w.id=COALESCE(d.work_id,n.work_id) "
            "JOIN ga_events e ON e.group_id=d.group_id AND e.id=COALESCE(d.request_event_id,w.request_event_id) "
            "JOIN ga_decisions c ON c.group_id=e.group_id AND c.event_id=e.id "
            "WHERE d.group_id=%s AND e.memory_state='active' AND NOT "
            + after_boundary_sql("e")
            + " "
            "AND ((d.state='accepted' AND d.platform_message_id=%s) "
            "OR (p.state='accepted' AND p.platform_message_id=%s)) LIMIT 1",
            (tx.group_id, through_sequence, through_sequence, message_id, message_id),
        ).fetchone()
        if row is None or row["id"] in private_events:
            return None
        return result_reference(row["body"], row["id"], row["actor_id"])

    def _quoted_reference(
        self, tx: UnitOfWork, event: GroupEvent, private_events: set[str], through_sequence: int
    ) -> dict[str, Any] | None:
        if not event.reply_to or event.reply_to in private_events:
            return None
        message_id = self._quote_message_id(tx, event)
        if message_id:
            reference = self._receipt_reference(tx, message_id, private_events, through_sequence)
            if reference:
                return reference
        row = tx.conn.execute(
            "SELECT e.actor_id,c.body FROM ga_events e JOIN ga_decisions c "
            "ON c.group_id=e.group_id AND c.event_id=e.id "
            "WHERE e.group_id=%s AND e.id=%s AND e.memory_state='active' AND NOT "
            + after_boundary_sql("e"),
            (tx.group_id, event.reply_to, through_sequence, through_sequence),
        ).fetchone()
        return result_reference(row["body"], event.reply_to, row["actor_id"]) if row else None

    def _index(
        self,
        tx: UnitOfWork,
        private_events: set[str],
        private_topics: set[str],
        through_sequence: int,
    ) -> tuple[list[dict], bool]:
        return visible_work_index_in_tx(
            tx,
            private_events=private_events,
            private_topics=private_topics,
            through_sequence=through_sequence,
        )

    def build(
        self, actor: ActorContext, event: GroupEvent, query: ContextQuery | None = None
    ) -> dict[str, Any]:
        query = query or ContextQuery.from_event(event, self.identity)
        group_zone = ZoneInfo(self.config.group.time_zone)

        def display_time(instant: datetime) -> str:
            return (instant.astimezone(group_zone) if query.model_led else instant).isoformat()

        # An early public check is followed by the locked check below. A topic
        # restriction introduced between them must never leak quoted text.
        for identifier in (event.id, event.reply_to):
            if identifier:
                try:
                    topics = self.memory.topics_for_event(actor, identifier)
                except DomainError as error:
                    if identifier == event.id or error.code not in {
                        "not_found",
                        "source_unavailable",
                    }:
                        raise
                else:
                    if any(topic.visibility == "participants" for topic in topics):
                        raise DomainError(
                            "permission_denied", "Restricted evidence cannot enter a group reply"
                        )

        with self.store.transaction(actor) as tx, ExitStack() as reads:
            self.memory.prepare_in_tx(tx)
            reads.enter_context(tx.context_event_reads())
            if tx.get_event(event.id) != event:
                raise DomainError(
                    "memory_changed",
                    "Canonical message changed before context construction",
                    retryable=True,
                )
            checkpoint = tx.conn.execute(
                "SELECT epoch FROM ga_memory_epochs WHERE group_id=%s", (tx.group_id,)
            ).fetchone()
            assert checkpoint is not None
            anchor = tx.conn.execute(
                "SELECT sequence,created_at FROM ga_events WHERE group_id=%s AND id=%s",
                (tx.group_id, event.id),
            ).fetchone()
            assert anchor is not None
            through_sequence = anchor["sequence"]
            evidence = _Evidence(tx, int(checkpoint["epoch"]), through_sequence)
            private_rows = tx.conn.execute(
                "SELECT t.id,r.event_id FROM ga_memory_topics t LEFT JOIN ga_topic_events r "
                "ON r.group_id=t.group_id AND r.topic_id=t.id WHERE t.group_id=%s AND t.visibility='participants'",
                (tx.group_id,),
            ).fetchall()
            private_topics = {row["id"] for row in private_rows}
            private_events = {row["event_id"] for row in private_rows if row["event_id"]}
            if event.id in private_events or event.reply_to in private_events:
                raise DomainError(
                    "permission_denied", "Restricted evidence cannot enter a group reply"
                )
            if not evidence.add("event", {event.id}):
                raise DomainError("source_unavailable", "Current source is unavailable")
            input_ids = [event.id]
            quoted_work_id = self._quoted_work(tx, event)
            index, index_complete = self._index(
                tx, private_events, private_topics, through_sequence
            )
            index_by_id = {row["id"]: row for row in index}
            work_overview = compact_work_overview(
                WorkFacts(self.store).overview_in_tx(
                    tx, active_limit=4, recent_limit=3, through_sequence=through_sequence
                )
            )
            if quoted_work_id not in index_by_id:
                quoted_work_id = None
            metadata_source_ids = WorkFacts(self.store).source_dependencies_in_tx(
                tx, tuple(row["id"] for row in work_overview["facts"])
            )
            topic_rows = tx.conn.execute(
                "SELECT t.id,t.title FROM ga_memory_topics t JOIN ga_topic_events r "
                "ON r.group_id=t.group_id AND r.topic_id=t.id WHERE r.group_id=%s AND r.event_id=ANY(%s) AND t.visibility='group'",
                (tx.group_id, [event.id, *([event.reply_to] if event.reply_to else [])]),
            ).fetchall()
            topic_id = topic_rows[0]["id"] if len({row["id"] for row in topic_rows}) == 1 else None
            if event.reply_to and event.reply_to not in private_events:
                if evidence.add("quote", {event.reply_to}):
                    input_ids.append(event.reply_to)
            recent_rows = visible_events(
                tx,
                topic_id=topic_id,
                limit=16,
                newest_first=True,
                public_only=True,
                through_sequence=through_sequence,
            )
            timeline: list[dict[str, Any]] = []
            associations = {
                row["event_id"]: row["body"]
                for row in tx.conn.execute(
                    "SELECT event_id,body FROM ga_decisions WHERE group_id=%s AND event_id=ANY(%s)",
                    (tx.group_id, [row["id"] for row in recent_rows]),
                ).fetchall()
            }
            for row in reversed(recent_rows):
                source = tx.get_event(row["id"])
                if source.id == event.id:
                    continue
                if (
                    event.occurred_at - source.occurred_at > timedelta(minutes=30)
                    and source.id != event.reply_to
                ):
                    continue
                speaker = (
                    "assistant"
                    if source.platform_message_id and tx.is_bot_message(source.platform_message_id)
                    else "human"
                )
                body = associations.get(source.id, {})
                reference = (
                    self._receipt_reference(
                        tx, source.platform_message_id, private_events, through_sequence
                    )
                    if speaker == "assistant" and source.platform_message_id
                    else result_reference(body, source.id, source.actor_id)
                )
                if reference:
                    reference["work_ids"] = [
                        value for value in reference["work_ids"] if value in index_by_id
                    ]
                timeline.append(
                    {
                        "id": source.id,
                        "actor_id": source.actor_id,
                        "kind": source.kind.value,
                        "content_origin": source.trusted_actor_origin,
                        "speaker": speaker,
                        "occurred_at": display_time(source.occurred_at),
                        "text": source.text[:2000],
                        "reply_to": source.reply_to,
                        "attachments": source.attachments,
                        "work_reference": reference,
                        "focus_reset": bool(body.get("privacy_redacted")),
                        "new_request": not query.model_led
                        and speaker == "human"
                        and ContextQuery.from_event(source, self.identity).new_request,
                    }
                )
            focus = select_focus(
                event,
                query,
                index,
                timeline,
                quoted_work_id=quoted_work_id,
                quoted_reference=self._quoted_reference(
                    tx, event, private_events, through_sequence
                ),
                identity=self.identity,
            )
            for source_id in focus.source_event_ids:
                if not evidence.add("focus:" + source_id, {source_id}):
                    focus = ContextFocus("unavailable")
                    break
            selected_work_ids: list[str] = []
            if focus.work_ids and (
                focus.kind == "quote" or query.continuation or "works" in query.resources
            ):
                selected_work_ids.extend(focus.work_ids[:8])
                evidence.incomplete |= len(focus.work_ids) > 8
            elif "works" in query.resources and not query.new_request:
                selected_work_ids = [
                    row["id"]
                    for row in index
                    if row["state"] in _LIVE and (not query.states or row["state"] in query.states)
                ][: 4 if query.preview else 8]
            works: list[dict] = []
            public_works: list[dict] = []
            for identifier in selected_work_ids:
                work = tx.get_work(identifier).model_dump(mode="json")
                sources = scope_sources(work["scope"]) | {work["request_event_id"]}
                logical_tasks = _task_ids(work["scope"])
                retained = set(work["scope"].get("retained_fact_source_ids", []))
                if not evidence.add(
                    "works:" + identifier, sources, logical_tasks, retained=retained
                ):
                    continue
                work["_requested_at"] = index_by_id[identifier]["_requested_at"]
                if work["pause_reason"] == "effect_unknown":
                    checkpoint = tx.conn.execute(
                        "SELECT body->'receipts' AS receipts FROM ga_runtime_checkpoints WHERE group_id=%s AND work_id=%s ORDER BY saved_at DESC LIMIT 1",
                        (tx.group_id, identifier),
                    ).fetchone()
                    failures = (
                        [r for r in checkpoint["receipts"] or [] if not r.get("success")]
                        if checkpoint
                        else []
                    )
                    if failures:
                        work["_runtime_failure"] = {
                            key: failures[-1].get(key) for key in ("tool_name", "error_code")
                        }
                works.append(work)
                detail = work_status_detail(work)
                projected = {
                    key: work[key]
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
                projected["goal"] = projected["goal"][:1500]
                projected["acceptance_criteria"] = [
                    str(value)[:300] for value in projected["acceptance_criteria"][:8]
                ]
                projected["scope"] = {
                    key: value
                    for key, value in work["scope"].items()
                    if key in {"topic_id", "outputs", "delivery_artifacts", "speech_requested"}
                }
                if work["state"] in {"paused", "waiting_input", "failed"}:
                    projected["status_detail"] = detail
                question = work["scope"].get("_runtime_input_request")
                if (
                    isinstance(question, dict)
                    and work["state"] == "waiting_input"
                    and question.get("goal_revision") == work["goal_revision"]
                ):
                    projected["waiting_question"] = question.get("question")
                public_works.append(projected)
            if focus.work_id and focus.work_id in {row["id"] for row in works} and topic_id is None:
                topic_id = index_by_id[focus.work_id].get("topic_id")
            tasks: list[dict[str, Any]] = []
            task_queries = []
            if "tasks" in query.resources:
                task_limit = 6 if query.preview else 20
                task_candidates, witness = self.memory.task_query_in_tx(
                    tx,
                    topic_id=topic_id,
                    limit=task_limit,
                    public_only=True,
                    through_sequence=through_sequence,
                )
                task_queries.append(witness)
                for task in task_candidates:
                    if evidence.add(
                        "tasks:" + task.id,
                        set(task.source_event_ids),
                        {task.id},
                        retained=set(task.source_event_ids),
                    ):
                        tasks.append(task.model_dump(mode="json"))
                    if len(tasks) >= task_limit:
                        evidence.incomplete = True
                        break
            rules: list[dict[str, Any]] = []
            if "schedules" in query.resources:
                schedule_limit = 6 if query.preview else 12
                for rule in self.scheduler.in_tx(tx).list(actor):
                    scope = rule.scope
                    if (
                        scope.get("privacy_redacted")
                        or scope.get("topic_id") in private_topics
                        or scope_sources(scope) & private_events
                    ):
                        continue
                    if evidence.add("schedules:" + rule.id, scope_sources(scope), _task_ids(scope)):
                        rules.append(rule.model_dump(mode="json"))
                    if len(rules) >= schedule_limit:
                        evidence.incomplete = True
                        break
            memory_items, original_items = [], []
            seen = set()
            memory_reads = (
                [(query.fact_predicate, True)] if query.fact_predicate is not None else []
            ) + [(term, False) for term in query.memory_terms]
            if query.preview:
                # Match names of actual records, not instruction words. This
                # brings a relevant durable scalar into chat in one model call.
                predicates = tx.conn.execute(
                    "SELECT DISTINCT f.predicate FROM ga_memory_facts f "
                    "LEFT JOIN ga_memory_topics t ON (t.group_id,t.id)=(f.group_id,f.topic_id) "
                    "WHERE f.group_id=%s AND f.state='active' "
                    "AND (t.id IS NULL OR t.visibility='group') "
                    "AND strpos(lower(%s),lower(f.predicate))>0 "
                    "ORDER BY f.predicate LIMIT 4",
                    (tx.group_id, event.text),
                ).fetchall()
                memory_reads.extend((row["predicate"], True) for row in predicates)
            for term, exact_predicate in memory_reads:
                for hit in self.memory.in_tx(tx).retrieve(
                    actor,
                    term,
                    topic_id=topic_id,
                    limit=8,
                    include_tasks=False,
                    through_sequence=through_sequence,
                    exact_predicate=exact_predicate,
                ):
                    if (hit.kind, hit.id) in seen or hit.topic_id in private_topics:
                        continue
                    seen.add((hit.kind, hit.id))
                    item = hit.model_dump(mode="json")
                    sources = {ref.event_id for ref in hit.source_refs}
                    if sources & private_events or not evidence.add(
                        "memory:" + hit.kind + ":" + hit.id,
                        sources,
                        set(hit.source_task_revisions),
                        retained=sources if hit.kind in {"fact", "decision"} else set(),
                    ):
                        continue
                    if hit.kind in {"fact", "decision"}:
                        fact = tx.conn.execute(
                            "SELECT revision,predicate,value,subject_id FROM ga_memory_facts "
                            "WHERE group_id=%s AND id=%s AND state='active'",
                            (tx.group_id, hit.id),
                        ).fetchone()
                        if fact:
                            item.update(fact)
                            if item["subject_id"] == actor.group_id:
                                item["subject_id"] = None
                    original_items.append(item)
                    memory_items.append(
                        {
                            **{
                                key: value
                                for key, value in item.items()
                                if key
                                in {
                                    "kind",
                                    "id",
                                    "author_id",
                                    "topic_id",
                                    "revision",
                                    "predicate",
                                    "value",
                                    "subject_id",
                                    "validity",
                                    "retrieval_reason",
                                    "incomplete",
                                }
                            },
                            "text": hit.text[:1800],
                            "context_excerpt_truncated": len(hit.text) > 1800,
                            "source_refs": item["source_refs"][:20],
                            "source_event_ids": [ref.event_id for ref in hit.source_refs[:20]],
                            "source_refs_truncated": len(hit.source_refs) > 20,
                        }
                    )
            member_rows = self.members.in_tx(tx).list(actor, include_left=True)
            recent = [] if query.new_request else timeline[-6:]
            if (
                event.reply_to
                and event.reply_to in input_ids
                and not any(row["id"] == event.reply_to for row in recent)
            ):
                source = tx.get_event(event.reply_to)
                recent.insert(
                    0,
                    {
                        "id": source.id,
                        "actor_id": source.actor_id,
                        "kind": source.kind.value,
                        "content_origin": source.trusted_actor_origin,
                        "speaker": "assistant"
                        if source.platform_message_id
                        and tx.is_bot_message(source.platform_message_id)
                        else "human",
                        "occurred_at": display_time(source.occurred_at),
                        "text": source.text[:2000],
                        "reply_to": source.reply_to,
                        "attachments": source.attachments,
                    },
                )
            recent = [
                row
                for row in recent
                if evidence.add(
                    "recent_messages:" + row["id"],
                    {row["id"]}
                    | (
                        {row["work_reference"]["source_event_id"]}
                        if row.get("work_reference")
                        else set()
                    ),
                )
            ]
            attachment_sources = []
            if "files" in query.resources:
                recent_ids = {row["id"] for row in recent}
                for row in timeline[-6:]:
                    if row["id"] in recent_ids or not (
                        row["kind"] in {"image", "audio", "file"}
                        or any(
                            part.get("kind") in {"image", "audio", "file"}
                            for part in row["attachments"]
                        )
                    ):
                        continue
                    if evidence.add("attachment_sources:" + row["id"], {row["id"]}):
                        attachment_sources.append(
                            {
                                key: row[key]
                                for key in (
                                    "id",
                                    "actor_id",
                                    "kind",
                                    "occurred_at",
                                    "text",
                                    "reply_to",
                                    "attachments",
                                )
                            }
                        )
            selected_topics = []
            for identifier in dict.fromkeys(
                [topic_id, *(row["scope"].get("topic_id") for row in works)]
            ):
                if not identifier:
                    continue
                topic = tx.conn.execute(
                    "SELECT t.id,t.title,t.visibility,o.source_event_ids FROM ga_memory_topics t LEFT JOIN ga_topic_origins o ON o.group_id=t.group_id AND o.topic_id=t.id WHERE t.group_id=%s AND t.id=%s AND t.visibility='group'",
                    (tx.group_id, identifier),
                ).fetchone()
                if topic and evidence.add(
                    "topics:" + identifier, set(topic["source_event_ids"] or [])
                ):
                    selected_topics.append(
                        {key: topic[key] for key in ("id", "title", "visibility")}
                    )
            files = []
            if self.artifacts and ("files" in query.resources or focus.work_ids and works):
                file_work_ids = [row["id"] for row in works] if focus.work_ids else []
                candidates = [
                    candidate
                    for identifier in (file_work_ids or [None])
                    for candidate in self.artifacts.list(
                        actor, limit=12, public_context=True, work_id=identifier
                    )
                ][:12]
                for candidate in candidates:
                    fingerprints, expired = self.artifacts.consume_in_tx(
                        tx, candidate["version_id"]
                    )
                    logical = self.artifacts.task_sources_in_tx(tx, candidate["version_id"])
                    if evidence.add(
                        "files:" + candidate["version_id"],
                        set(fingerprints),
                        set(logical),
                        retained=set(expired),
                    ):
                        files.append(candidate)
            policies = tx.conn.execute(
                "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s)",
                (tx.group_id, actor.member_id),
            ).fetchall()
            selected_actor_ids = {
                actor.member_id,
                *(row["actor_id"] for row in recent + attachment_sources),
            }

            # Historical authors need aliases even when their roster entry was
            # never observed. Source authorship must survive the public view.
            def collect_identities(value, name=None):
                if isinstance(value, str) and name in _IDENTITY_FIELDS:
                    selected_actor_ids.add(value)
                elif isinstance(value, list):
                    for item in value:
                        collect_identities(item, name)
                elif isinstance(value, dict):
                    for key, item in value.items():
                        collect_identities(item, key)

            collect_identities(
                [
                    public_works,
                    work_overview,
                    tasks,
                    rules,
                    memory_items,
                    recent,
                    attachment_sources,
                ]
            )
            text_for_names = event.text + "\n" + "\n".join(row["text"] for row in recent)
            authored = outside_quotes(event.text)
            authored_members = {
                member.member_id
                for member in member_rows
                if query.model_led and authored_identifier(authored, member.member_id)
            }
            visible_members = [
                member
                for member in member_rows
                if query.roster
                or member.member_id in selected_actor_ids
                or member.member_id in authored_members
                or member.display_name != "群昵称待补充"
                and member.display_name in text_for_names
            ]
            all_ids = sorted(
                {member.member_id for member in member_rows}
                | selected_actor_ids
                | {row["subject_id"] for row in memory_items if row.get("subject_id")}
            )
            aliases = {f"member_{i + 1}": member_id for i, member_id in enumerate(all_ids)}
            inverse = {member_id: alias for alias, member_id in aliases.items()}
            payload = {
                "now": self.clock().astimezone(group_zone).isoformat(),
                "group_time_zone": self.config.group.time_zone,
                "role": actor.role.value,
                "read_only": actor.role in {Role.OBSERVER, Role.MAINTAINER},
                "capabilities": {
                    **self.config.features.model_dump(mode="json"),
                    "public_image_retrieval": self.config.features.public_image_retrieval
                    and self.config.features.web_research,
                },
                "requester_member_id": actor.member_id,
                "members": [
                    {
                        "member_id": member.member_id,
                        "name": member.display_name,
                        "name_source": member.name_source,
                        "membership_state": member.membership_state,
                        "revision": member.revision,
                        **(
                            {"request_reference": member.member_id}
                            if member.member_id in authored_members
                            else {}
                        ),
                    }
                    for member in visible_members
                ],
                "works": public_works,
                "work_facts_overview": work_overview,
                "tasks": [
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
                ],
                "schedules": [
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
                ],
                "topics": selected_topics,
                "memory": memory_items,
                "summaries_are_lossy": True,
                "context_incomplete": evidence.incomplete,
                "conversation_focus": focus.kind,
                "focused_work_id": focus.work_id,
                "focused_work_ids": list(focus.work_ids),
                "quoted_work_id": quoted_work_id
                if quoted_work_id in {row["id"] for row in works}
                else None,
                "recent_messages": recent,
                "attachment_sources": attachment_sources,
                "files": files,
                "model_event": {
                    "id": event.id,
                    "actor_id": event.actor_id,
                    "kind": event.kind.value,
                    "content_origin": event.trusted_actor_origin,
                    "text": event.text,
                    "occurred_at": display_time(event.occurred_at),
                    "reply_to": event.reply_to,
                    "attachments": event.attachments,
                },
                **self.identity.context(),
            }
            if query.operations:
                payload["runtime_limits"] = self.config.execution.model_dump(mode="json")
            if query.model_led:
                available = "preview" if query.preview else "loaded"
                payload["context_coverage"] = {
                    "recent_messages": "window",
                    "works": "preview" if query.preview else "loaded" if works else "on_request",
                    "members": "loaded" if query.roster else "visible_participants",
                    "tasks": available if "tasks" in query.resources else "on_request",
                    "schedules": available if "schedules" in query.resources else "on_request",
                    "files": "loaded" if "files" in query.resources else "on_request",
                    "memory": available if memory_reads else "on_request",
                }
            for key in (
                "memory",
                "recent_messages",
                "attachment_sources",
                "files",
                "works",
                "schedules",
                "tasks",
            ):
                while (
                    len(json.dumps(payload, ensure_ascii=False, default=str).encode()) > 48_000
                    and payload[key]
                ):
                    payload[key].pop(0 if key == "recent_messages" else -1)
                    payload["context_incomplete"] = True
            keys = {
                "event",
                *(["quote"] if "quote" in evidence.entries else []),
                *("focus:" + source_id for source_id in focus.source_event_ids),
            }
            for name in (
                "works",
                "tasks",
                "schedules",
                "topics",
                "recent_messages",
                "attachment_sources",
                "files",
                "memory",
            ):
                for row in payload[name]:
                    identifier = row.get("version_id") if name == "files" else row["id"]
                    keys.add(
                        name + ":" + (row["kind"] + ":" if name == "memory" else "") + identifier
                    )
            snapshot_items = [
                row for row in original_items if "memory:" + row["kind"] + ":" + row["id"] in keys
            ]
            snapshot = evidence.snapshot(actor, topic_id, snapshot_items, keys, input_ids)
            snapshot["source_fact_revisions"] = {
                row["id"]: row["revision"]
                for row in snapshot_items
                if row["kind"] in {"fact", "decision"} and "revision" in row
            }
            snapshot["task_queries"] = task_queries
            snapshot["context_artifact_versions"] = [row["version_id"] for row in payload["files"]]
            kept_works = {row["id"] for row in payload["works"]}
            kept_tasks = {row["id"] for row in payload["tasks"]}
            kept_rules = {row["id"] for row in payload["schedules"]}

            def alias(value, field_name=None):
                if isinstance(value, str):
                    return inverse.get(value, value) if field_name in _IDENTITY_FIELDS else value
                if isinstance(value, list):
                    return [alias(item, field_name) for item in value]
                if isinstance(value, dict):
                    return {key: alias(item, key) for key, item in value.items()}
                return value

            payload = alias(payload)
            payload.update(
                {
                    "_snapshot": snapshot,
                    "_work_fact_source_ids": sorted(metadata_source_ids),
                    "_aliases": aliases,
                    "_policies": policies,
                    "_topic_id": topic_id,
                    "_works": [row for row in works if row["id"] in kept_works],
                    "_tasks": [row for row in tasks if row["id"] in kept_tasks],
                    "_rules": [row for row in rules if row["id"] in kept_rules],
                    "_control_works": index,
                    "_control_works_complete": index_complete,
                    "_focus_work_id": focus.work_id,
                    "_focus_work_ids": focus.work_ids,
                    "_quoted_work_id": quoted_work_id,
                    "_invocation": {
                        "addressed": self.identity.directly_addressed(outside_quotes(event.text)),
                        "quoted_bot": bool(
                            event.reply_to
                            and any(
                                row["id"] == event.reply_to and row.get("speaker") == "assistant"
                                for row in recent
                            )
                        ),
                        "continuing_work": focus.kind == "continuation",
                        "current_actor_id": actor.member_id,
                    },
                }
            )
            return payload
