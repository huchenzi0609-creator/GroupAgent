"""One durable application: intake, dialogue, execution, schedules and delivery.

LocalChannel is the default. Construction of a live channel additionally requires
an explicit live-send flag and the production configuration gates.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from groupagent.artifacts import ArtifactStore
from groupagent.billing import BudgetLedger
from groupagent.billing.metering import MeteredModel
from groupagent.billing.prices import deepseek_20261114
from groupagent.channels import AgentWechatAdapter, AgentWechatConfig, MemberRecord
from groupagent.channels.local import LocalChannel
from groupagent.config import AppConfig, ConfigError
from groupagent.conversation.intent import explicitly_requires_execution, new_work_intent
from groupagent.conversation.participation import outside_quotes
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.execution.deepseek import DeepSeekHTTPModel
from groupagent.identity import BotIdentity, control_request
from groupagent.media.isolation import IsolatedMediaWorker
from groupagent.media.models import Capability, ProviderRoute
from groupagent.media.providers import MediaHTTPProvider
from groupagent.memory import (
    ChannelRosterProvider,
    MemberService,
    MemoryService,
    ModelPortSummarizer,
)
from groupagent.ops import BackupManager, CleanupRunner, DeletionJournal
from groupagent.persistence import PostgresStore
from groupagent.research.deepseek import DeepSeekResearchClient
from groupagent.resources import ResourceDeferred, ResourceGovernor, ResourcePolicy
from groupagent.sandbox import DockerSandbox
from groupagent.scheduling import Scheduler
from groupagent.scheduling.triggers import EventTriggerService
from groupagent.security import load_secret_file
from groupagent.transport import ChannelIntake

from .artifacts import ArtifactRegistry
from .cleanup import RuntimeCleanupHandler
from .contracts import ProcessLease
from .dispatch import Dispatcher
from .executor import WorkSupervisor
from .inputs import InputProcessor
from .notices import NoticeService
from .resource_profile import worker_limits
from .roster import RosterRefresher
from .state import RuntimeState
from .toolkit import WorkToolkit


class RuntimeApp:
    def __init__(
        self,
        coordinator: ConversationCoordinator,
        channel: Any,
        supervisor: WorkSupervisor,
        dispatcher: Dispatcher,
        *,
        inputs: InputProcessor | None = None,
        cleanup: Any = None,
        resources: tuple[Any, ...] = (),
        worker_governor: ResourceGovernor | None = None,
        worker_startup: Callable[[], Awaitable[object]] | None = None,
    ) -> None:
        self.coordinator, self.channel = coordinator, channel
        self.config, self.store = coordinator.config, coordinator.store
        self.identity = BotIdentity.from_runtime(self.config.runtime)
        self.supervisor, self.dispatcher, self.inputs = supervisor, dispatcher, inputs
        self.state, self.cleanup, self.resources = RuntimeState(self.store), cleanup, resources
        self.worker_governor = worker_governor
        self.worker_startup = worker_startup
        self.intake = ChannelIntake(channel, coordinator)
        self.owner = self.resolve_actor(self.config.group.owner_ids[0])
        self.technical = ActorContext(
            group_id=self.owner.group_id,
            member_id="service:runtime",
            role=Role.MAINTAINER,
            policy_revision=self.owner.policy_revision,
        )
        self.triggers = EventTriggerService(
            self.store,
            self.resolve_actor,
            max_active_per_group=self.config.schedule.max_active_per_group,
            default_valid_days=self.config.schedule.default_valid_days,
        )
        if self.config.runtime.channel == "agentwechat":
            self.supervisor.work_ready = self._admission_delivered
        self.stop = asyncio.Event()
        self.instance_id = "runtime:" + uuid4().hex
        self._last_arrival = self._first_arrival = 0.0
        self._last_maintenance = 0.0
        self._processing: set[str] = set()
        self._tasks: list[asyncio.Task] = []
        self._application_lease: ProcessLease | None = None
        self.coordinator.execution_guard = self._fence_application
        self.roster = RosterRefresher(
            coordinator.members, channel, self.technical, self._fence_application
        )
        self.coordinator.refresh_roster = self.roster.query
        self.notices = NoticeService(
            self.store,
            self.technical,
            self.resolve_actor,
            interval_seconds=self.config.conversation.min_progress_interval_seconds,
        )

    def resolve_actor(self, member_id: str) -> ActorContext:
        group = self.config.group
        role = (
            Role.OWNER
            if member_id in group.owner_ids
            else (
                Role.MAINTAINER
                if member_id in group.maintainer_ids
                else Role.OBSERVER
                if member_id in group.observer_ids
                else Role.MEMBER
            )
        )
        actor = ActorContext(
            group_id=group.internal_id,
            member_id=member_id,
            role=role,
            policy_revision=group.policy_revision,
        )
        with self.store.transaction(actor):
            pass  # Revalidate membership and restore/policy fences on every use.
        return actor

    def _admission_delivered(self, work) -> bool:
        """Keep native attachment I/O behind the useful initial work response."""
        with self.store.transaction(self.technical) as tx:
            row = tx.conn.execute(
                "SELECT state FROM ga_deliveries WHERE group_id=%s AND request_event_id=%s "
                "AND idempotency_key=%s",
                (tx.group_id, work.request_event_id, "reply:" + work.request_event_id + ":1"),
            ).fetchone()
        return row is None or row["state"] == "accepted"

    async def start(self) -> None:
        self._fence_application()
        self._publish_foreground()
        await asyncio.to_thread(self._retire_pending_acknowledgements)
        if self.worker_startup is not None:
            try:
                await self.worker_startup()
            except DomainError as error:
                # A failed inventory keeps heavy admission closed; the group
                # can still chat and inspect/cancel its durable queue.
                self.state.alert(self.technical, error.code, resource_id="worker_startup")
        self._fence_application()
        await self.coordinator.members.sync_startup(
            self.technical, self.instance_id, ChannelRosterProvider(self.channel)
        )
        with self.store.transaction(self.technical) as tx:
            body = self.config.model_dump(mode="json")
            digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
            if self.config.runtime.channel == "agentwechat":
                prior = tx.conn.execute(
                    "SELECT policy_revision,sha256 FROM ga_runtime_configuration WHERE group_id=%s FOR UPDATE",
                    (tx.group_id,),
                ).fetchone()
                if (
                    prior
                    and prior["policy_revision"] == self.config.group.policy_revision
                    and prior["sha256"] != digest
                ):
                    raise DomainError(
                        "configuration_changed",
                        "A changed live configuration requires an explicit new policy revision",
                    )
                tx.conn.execute(
                    "INSERT INTO ga_runtime_configuration(group_id,policy_revision,sha256) VALUES(%s,%s,%s) ON CONFLICT(group_id) DO UPDATE SET policy_revision=EXCLUDED.policy_revision,sha256=EXCLUDED.sha256",
                    (tx.group_id, self.config.group.policy_revision, digest),
                )
            tx._audit(
                "runtime_started",
                self.instance_id,
                config_sha256=digest,
                channel=self.config.runtime.channel,
            )

    async def poll_intake(self) -> int:
        self._fence_application()
        batch = await self.intake.poll_once()
        self._fence_application()
        self.roster.changed(batch.roster_updates)
        if batch.event_ids:
            now = time.monotonic()
            if not self._first_arrival:
                self._first_arrival = now
            self._last_arrival = now
        return len(batch.event_ids)

    def _publish_foreground(self) -> None:
        publish = getattr(self.worker_governor, "foreground_pending", None)
        if callable(publish):
            publish(len(self._processing))

    def _retire_pending_acknowledgements(self) -> int:
        """Cancel only old source-bound placeholders with no submitted part."""
        with self.store.transaction(self.technical) as tx:
            rows = tx.conn.execute(
                "SELECT id,created_by FROM ga_deliveries WHERE group_id=%s AND state='pending' "
                "AND request_event_id IS NOT NULL AND work_id IS NULL "
                "AND left(idempotency_key,10)='early_ack:' ORDER BY created_at,id LIMIT 100",
                (tx.group_id,),
            ).fetchall()
        count = 0
        for row in rows:
            try:
                actor = self.resolve_actor(row["created_by"])
                with self.store.transaction(actor) as tx:
                    self._fence_application(tx)
                    intent = tx.get_delivery(row["id"])
                    if intent.state.value != "pending" or not intent.idempotency_key.startswith(
                        "early_ack:"
                    ):
                        continue
                    if tx.conn.execute(
                        "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s "
                        "AND state<>'pending' LIMIT 1",
                        (tx.group_id, intent.id),
                    ).fetchone():
                        continue
                    tx.cancel_pending_delivery(intent.id, expected_revision=intent.revision)
                    count += 1
            except DomainError as error:
                if error.code in {"stale_lease", "policy_changed", "restore_pending"}:
                    raise
        return count

    async def early_ack_tick(self) -> int:
        """Compatibility hook: ordinary chat never emits placeholder replies."""
        self._publish_foreground()
        return 0

    def _ready_events(self) -> list[GroupEvent]:
        with self.store.transaction(self.technical) as tx:
            rows = tx.conn.execute(
                "SELECT e.id FROM ga_events e LEFT JOIN ga_event_retry r ON r.group_id=e.group_id "
                "AND r.event_id=e.id WHERE e.group_id=%s AND e.memory_state='active' "
                "AND NOT EXISTS(SELECT 1 FROM ga_decisions d WHERE d.group_id=e.group_id AND d.event_id=e.id) "
                "AND (r.event_id IS NULL OR (NOT r.blocked AND r.next_attempt_at<=clock_timestamp())) "
                "ORDER BY e.sequence LIMIT 100",
                (tx.group_id,),
            ).fetchall()
            return [tx.get_event(row["id"]) for row in rows if row["id"] not in self._processing]

    def _audio_echo(self, event: GroupEvent) -> bool:
        with self.store.transaction(self.technical) as tx:
            if event.platform_message_id and (
                tx.is_bot_message(event.platform_message_id)
                or tx.conn.execute(
                    "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND "
                    "platform_message_id=%s AND state='accepted' LIMIT 1",
                    (tx.group_id, event.platform_message_id),
                ).fetchone()
            ):
                return True
            if event.actor_id == self.config.group.bot_member_id:
                if not self.config.group.shared_account:
                    return True
                if tx.conn.execute(
                    "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND "
                    "state IN ('submitted','unknown') LIMIT 1",
                    (tx.group_id,),
                ).fetchone():
                    raise DomainError(
                        "echo_reconciliation_pending",
                        "Awaiting authenticated send receipt",
                        retryable=True,
                    )
        return False

    async def _process(self, event: GroupEvent) -> None:
        self._fence_application()
        source_id = event.id
        self._processing.add(source_id)
        self._publish_foreground()
        try:
            if (
                event.kind == EventKind.AUDIO
                and self.config.features.audio_transcription
                and self.inputs
                and not self._audio_echo(event)
            ):
                if self.worker_governor and not self.worker_governor.ready(
                    self.config.resources.worker_memory_mb
                ):
                    raise ResourceDeferred()
                event = await self.inputs.preprocess_audio(event)
            await self.coordinator.process(event)
            with self.store.transaction(self.technical) as tx:
                tx.conn.execute(
                    "DELETE FROM ga_event_retry WHERE group_id=%s AND event_id=%s",
                    (tx.group_id, event.id),
                )
        except DomainError as error:
            if error.code in {"stale_lease", "policy_changed", "restore_pending"}:
                self.stop.set()
                raise
            temporary = error.retryable or error.code in {
                "memory_changed",
                "attachment_pending",
                "echo_reconciliation_pending",
            }
            with self.store.transaction(self.technical) as tx:
                row = tx.conn.execute(
                    "SELECT attempts FROM ga_event_retry WHERE group_id=%s AND event_id=%s FOR UPDATE",
                    (tx.group_id, event.id),
                ).fetchone()
                waiting_resource = error.code == "resource_deferred"
                attempts = (row["attempts"] if row else 0) + (0 if waiting_resource else 1)
                blocked = not temporary or attempts >= 3
                if waiting_resource:
                    blocked = False
                # Waiting for an external attachment/receipt is not a paid-model retry.
                if error.code in {"attachment_pending", "echo_reconciliation_pending"}:
                    blocked = attempts >= 60
                tx.conn.execute(
                    "INSERT INTO ga_event_retry(group_id,event_id,attempts,next_attempt_at,blocked,code) "
                    "VALUES(%s,%s,%s,clock_timestamp()+%s*interval '1 second',%s,%s) ON CONFLICT(group_id,event_id) "
                    "DO UPDATE SET attempts=EXCLUDED.attempts,next_attempt_at=EXCLUDED.next_attempt_at,blocked=EXCLUDED.blocked,code=EXCLUDED.code",
                    (
                        tx.group_id,
                        event.id,
                        attempts,
                        self.config.resources.retry_seconds
                        if waiting_resource
                        else min(30, 2 ** min(attempts, 5)),
                        blocked,
                        error.code,
                    ),
                )
            if error.code != "resource_deferred":
                self.state.alert(self.technical, error.code, resource_id=event.id)
            if blocked:
                self._failure_reply(event, error.code)
        finally:
            self._processing.discard(source_id)
            self._publish_foreground()

    def _failure_reply(self, event: GroupEvent, code: str) -> None:
        # A missing capability or exhausted budget is not a successful response.
        if event.kind not in {
            EventKind.TEXT,
            EventKind.QUOTE,
            EventKind.AUDIO,
            EventKind.IMAGE,
            EventKind.FILE,
        }:
            return
        if event.kind in {EventKind.IMAGE, EventKind.FILE} and not re.search(
            r"请|帮|看看|分析|总结|识别|转写|整理|提取", event.text
        ):
            actor = self.resolve_actor(event.actor_id)
            with self.store.transaction(actor) as tx:
                if tx.get_decision(event.id) is None:
                    result = CoordinationResult(
                        event_id=event.id, decision=Decision(kind="observe", reason_code=code)
                    )
                    tx.record_decision(
                        event.id, {"kind": "observe", "result": result.model_dump(mode="json")}
                    )
            return
        text = "这次处理没有完成，原有记录已保留。"
        if code in {"budget_exhausted", "budget_not_configured"}:
            text = "当前调用额度不足，我先停止付费处理，已有任务和记录会保留。"
        elif code in {"attachment_pending", "attachment_expired", "attachment_unavailable"}:
            text = "附件暂时不可用，请重新发送需要处理的资料。"
        elif code == "provider_output_invalid":
            text = "这次我没有可靠理解你的意思，请补充一下要处理的对象。"
        try:
            actor = self.resolve_actor(event.actor_id)
            with self.store.transaction(actor) as tx:
                if tx.get_decision(event.id) or tx.get_event(event.id).content_state != "active":
                    return
                delivery = tx.enqueue_reply(
                    event.id,
                    text,
                    read_only_authorized=actor.role.value in self.config.group.read_only_qa_roles,
                )
                result = CoordinationResult(
                    event_id=event.id,
                    decision=Decision(kind="reply", reason_code=code, response=text),
                    delivery_id=delivery.id,
                )
                tx.record_decision(
                    event.id, {"kind": "reply", "result": result.model_dump(mode="json")}
                )
        except DomainError:
            pass

    def _bypass_batch(self, event: GroupEvent) -> bool:
        if event.kind != EventKind.TEXT or event.reply_to or event.attachments:
            return False
        if new_work_intent(event.text, identity=self.identity) is not None:
            return True  # Preserve the existing independent-work admission path.
        plain = outside_quotes(event.text)
        if not plain or plain != event.text.strip() or len(plain) > 1200:
            return False
        request = self.identity.strip_address(plain)
        if explicitly_requires_execution(plain, identity=self.identity):
            return False
        if request == plain and not re.search(r"[?？]", request):
            return False
        if re.search(
            r"文件|文档|附件|图片|截图|音频|语音|视频|生成|制作|导出|交付|下载|上传|删除|修改|"
            r"分配|安排|提醒|忽略|绕过|越权|密钥|密码|"
            r"(?<![A-Za-z0-9_])(?:PDF|DOCX|PPTX?|Markdown|WAV|MP3|PNG|JPEG|Shell|secret|password|prompt)(?![A-Za-z0-9_])",
            request,
            re.I,
        ):
            return False
        return (
            re.search(
                r"[?？]|什么|啥|怎么|为何|为什么|解释|说明|讲讲|告诉我|叫什么|是谁|多少|区别|吗|"
                r"\b(?:what|how|why|who|explain|describe)\b",
                request,
                re.I,
            )
            is not None
        )

    async def process_pending(self, *, controls_only: bool = False, batched: bool = True) -> int:
        events = self._ready_events()
        controls = [
            event
            for event in events
            if event.kind in {EventKind.TEXT, EventKind.QUOTE}
            and control_request(event.text, identity=self.identity) is not None
        ]
        if controls:
            await self._process(controls[0])
            return 1
        if events:
            # Receipt-proven echoes are bookkeeping, not conversation. Drain
            # them without the idle window or a full history reconstruction.
            with self.store.transaction(self.technical) as tx:
                echo = tx.conn.execute(
                    "SELECT e.id FROM ga_events e WHERE e.group_id=%s AND e.id=ANY(%s) AND e.kind<>'recall' "
                    "AND (EXISTS(SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=e.group_id "
                    "AND p.platform_message_id=e.body->>'platform_message_id' AND p.state='accepted') "
                    "OR EXISTS(SELECT 1 FROM ga_deliveries d WHERE d.group_id=e.group_id "
                    "AND d.platform_message_id=e.body->>'platform_message_id' AND d.state='accepted')) "
                    "ORDER BY e.sequence LIMIT 1",
                    (self.technical.group_id, [item.id for item in events]),
                ).fetchone()
            if echo:
                await self._process(next(item for item in events if item.id == echo["id"]))
                return 1
        identities = [
            event
            for event in events
            if self._bypass_batch(event)
            and self.coordinator._fast_identity_reply(event, self.identity.context()) is not None
        ]
        if identities:
            # A verified name response performs no model/tool call, so it can
            # use the same responsive lane as controls during a slow dialogue.
            await self._process(identities[0])
            return 1
        if controls_only or not events:
            return 0
        now = time.monotonic()
        if (
            batched
            and not self._bypass_batch(events[0])
            and self._first_arrival
            and (
                now - self._last_arrival < self.config.conversation.idle_batch_seconds
                and now - self._first_arrival < self.config.conversation.max_batch_wait_seconds
            )
        ):
            return 0
        await self._process(events[0])
        if len(events) == 1:
            self._first_arrival = 0
        return 1

    async def execute_tick(self) -> None:
        self._fence_application()
        active = self.coordinator.current_mode() not in {"paused", "disabled"}
        if active and self.config.features.durable_schedules:
            self.coordinator.scheduler.due(self.owner)
            self.triggers.poll(self.technical)
        if active:
            await self.supervisor.tick()
        self.notices.tick()

    async def maintenance(self, *, force: bool = False) -> None:
        self._fence_application()
        interval = self.config.resources.maintenance_interval_seconds
        if not force and time.monotonic() - self._last_maintenance < interval:
            return
        self._last_maintenance = time.monotonic()
        if self.config.resources.enabled and (
            self._processing
            or self.supervisor.active_work_id is not None
            or self.worker_governor is None
            or not self.worker_governor.ready(self.config.resources.worker_memory_mb)
        ):
            # No additional model/cleanup load while a foreground or isolated
            # job is active. Expiry is also enforced on reads until the next pass.
            return
        try:
            if self.coordinator.current_mode() not in {"paused", "disabled"}:
                await self.coordinator.memory.compact(self.owner)
        except DomainError as error:
            if error.code in {"stale_lease", "policy_changed", "restore_pending"}:
                raise
            self.state.alert(self.technical, error.code, resource_id="memory_compaction")
        self.coordinator.memory.expire(
            self.technical,
            before=datetime.now(UTC) - timedelta(days=self.config.retention.raw_message_days),
        )
        if self.cleanup:
            cleaned = await self.cleanup.run(self.technical)
            for code in set(cleaned.errors.values()):
                self.state.alert(self.technical, code, resource_id="data_cleanup")

    async def step(self) -> dict[str, int]:
        received = await self.poll_intake()
        processed = await self.process_pending(batched=False)
        await self.execute_tick()
        sent = await self.dispatcher.tick()
        return {"received": received, "processed": processed, "sent": sent}

    async def run(self) -> None:
        lease = self.state.claim_process(
            self.technical, "application", self.instance_id, ttl_seconds=30
        )
        if lease is None:
            raise DomainError("runtime_busy", "Another application owns this group")
        self._application_lease = lease

        async def loop(operation, delay):
            failures = 0
            while not self.stop.is_set():
                next_delay = delay
                try:
                    self._fence_application()
                    await operation()
                    failures = 0
                except DomainError as error:
                    if error.code in {"stale_lease", "policy_changed", "restore_pending"}:
                        self.stop.set()
                        raise
                    self.state.alert(self.technical, error.code)
                    failures += 1
                    next_delay = max(
                        delay,
                        30
                        if error.code in {"auth_required", "invalid_credentials"}
                        else min(30, 2 ** min(failures, 5)),
                    )
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=next_delay)
                except TimeoutError:
                    pass

        async def intake_and_controls():
            await self.poll_intake()
            await self.process_pending(controls_only=True)

        async def heartbeat():
            nonlocal lease
            assert lease is not None
            lease = self.state.heartbeat_process(self.technical, lease, ttl_seconds=30)
            self._application_lease = lease

        try:
            self._tasks = [asyncio.create_task(loop(heartbeat, 8))]
            await self.start()
            self._tasks.extend(
                asyncio.create_task(loop(fn, seconds))
                for fn, seconds in (
                    (intake_and_controls, self.config.runtime.poll_seconds),
                    (self.process_pending, self.config.runtime.poll_seconds),
                    (self.roster.sync_pending, self.config.runtime.poll_seconds),
                    (self.execute_tick, self.config.runtime.poll_seconds),
                    (self.dispatcher.tick, self.config.runtime.poll_seconds),
                    (self.maintenance, self.config.resources.maintenance_interval_seconds),
                )
            )
            done, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            await self.close()
            self.state.release_process(self.technical, lease)

    def _fence_application(self, tx=None) -> None:
        lease = self._application_lease
        if lease is None:
            return
        if tx is None:
            self.state.assert_process(self.technical, lease)
        else:
            row = tx.conn.execute(
                "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND name=%s AND owner_id=%s AND fencing_token=%s AND expires_at>clock_timestamp() FOR SHARE",
                (self.technical.group_id, lease.name, lease.owner_id, lease.fencing_token),
            ).fetchone()
            if row is None:
                raise DomainError("stale_lease", "Application no longer owns this group")

    async def close(self) -> None:
        self.stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.supervisor.close()
        self._publish_foreground()
        governor_close = getattr(self.worker_governor, "aclose", None)
        if callable(governor_close):
            await governor_close()
        for resource in self.resources:
            await resource.aclose()


def channel_config(config: AppConfig) -> AgentWechatConfig:
    binding = AgentWechatConfig(
        base_url=config.runtime.channel_base_url,
        account_id=config.runtime.channel_account_id,
        external_group_id=config.group.external_group_id,
        internal_group_id=config.group.internal_id,
        source_epoch=config.runtime.source_epoch,
        policy_revision=config.group.policy_revision,
    )
    if config.resources.enabled:
        return binding.model_copy(
            update={
                "max_attachment_bytes": min(
                    binding.max_attachment_bytes,
                    config.limits.total_input_bytes_per_work,
                    max(
                        config.limits.image_bytes,
                        config.limits.markdown_bytes,
                        config.limits.office_or_pdf_bytes,
                        config.limits.audio_file_bytes,
                    ),
                )
            }
        )
    return binding


def build_app(
    config: AppConfig, store: PostgresStore, root: Path, secrets_file: Path
) -> RuntimeApp:
    if not config.group.enabled or not config.runtime.worker_image:
        raise ConfigError("Enable the intended group and configure an immutable worker image")
    if config.runtime.channel == "agentwechat":
        config.require_production_ready()
        if not config.runtime.allow_live_send or not config.group.bot_member_id:
            raise ConfigError(
                "Live channel needs explicit send authorization and authenticated bot member identity"
            )
    if config.spend.group_daily_limit is None or not config.spend.pricing_snapshot_id:
        raise ConfigError("A monetary limit and verified price snapshot are required")
    secrets = load_secret_file(
        secrets_file, frozenset({"DEEPSEEK_API_KEY", "GROUPAGENT_CHANNEL_TOKEN"})
    )
    if "DEEPSEEK_API_KEY" not in secrets:
        raise ConfigError("DEEPSEEK_API_KEY is required in the private secrets file")
    root = root.absolute()
    if root != root.resolve():
        raise ConfigError("Runtime root must be canonical and cannot traverse symlinks")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    store.migrate()
    store.register_group(config.group.internal_id, config.group.policy_revision)
    owner = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=config.group.policy_revision,
    )
    ledger = BudgetLedger(store)
    with store.transaction(owner) as tx:
        policy = tx.conn.execute(
            "SELECT currency,daily_limit,total_limit FROM ga_budget_policy WHERE group_id=%s",
            (tx.group_id,),
        ).fetchone()
    expected = (
        config.spend.currency,
        config.spend.group_daily_limit,
        config.spend.session_total_limit,
    )
    if policy is None:
        ledger.configure(
            owner,
            currency=config.spend.currency,
            daily_limit=config.spend.group_daily_limit,
            total_limit=config.spend.session_total_limit,
            time_zone=config.group.time_zone,
        )
    elif (policy["currency"], policy["daily_limit"], policy["total_limit"]) != expected:
        raise ConfigError(
            "Existing budget differs; restart cannot change or reset an authorized budget"
        )
    for price in deepseek_20261114():
        ledger.register_price(owner, price)
    key = secrets["DEEPSEEK_API_KEY"]
    model = DeepSeekHTTPModel(key, model=config.model_routes.text_agent)
    provider = MediaHTTPProvider("deepseek", key.get_secret_value())
    research = DeepSeekResearchClient(
        key,
        max_search_content_bytes=8_000,
        max_search_text_bytes=16_000,
        max_fetch_content_bytes=12_000,
    )
    route_models = [
        ("vision", Capability.VISION, config.model_routes.vision, "vision"),
        ("asr", Capability.ASR, config.model_routes.speech_to_text, "asr"),
        ("tts", Capability.TTS, config.model_routes.text_to_speech, "tts"),
        ("ocr", Capability.OCR, "tools/ocr", "ocr"),
    ]
    enabled_routes = {
        "vision": config.features.image_understanding,
        "ocr": config.features.image_understanding,
        "asr": config.features.audio_transcription,
        "tts": config.features.speech_reply_on_request,
    }
    route_models = [row for row in route_models if enabled_routes[row[0]]]
    routes = {
        name: ProviderRoute(
            route_id=name,
            provider="deepseek",
            capability=capability,
            model=route_model,
            price_id="deepseek-20261114-media." + suffix,
        )
        for name, capability, route_model, suffix in route_models
    }
    binding = channel_config(config)
    channel: Any
    if config.runtime.channel == "local":
        channel = LocalChannel(
            root / "channel",
            binding,
            members=tuple(
                MemberRecord(
                    group_id=config.group.internal_id,
                    member_id=identifier,
                    room_display_name=name,
                    membership_state="present",
                    name_source="roomDisplayName",
                )
                for identifier, name in config.runtime.local_member_names.items()
            ),
        )
    else:
        token = secrets.get("GROUPAGENT_CHANNEL_TOKEN")
        if token is None:
            raise ConfigError("GROUPAGENT_CHANNEL_TOKEN is required for the live adapter")
        channel = AgentWechatAdapter(binding, token.get_secret_value())
    backups = None
    try:
        backups = BackupManager(
            store,
            root / "backups",
            Path(config.runtime.pg_bin_dir) if config.runtime.pg_bin_dir else None,
            retention_days=config.retention.backup_days,
        )
    except DomainError as error:
        if error.code != "dependency_unavailable":
            raise
    journal = backups.journal if backups else DeletionJournal(root / "backups" / "deletions")
    memory = MemoryService(
        store,
        ModelPortSummarizer(
            MeteredModel(
                model,
                ledger,
                owner,
                price_id=config.spend.pricing_snapshot_id,
                additional_retry_limit=config.execution.additional_retry_limit,
            )
        ),
        deletion_sink=journal,
    )
    members = MemberService(store)
    artifacts = ArtifactStore(root / "artifacts")
    registry = ArtifactRegistry(
        store, artifacts, retention_days=config.retention.generated_artifact_days
    )
    scheduler = Scheduler(
        store,
        max_active_per_group=config.schedule.max_active_per_group,
        min_interval_seconds=config.schedule.min_interval_seconds,
        default_valid_days=config.schedule.default_valid_days,
        one_shot_grace_seconds=config.schedule.missed_one_shot_grace_seconds,
    )

    def decision_provider(actor, event):
        return ModelDecisionProvider(
            MeteredModel(
                model,
                ledger,
                actor,
                price_id=config.spend.pricing_snapshot_id or "deepseek-20261114-text",
                source_event_id=event.id,
                read_only_authorized=actor.role.value in config.group.read_only_qa_roles,
                additional_retry_limit=config.execution.additional_retry_limit,
            )
        )

    coordinator = ConversationCoordinator(
        config,
        store,
        decision_provider,
        memory=memory,
        members=members,
        scheduler=scheduler,
        artifacts=registry,
    )

    def resolve(member_id):
        return app.resolve_actor(member_id)

    worker_governor = ResourceGovernor(
        ResourcePolicy(
            enabled=config.resources.enabled,
            reserve_memory_mb=config.resources.reserve_memory_mb,
            transfer_memory_mb=config.resources.transfer_memory_mb,
        ),
        foreground_socket=(
            Path(config.runtime.broker_control_socket)
            if config.runtime.broker_control_socket
            else None
        ),
    )
    input_sandbox = DockerSandbox(
        config.runtime.worker_image,
        docker_host=config.runtime.docker_host,
        limits=worker_limits(config),
        resource_governor=worker_governor,
    )
    worker = IsolatedMediaWorker(input_sandbox)
    inputs = InputProcessor(
        store,
        artifacts,
        worker,
        {"deepseek": provider},
        routes,
        ledger,
        channel,
        root / "inputs",
        resolve,
        max_total_input_bytes=config.limits.total_input_bytes_per_work,
        max_images=config.limits.images_per_work,
        max_input_files=config.limits.input_files_per_work,
        max_image_bytes=config.limits.image_bytes,
        max_image_pixels=config.limits.image_pixels,
        max_document_bytes=config.limits.office_or_pdf_bytes,
    )
    toolkit = WorkToolkit(
        config,
        store,
        artifacts,
        registry,
        memory,
        model,
        ledger,
        media_providers={"deepseek": provider},
        routes=routes,
        research=research,
        inputs=inputs,
        scheduler=scheduler,
        resource_governor=worker_governor,
        triggers=EventTriggerService(
            store,
            resolve,
            max_active_per_group=config.schedule.max_active_per_group,
            default_valid_days=config.schedule.default_valid_days,
        ),
    )
    technical = ActorContext(
        group_id=owner.group_id,
        member_id="service:runtime",
        role=Role.MAINTAINER,
        policy_revision=owner.policy_revision,
    )
    state = RuntimeState(store)
    supervisor = WorkSupervisor(
        store,
        state,
        technical,
        resolve,
        toolkit.prepare,
        toolkit,
        root / "workspaces",
        lease_seconds=config.execution.lease_seconds,
        heartbeat_seconds=config.execution.heartbeat_seconds,
        cancel_grace_seconds=config.execution.cancel_grace_seconds,
        resource_ready=(lambda: worker_governor.ready(config.resources.worker_memory_mb))
        if config.resources.enabled
        else None,
        resource_retry_seconds=config.resources.retry_seconds,
        execution_backend=config.runtime.engine,
        release_attempt=toolkit.release_attempt,
    )
    dispatcher = Dispatcher(
        state,
        technical,
        resolve,
        channel,
        registry,
        badge=config.runtime.bot_badge,
        chunk_characters=config.runtime.message_chunk_characters,
        time_zone=config.group.time_zone,
        quiet_hours=config.group.quiet_hours,
    )
    app = RuntimeApp(
        coordinator,
        channel,
        supervisor,
        dispatcher,
        inputs=inputs,
        resources=(channel, model, provider, research),
        worker_governor=worker_governor,
        worker_startup=(lambda: input_sandbox.reconcile_group(config.group.internal_id))
        if config.resources.enabled
        else None,
    )

    def busy(work_ids, event_ids):
        return bool(
            set(event_ids) & app._processing
            or supervisor.active_work_id in work_ids
            or set(work_ids) & set(supervisor.delivering_work_ids)
        )

    async def quiesce(work_ids, event_ids):
        results = await asyncio.gather(
            supervisor.quiesce(work_ids), inputs.quiesce(event_ids), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    cleanup_handler = RuntimeCleanupHandler(
        store,
        artifacts,
        root,
        technical=technical,
        channel=channel,
        is_busy=busy,
        deletion_sink=journal,
        quiesce=quiesce,
        audit_days=config.retention.audit_days,
    )
    app.cleanup = CleanupRunner(memory, cleanup_handler, journal=journal, backups=backups)
    return app
