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
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.execution.deepseek import DeepSeekHTTPModel
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
from groupagent.sandbox import DockerSandbox, SandboxLimits
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
from .state import RuntimeState
from .toolkit import WorkToolkit

CONTROL = re.compile(
    r"^(?:(?:/组长|@?机器人组长|@?组长)[，,：:\s]*)?(?:停止|取消|暂停|先停一下|停一下|别做了)(?:这个任务|这项任务|刚才的任务|这个工作|任务)?[。！!\s]*$"
)


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
    ) -> None:
        self.coordinator, self.channel = coordinator, channel
        self.config, self.store = coordinator.config, coordinator.store
        self.supervisor, self.dispatcher, self.inputs = supervisor, dispatcher, inputs
        self.state, self.cleanup, self.resources = RuntimeState(self.store), cleanup, resources
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
        self.stop = asyncio.Event()
        self.instance_id = "runtime:" + uuid4().hex
        self._last_arrival = self._first_arrival = 0.0
        self._last_maintenance = 0.0
        self._processing: set[str] = set()
        self._tasks: list[asyncio.Task] = []
        self._application_lease: ProcessLease | None = None
        self.coordinator.execution_guard = self._fence_application
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

    async def start(self) -> None:
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
        if batch.event_ids:
            now = time.monotonic()
            if not self._first_arrival:
                self._first_arrival = now
            self._last_arrival = now
        return len(batch.event_ids)

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
        try:
            if (
                event.kind == EventKind.AUDIO
                and self.config.features.audio_transcription
                and self.inputs
                and not self._audio_echo(event)
            ):
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
                attempts = (row["attempts"] if row else 0) + 1
                blocked = not temporary or attempts >= 3
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
                        min(30, 2 ** min(attempts, 5)),
                        blocked,
                        error.code,
                    ),
                )
            self.state.alert(self.technical, error.code, resource_id=event.id)
            if blocked:
                self._failure_reply(event, error.code)
        finally:
            self._processing.discard(source_id)

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

    async def process_pending(self, *, controls_only: bool = False, batched: bool = True) -> int:
        events = self._ready_events()
        controls = [
            event
            for event in events
            if event.kind in {EventKind.TEXT, EventKind.QUOTE}
            and CONTROL.fullmatch(event.text.strip())
        ]
        if controls:
            await self._process(controls[0])
            return 1
        if controls_only or not events:
            return 0
        now = time.monotonic()
        if (
            batched
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
        if not force and time.monotonic() - self._last_maintenance < 60:
            return
        self._last_maintenance = time.monotonic()
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
            await self.start()
            self._tasks = [
                asyncio.create_task(loop(fn, seconds))
                for fn, seconds in (
                    (intake_and_controls, self.config.runtime.poll_seconds),
                    (self.process_pending, self.config.runtime.poll_seconds),
                    (self.execute_tick, self.config.runtime.poll_seconds),
                    (self.dispatcher.tick, self.config.runtime.poll_seconds),
                    (self.maintenance, 60),
                    (heartbeat, 8),
                )
            ]
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
        for resource in self.resources:
            await resource.aclose()


def channel_config(config: AppConfig) -> AgentWechatConfig:
    return AgentWechatConfig(
        base_url=config.runtime.channel_base_url,
        account_id=config.runtime.channel_account_id,
        external_group_id=config.group.external_group_id,
        internal_group_id=config.group.internal_id,
        source_epoch=config.runtime.source_epoch,
        policy_revision=config.group.policy_revision,
    )


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
            MeteredModel(model, ledger, owner, price_id=config.spend.pricing_snapshot_id)
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

    worker = IsolatedMediaWorker(
        DockerSandbox(
            config.runtime.worker_image,
            docker_host=config.runtime.docker_host,
            limits=SandboxLimits(memory_mb=1024, pids=256, workspace_mb=512, temp_mb=128),
        )
    )
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
