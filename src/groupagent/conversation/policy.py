"""Participation commands and trusted admission at both conversation boundaries.

A model's directness label is never evidence that someone called the bot.
Quiet cuts off ordinary replies, while accepted work retains its own authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from groupagent.domain import (
    ActorContext,
    DeliveryIntent,
    DomainError,
    EventKind,
    GroupEvent,
    WorkOrder,
)
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity
from groupagent.memory.common import memory_lock
from groupagent.persistence import UnitOfWork
from groupagent.scheduling.reminders import get_reminder_body
from groupagent.scheduling.service import VERIFIED_SCHEDULE_ORIGINS

from .quiet import is_quiet
from .text import EXPLANATION, outside_quotes
from .timeparse import normalize_time
from .turns import RecipientEvidence, TurnContract, verified_human_addressee

_GROUP = re.compile(
    r"全群|本群|整个群|这个群|当前群|群内|群里所有|群的|群参与|群讨论|群消息|群处理"
)
_GROUP_PROCESSING = (
    r"(?:全群|本群|整个群|这个群|当前群|群)(?:的)?"
    r"(?:(?:普通|所有|全部)?消息|助手|机器人)?处理"
)
_PERSONAL = re.compile(r"对我|给我|跟我|和我|我这边|我的|本人|个人|只.{0,3}我")
_SPEECH = r"(?:主动(?:回应|回复|发言|补充|提醒|交流|建议)?|讲话|说话|发言|出声|回复|回应|回答|插话|打扰|交流)"
_PAUSE = re.compile(
    r"(?:不要|不用|不必|无需|不需要|别|停止|暂停|关闭|禁止).{0,12}"
    + _SPEECH
    + r"|"
    + _SPEECH
    + r".{0,8}(?:关闭|暂停|停止)"
    r"|(?:保持|进入|开始|先|请|暂时|继续).{0,4}(?:安静|静默)"
    r"|(?:^|[，,。;；\s])(?:安静|静默)(?:[，,。;；\s]|到|十|一|两|半|\d|$)"
    r"|没叫你.{0,8}(?:别|不)(?:回应|回复|讲话|说话)"
    r"|(?:先|暂时|现在)?(?:歇着|休息).{0,16}(?:等|再|叫|呼叫|点名)"
)
_RESUME = re.compile(
    r"(?:不用|无需|不必|不要|不再|别)(?:再|继续|保持|处于|进入|那么|这么|\s)*(?:安静|静默)"
    r"|(?:解除|结束)(?:安静|静默)"
    r"|恢复.{0,8}(?:正常交流|正常参与|主动|回应)"
    r"|(?:继续|开启|打开|可以).{0,8}主动|不要关闭.{0,8}主动"
)
_RESUME_SPEECH_COMMAND = re.compile(
    r"^(?:(?:现在|你|您|请|麻烦你|先)\s*)*"
    r"(?:可以|继续|恢复|重新开始|允许)(?:正常|自由|主动)?"
    + _SPEECH
    + r"(?:了|吧|啦|咯|[。.!！\s])*$"
)


@dataclass(frozen=True)
class PolicyIntent:
    operation: str
    subject: str
    transition: str
    until: str | None = None
    hours: tuple[str, ...] | None = None

    def action(self) -> dict[str, Any]:
        if self.operation == "participation":
            return {"op": "participation", "mode": self.transition}
        value: dict[str, Any] = {
            "op": "quiet",
            "scope": self.subject,
            "enabled": self.transition == "pause_proactive",
        }
        if self.until is not None:
            value["until"] = self.until
        if self.hours is not None:
            value["hours"] = list(self.hours)
        return value


def interpret_policy(
    event: GroupEvent,
    actor: ActorContext,
    identity: BotIdentity = DEFAULT_IDENTITY,
    *,
    time_zone: str = "Asia/Shanghai",
) -> PolicyIntent | None:
    return policy_intent(
        event, {"role": actor.role.value, "group_time_zone": time_zone, **identity.context()}
    )


def policy_intent(event: GroupEvent, context: dict[str, Any]) -> PolicyIntent | None:
    """Interpret only an outer policy instruction, with scope from trusted role."""
    if event.kind not in {EventKind.TEXT, EventKind.QUOTE} or len(event.text) > 2000:
        return None
    text = BotIdentity.from_context(context).strip_address(outside_quotes(event.text))
    if not text or EXPLANATION.search(text):
        return None
    if re.search(
        r"假设|假如|如果|举例|只是.{0,4}(?:引用|转发)|(?:同时|顺便|另外).{0,8}(?:制作|生成|整理|安排|提醒|计算)|(?:帮我|请你).{0,5}(?:做|制作|生成|分析|计算)",
        text,
    ):
        return None
    subject = (
        "self"
        if _PERSONAL.search(text)
        else "group"
        if _GROUP.search(text) or context.get("role") == "owner"
        else "self"
    )
    explicit_group = _GROUP.search(text) is not None
    if (
        subject == "group"
        and explicit_group
        and (
            re.search(r"(?:停止|停用|关闭).{0,10}(?:助手|机器人)|停止处理.{0,12}消息", text)
            or re.search(
                r"(?:停止|停用|关闭)(?:对)?"
                + _GROUP_PROCESSING
                + "|"
                + _GROUP_PROCESSING
                + r"(?:先|暂时)?(?:停止|停用|关闭)",
                text,
            )
        )
    ):
        return PolicyIntent("participation", "group", "disabled")
    if (
        subject == "group"
        and explicit_group
        and (
            re.search(
                r"重新启用(?:本群|这个群|当前群|全群)|(?:恢复|开启|启用).{0,8}(?:助手|机器人|处理.{0,8}消息)|恢复.{0,8}自然参与模式|切换.{0,8}自然(?:参与)?模式",
                text,
            )
            or re.search(
                r"(?:恢复|开启|启用)(?:对)?"
                + _GROUP_PROCESSING
                + "|"
                + _GROUP_PROCESSING
                + r"(?:重新)?(?:恢复|开启|启用)",
                text,
            )
        )
    ):
        return PolicyIntent("participation", "group", "natural")
    if (
        subject == "group"
        and explicit_group
        and not re.search(r"安静|静默", text)
        and re.search(r"以后|模式|只有|仅在|只在", text)
        and re.search(r"明确.{0,8}(?:叫|呼叫|点名)|(?:被|收到)@", text)
    ):
        return PolicyIntent("participation", "group", "on_request")
    resume = bool(_RESUME.search(text) or _RESUME_SPEECH_COMMAND.fullmatch(text))
    pause = bool(_PAUSE.search(text))
    if re.search(r"不要恢复.{0,8}主动", text):
        resume, pause = False, True
    if resume:
        return PolicyIntent("quiet", subject, "resume_proactive")
    if not pause:
        return None
    if re.search(r"(?:后|之后).{0,4}(?:再|开始).{0,8}(?:安静|静默)", text):
        return None
    zone = str(context.get("group_time_zone", "Asia/Shanghai"))
    window = re.search(
        r"(?:每天|每日)\s*(.+?)\s*(?:到|至)\s*(.+?)(?=保持|安静|静默|不要|别|不主动|$)", text
    )
    if window:
        left = normalize_time("今天" + window[1], event.occurred_at, zone)
        right = normalize_time("今天" + window[2], event.occurred_at, zone)
        if left.local_time and right.local_time and left.local_time != right.local_time:
            return PolicyIntent(
                "quiet",
                subject,
                "pause_proactive",
                hours=(left.local_time + "-" + right.local_time,),
            )
        return None
    temporal = normalize_time(text, event.occurred_at, zone)
    if temporal.instant:
        return PolicyIntent("quiet", subject, "pause_proactive", until=temporal.instant)
    duration = re.search(r"(?:半|\d+|[一二两三四五六七八九十]+)(?:小时|分钟)", text)
    if duration and not re.search(r"(?:小时|分钟)后", text):
        temporal = normalize_time(duration.group() + "后", event.occurred_at, zone)
        if temporal.instant:
            return PolicyIntent("quiet", subject, "pause_proactive", until=temporal.instant)
    if re.search(r"直到|安静到|静默到|持续到|今晚|明天|明晚|分钟|小时", text):
        return None
    return PolicyIntent("quiet", subject, "pause_proactive")


@dataclass(frozen=True)
class PolicyAdmission:
    allowed: bool
    reason_code: str = "policy_allows"
    invocation_root_event_id: str | None = None
    recipient: RecipientEvidence | None = None


@dataclass(frozen=True)
class DeliveryAdmission:
    allowed: bool
    reason_code: str = "policy_allows"
    retire: bool = False


def _sequence(tx: UnitOfWork, event_id: str) -> int | None:
    row = tx.conn.execute(
        "SELECT sequence FROM ga_events WHERE group_id=%s AND id=%s",
        (tx.group_id, event_id),
    ).fetchone()
    return int(row["sequence"]) if row else None


def recipient_evidence_in_tx(
    tx: UnitOfWork,
    event: GroupEvent,
    identity: BotIdentity = DEFAULT_IDENTITY,
    *,
    after_sequence: int | None = None,
    bot_member_id: str | None = None,
    shared_account: bool = False,
) -> RecipientEvidence:
    """Resolve direction once from native facts, then natural address/session.

    A shared account ID is a human identity, not proof of the Pot persona.
    Only a bot delivery receipt can authenticate a quote of that persona.
    """
    sequence = _sequence(tx, event.id)
    cutoff = after_sequence or 0
    if sequence is None or sequence <= cutoff:
        return RecipientEvidence()
    outside = outside_quotes(event.text)
    if identity.directly_addressed(outside):
        return RecipientEvidence(
            recipient="agent",
            source="direct_address",
            invocation_root_event_id=event.id,
        )
    if event.trusted_actor_origin == "agentwechat_authenticated_message_metadata":
        mentions = event.mentioned_member_ids
        if mentions:
            isolated_bot = not shared_account and bot_member_id in mentions
            humans = [member for member in mentions if shared_account or member != bot_member_id]
            if isolated_bot and not humans:
                return RecipientEvidence(
                    recipient="agent", source="native_mention", invocation_root_event_id=event.id
                )
            if humans:
                return RecipientEvidence(
                    recipient="other",
                    source="native_mention",
                    member_id=humans[0] if len(humans) == 1 else None,
                )
    if event.reply_to:
        source = tx.conn.execute(
            "SELECT actor_id,body->>'platform_message_id' AS platform_message_id FROM ga_events "
            "WHERE group_id=%s AND id=%s AND memory_state='active'",
            (tx.group_id, event.reply_to),
        ).fetchone()
        platform_id = source["platform_message_id"] if source else event.reply_to
        if platform_id and tx.is_bot_message(platform_id):
            return RecipientEvidence(
                recipient="agent", source="bot_reply", invocation_root_event_id=event.id
            )
        # Native quote metadata comes from authenticated adapter normalization.
        # This also works before the bot's outgoing echo has been ingested;
        # quoted text and quoted display names never supply this evidence.
        if (
            event.kind == EventKind.QUOTE
            and event.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
            and event.channel_account_id
            and any(
                part.get("kind") == "quote"
                and part.get("state") in {"embedded", "missing"}
                and isinstance(part.get("server_id"), str)
                and tx.is_bot_message(str(part["server_id"]))
                for part in event.attachments
            )
        ):
            return RecipientEvidence(
                recipient="agent", source="bot_reply", invocation_root_event_id=event.id
            )
        quote = next((part for part in event.attachments if part.get("kind") == "quote"), {})
        quoted_sender = (
            source["actor_id"]
            if source
            else (
                quote.get("sender_id")
                if event.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
                else None
            )
        )
        if isinstance(quoted_sender, str) and quoted_sender and quoted_sender != event.actor_id:
            return RecipientEvidence(
                recipient="other", source="human_reply", member_id=quoted_sender
            )
    rows = tx.conn.execute(
        "SELECT member_id,COALESCE(room_name,confirmed_name,'群昵称待补充') AS name,"
        "CASE WHEN room_name IS NOT NULL THEN 'room_display_name' "
        "WHEN confirmed_name IS NOT NULL THEN 'owner_confirmed' ELSE 'unknown' END AS name_source,"
        "membership_state FROM ga_members "
        "WHERE group_id=%s AND membership_state<>'left' ORDER BY member_id LIMIT 10000",
        (tx.group_id,),
    ).fetchall()
    human = verified_human_addressee(
        outside,
        {
            **identity.context(),
            "requester_member_id": event.actor_id,
            "members": rows,
        },
    )
    if human:
        return RecipientEvidence(recipient="other", source="human_address", member_id=human)
    if identity.directly_addressed(outside):
        return RecipientEvidence(
            recipient="agent", source="direct_address", invocation_root_event_id=event.id
        )
    previous = tx.conn.execute(
        "SELECT e.actor_id,e.memory_state,l.invocation_root_event_id,root.sequence AS root_sequence "
        "FROM ga_events e LEFT JOIN ga_participation_log l "
        "ON l.group_id=e.group_id AND l.event_id=e.id LEFT JOIN ga_events root "
        "ON root.group_id=l.group_id AND root.id=l.invocation_root_event_id "
        "AND root.actor_id=e.actor_id AND root.memory_state='active' "
        "WHERE e.group_id=%s AND e.sequence>%s AND e.sequence<%s "
        "AND e.kind NOT IN ('system','membership','recall') "
        "AND NOT EXISTS(SELECT 1 FROM ga_deliveries d WHERE d.group_id=e.group_id "
        "AND d.platform_message_id=e.body->>'platform_message_id' AND d.state='accepted') "
        "AND NOT EXISTS(SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=e.group_id "
        "AND p.platform_message_id=e.body->>'platform_message_id' AND p.state='accepted') "
        "ORDER BY e.sequence DESC LIMIT 1",
        (tx.group_id, cutoff, sequence),
    ).fetchone()
    if (
        previous
        and previous["actor_id"] == event.actor_id
        and previous["memory_state"] == "active"
        and previous["invocation_root_event_id"]
        and previous["root_sequence"] is not None
        and previous["root_sequence"] > cutoff
    ):
        return RecipientEvidence(
            recipient="agent",
            source="continuation",
            invocation_root_event_id=str(previous["invocation_root_event_id"]),
        )
    return RecipientEvidence()


def trusted_invocation_in_tx(
    tx: UnitOfWork,
    event: GroupEvent,
    identity: BotIdentity = DEFAULT_IDENTITY,
    *,
    after_sequence: int | None = None,
    bot_member_id: str | None = None,
    shared_account: bool = False,
) -> str | None:
    """Compatibility projection of the authoritative recipient evidence."""
    return recipient_evidence_in_tx(
        tx,
        event,
        identity,
        after_sequence=after_sequence,
        bot_member_id=bot_member_id,
        shared_account=shared_account,
    ).invocation_root_event_id


def _policies(tx: UnitOfWork, member_id: str) -> list[dict]:
    memory_lock(tx)
    return tx.conn.execute(
        "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s) FOR SHARE",
        (tx.group_id, member_id),
    ).fetchall()


def ingress_admission(
    tx: UnitOfWork,
    event: GroupEvent,
    identity: BotIdentity = DEFAULT_IDENTITY,
    *,
    time_zone: str = "Asia/Shanghai",
    quiet_hours: list[str] | None = None,
    default_mode: str = "natural",
    now: datetime | None = None,
    bot_member_id: str | None = None,
    shared_account: bool = False,
) -> PolicyAdmission:
    policies = _policies(tx, event.actor_id)
    cutoff = max((row.get("quiet_since_sequence") or 0 for row in policies), default=0)
    sequence = _sequence(tx, event.id)
    if sequence is not None and sequence <= cutoff:
        return PolicyAdmission(False, "quiet_barrier")
    recipient = recipient_evidence_in_tx(
        tx,
        event,
        identity,
        after_sequence=cutoff,
        bot_member_id=bot_member_id,
        shared_account=shared_account,
    )
    root = recipient.invocation_root_event_id
    mode = next((row["mode"] for row in policies if row["subject_id"] == "*"), default_mode)
    if mode in {"paused", "disabled"}:
        return PolicyAdmission(False, "group_" + mode)
    quiet = is_quiet(policies, now or datetime.now(UTC), time_zone, quiet_hours)
    if quiet and root is None:
        return PolicyAdmission(False, "quiet_barrier")
    if mode == "on_request" and root is None:
        return PolicyAdmission(False, "explicit_invocation_required")
    return PolicyAdmission(True, invocation_root_event_id=root, recipient=recipient)


def _pending_obsolete_notices(
    tx: UnitOfWork, cutoff: int, subject_id: str, *, intent_id: str | None = None
) -> list[str]:
    """Find unsubmitted status text linked to work accepted before the barrier."""
    rows = tx.conn.execute(
        "SELECT d.id FROM ga_deliveries d "
        "WHERE d.group_id=%s AND d.state='pending' "
        "AND d.work_id IS NULL AND d.request_event_id IS NULL AND d.payload_ref LIKE 'text:%%' "
        "AND (%s::text IS NULL OR d.id=%s) "
        "AND d.platform_message_id IS NULL AND d.platform_job_id IS NULL "
        "AND EXISTS (SELECT 1 FROM ga_progress_notices n "
        "    JOIN ga_work_orders w ON w.group_id=n.group_id AND w.id=n.work_id "
        "    JOIN ga_events e ON e.group_id=w.group_id AND e.id=w.request_event_id "
        "    WHERE n.group_id=d.group_id AND n.intent_id=d.id "
        "    AND d.created_by=w.requester_id AND e.sequence<=%s "
        "    AND (%s='*' OR w.requester_id=%s) "
        "    AND NOT (w.state IN ('waiting_input','paused','failed') "
        "        AND split_part(n.notice_key,':',1)=w.state "
        "        AND split_part(n.notice_key,':',2)=w.goal_revision::text)) "
        "AND NOT EXISTS (SELECT 1 FROM ga_schedule_occurrences o "
        "    WHERE o.group_id=d.group_id AND o.delivery_id=d.id) "
        "AND NOT EXISTS (SELECT 1 FROM ga_delivery_parts p "
        "    WHERE p.group_id=d.group_id AND p.intent_id=d.id AND p.state<>'pending') "
        "ORDER BY d.id FOR UPDATE OF d",
        (tx.group_id, intent_id, intent_id, cutoff, subject_id, subject_id),
    ).fetchall()
    return [row["id"] for row in rows]


def delivery_admission_in_tx(
    tx: UnitOfWork,
    intent: DeliveryIntent,
    identity: BotIdentity = DEFAULT_IDENTITY,
    *,
    time_zone: str = "Asia/Shanghai",
    quiet_hours: list[str] | None = None,
    default_mode: str = "natural",
    now: datetime | None = None,
    bot_member_id: str | None = None,
    shared_account: bool = False,
) -> DeliveryAdmission:
    """Derive purpose from durable provenance, never from model labels."""
    if intent.idempotency_key.startswith("react-observer:"):
        requester = tx.conn.execute(
            "SELECT e.actor_id FROM ga_react_observations o JOIN ga_events e "
            "ON e.group_id=o.group_id AND e.id=o.source_event_id "
            "WHERE o.group_id=%s AND o.delivery_id=%s AND o.state='published'",
            (tx.group_id, intent.id),
        ).fetchone()
        if requester is None:
            return DeliveryAdmission(False, "observer_source_unavailable", retire=True)
        policies = _policies(tx, requester["actor_id"])
        mode = next((row["mode"] for row in policies if row["subject_id"] == "*"), default_mode)
        if mode != "natural":
            return DeliveryAdmission(False, "observer_participation_changed", retire=True)
        quiet = is_quiet(policies, now or datetime.now(UTC), time_zone, quiet_hours)
        return DeliveryAdmission(not quiet, "quiet_hold" if quiet else "policy_allows")
    if (
        intent.work_id is None
        and intent.request_event_id is None
        and intent.idempotency_key.startswith("schedule-occurrence:")
    ):
        policies = _policies(tx, intent.created_by)
        scheduled = tx.conn.execute(
            "SELECT o.id,o.delivery_purpose,s.creator_id,s.state,s.valid_until,s.policy_revision "
            "FROM ga_schedule_occurrences o JOIN ga_schedules s "
            "ON s.group_id=o.group_id AND s.id=o.schedule_id "
            "WHERE o.group_id=%s AND o.delivery_id=%s",
            (tx.group_id, intent.id),
        ).fetchone()
        if (
            scheduled is None
            or scheduled["creator_id"] != intent.created_by
            or intent.idempotency_key != "schedule-occurrence:" + scheduled["id"]
            or not intent.payload_ref.startswith("text:")
        ):
            return DeliveryAdmission(False, "invalid_schedule_delivery", retire=True)
        current = now or datetime.now(UTC)
        if scheduled["state"] == "cancelled":
            return DeliveryAdmission(False, "schedule_cancelled", retire=True)
        if scheduled["policy_revision"] != intent.policy_revision:
            return DeliveryAdmission(False, "schedule_policy_changed", retire=True)
        if current > scheduled["valid_until"]:
            return DeliveryAdmission(False, "schedule_validity_expired", retire=True)
        if scheduled["state"] == "paused":
            return DeliveryAdmission(False, "schedule_paused")
        return _scheduled_quiet(
            policies,
            current,
            time_zone,
            quiet_hours,
            default_mode,
            explicit=scheduled["delivery_purpose"] == "explicit_reminder",
        )
    notice = tx.conn.execute(
        "SELECT work_id FROM ga_progress_notices WHERE group_id=%s AND intent_id=%s",
        (tx.group_id, intent.id),
    ).fetchone()
    work_id = intent.work_id or (notice["work_id"] if notice else None)
    work = tx.get_work(work_id) if work_id else None
    autonomous = bool(
        notice
        or not (intent.request_event_id or work)
        or (
            work
            and (work.scope.get("origin_schedule_id") or work.scope.get("origin_trigger_rule_id"))
        )
    )
    if not autonomous and work is not None:
        return DeliveryAdmission(True)
    if not autonomous and intent.request_event_id:
        body = tx.get_decision(intent.request_event_id) or {}
        saved_contract = body.get("turn_contract")
        if saved_contract is not None and not TurnContract.model_validate(saved_contract).visible:
            return DeliveryAdmission(False, "turn_not_addressed_to_agent", retire=True)
        saved_result = body.get("result")
        saved_decision = saved_result.get("decision") if isinstance(saved_result, dict) else None
        if (
            not body.get("agent_pending_response")
            and isinstance(saved_decision, dict)
            and saved_decision.get("kind") in ("observe", "ignore")
        ):
            return DeliveryAdmission(False, "turn_not_addressed_to_agent", retire=True)
        admission = ingress_admission(
            tx,
            tx.get_event(intent.request_event_id),
            identity,
            time_zone=time_zone,
            quiet_hours=quiet_hours,
            default_mode=default_mode,
            now=now,
            bot_member_id=bot_member_id,
            shared_account=shared_account,
        )
        return DeliveryAdmission(
            admission.allowed, admission.reason_code, retire=not admission.allowed
        )
    policies = _policies(tx, work.requester_id if work else intent.created_by)
    if notice and (
        cutoff := max((row.get("quiet_since_sequence") or 0 for row in policies), default=0)
    ):
        if _pending_obsolete_notices(tx, cutoff, "*", intent_id=intent.id):
            return DeliveryAdmission(False, "quiet_barrier", retire=True)
    if work and not notice and work.scope.get("origin_schedule_id"):
        return _scheduled_quiet(
            policies,
            now or datetime.now(UTC),
            time_zone,
            quiet_hours,
            default_mode,
            explicit=_exact_legacy_reminder(tx, intent, work),
        )
    mode = next((row["mode"] for row in policies if row["subject_id"] == "*"), default_mode)
    held = mode in {"paused", "disabled"} or is_quiet(
        policies, now or datetime.now(UTC), time_zone, quiet_hours
    )
    return DeliveryAdmission(not held, "quiet_hold" if held else "policy_allows")


def _scheduled_quiet(
    policies: list[dict],
    now: datetime,
    time_zone: str,
    quiet_hours: list[str] | None,
    default_mode: str,
    *,
    explicit: bool,
) -> DeliveryAdmission:
    group_policies = [row for row in policies if row["subject_id"] == "*"]
    mode = group_policies[0]["mode"] if group_policies else default_mode
    if mode in {"paused", "disabled"}:
        return DeliveryAdmission(False, "group_" + mode)
    if is_quiet(group_policies, now, time_zone, quiet_hours):
        return DeliveryAdmission(False, "group_quiet_hold")
    if not explicit and is_quiet(policies, now, time_zone, quiet_hours):
        return DeliveryAdmission(False, "quiet_hold")
    return DeliveryAdmission(True)


def _exact_legacy_reminder(tx: UnitOfWork, intent: DeliveryIntent, work: WorkOrder) -> bool:
    """Recognize only an existing scheduled WorkOrder delivering its exact authorized body."""
    try:
        body = get_reminder_body(work.scope)
    except DomainError:
        return False
    if not body or intent.payload_ref not in {"text:" + body, "text:提醒：" + body}:
        return False
    occurrence = tx.conn.execute(
        "SELECT 1 FROM ga_schedule_occurrences WHERE group_id=%s AND work_id=%s "
        "AND id=%s AND schedule_id=%s",
        (
            tx.group_id,
            work.id,
            work.scope.get("origin_occurrence_id"),
            work.scope["origin_schedule_id"],
        ),
    ).fetchone()
    if not occurrence:
        return False
    source_id = work.scope["reminder_body_source_event_id"]
    fingerprints = work.scope.get("source_fingerprints")
    if not isinstance(source_id, str) or not isinstance(fingerprints, dict):
        return False
    try:
        source = tx.get_event(source_id)
    except DomainError:
        return False
    return (
        source.actor_id == work.requester_id
        and source.trusted_actor_origin in VERIFIED_SCHEDULE_ORIGINS
        and source.content_state == "active"
        and body in source.text
        and tx.event_fingerprint(source_id) == fingerprints.get(source_id)
    )


def retire_delivery_in_tx(tx: UnitOfWork, intent_id: str, *, reason: str = "quiet_barrier") -> None:
    """Retire only proven unsubmitted parts; native evidence remains immutable."""
    tx.conn.execute(
        "SELECT id FROM ga_deliveries WHERE group_id=%s AND id=%s FOR UPDATE",
        (tx.group_id, intent_id),
    ).fetchone()
    tx.conn.execute(
        "UPDATE ga_delivery_parts SET state='rejected',reason=%s,updated_at=clock_timestamp() "
        "WHERE group_id=%s AND intent_id=%s AND state='pending'",
        (reason, tx.group_id, intent_id),
    )
    tx.conn.execute(
        "UPDATE ga_deliveries d SET state='rejected',reason=%s,claim_expires_at=NULL,"
        "revision=revision+1,updated_at=clock_timestamp() WHERE d.group_id=%s AND d.id=%s "
        "AND d.state IN ('pending','dispatching','unknown') AND NOT EXISTS "
        "(SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=d.group_id AND p.intent_id=d.id "
        "AND p.state IN ('submitted','unknown')) AND (NOT EXISTS "
        "(SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=d.group_id AND p.intent_id=d.id) "
        "OR EXISTS (SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=d.group_id "
        "AND p.intent_id=d.id AND p.state='rejected'))",
        (reason, tx.group_id, intent_id),
    )


def apply_quiet_barrier_in_tx(tx: UnitOfWork, event: GroupEvent, subject_id: str) -> int:
    """Advance a source sequence barrier without cancelling accepted work."""
    memory_lock(tx)
    sequence = _sequence(tx, event.id)
    if sequence is None:
        return 0
    tx.conn.execute(
        "UPDATE ga_conversation_policy SET quiet_since_sequence=GREATEST("
        "COALESCE(quiet_since_sequence,0),%s) WHERE group_id=%s AND subject_id=%s",
        (sequence, tx.group_id, subject_id),
    )
    rows = tx.conn.execute(
        "SELECT d.id FROM ga_deliveries d JOIN ga_events e "
        "ON e.group_id=d.group_id AND e.id=d.request_event_id WHERE d.group_id=%s "
        "AND d.work_id IS NULL AND d.state IN ('pending','dispatching','unknown') "
        "AND e.sequence<=%s AND (%s='*' OR e.actor_id=%s) ORDER BY d.id FOR UPDATE OF d",
        (tx.group_id, sequence, subject_id, subject_id),
    ).fetchall()
    for row in rows:
        retire_delivery_in_tx(tx, row["id"])
    obsolete_notices = _pending_obsolete_notices(tx, sequence, subject_id)
    for intent_id in obsolete_notices:
        retire_delivery_in_tx(tx, intent_id)
    return len(rows) + len(obsolete_notices)
