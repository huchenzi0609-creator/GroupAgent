"""Quantities come from scoped human evidence, not the planner's preferred batch."""

import hashlib
from datetime import UTC, datetime

import pytest

from groupagent.conversation.image_requirements import (
    normalize_image_quantity,
    projected_image_quantity,
)
from groupagent.coordinator import RequirementEvidence
from groupagent.domain import DomainError, EventKind, GroupEvent, WorkOrder
from groupagent.runtime.review import review_request

pytestmark = pytest.mark.public_image_workflow
NOW = datetime(2026, 11, 16, tzinfo=UTC)


def event(
    identifier, text, *, actor="requester", platform=None, origin="verified_channel_metadata"
):
    return GroupEvent(
        id=identifier,
        source_key=identifier,
        group_id="synthetic-group",
        actor_id=actor,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=NOW,
        received_at=NOW,
        platform_message_id=platform,
        trusted_actor_origin=origin,
    )


class Sources:
    def __init__(self, *events, bots=()):
        self.events = {item.id: item for item in events}
        self.bots = set(bots)

    def get_event(self, identifier):
        return self.events[identifier]

    def event_fingerprint(self, identifier):
        return hashlib.sha256(self.events[identifier].model_dump_json().encode()).hexdigest()

    def is_bot_message(self, identifier):
        return identifier in self.bots

    def _validate_transcription_origin(self, source):
        # Exercise the production same-sender/audio-origin/echo contract without DB.
        if source.trusted_actor_origin == "authenticated_audio_transcription":
            parent = self.events.get(source.reply_to)
            if (
                source.kind != EventKind.QUOTE
                or parent is None
                or parent.kind != EventKind.AUDIO
                or parent.actor_id != source.actor_id
                or self.is_bot_message(parent.platform_message_id)
            ):
                raise DomainError("permission_denied", "Invalid synthetic transcription origin")

    def snapshot(self):
        return {identifier: self.event_fingerprint(identifier) for identifier in self.events}


def work(source, **scope):
    return WorkOrder(
        id="synthetic-work",
        group_id=source.group_id,
        requester_id=source.actor_id,
        request_event_id=source.id,
        goal="The planner may write five here",
        scope={"work_capabilities": ["public_images"], **scope},
        created_at=NOW,
        updated_at=NOW,
    )


def normalize(tx, source, *, count=None, evidence=None, previous=None, authorized=None):
    return normalize_image_quantity(
        tx,
        source,
        requester_id=source.actor_id,
        authorized=tx.snapshot() if authorized is None else authorized,
        count=count,
        evidence=evidence,
        previous=previous,
    )


def test_unspecified_quantity_never_promotes_planner_five_or_scans_unrelated_old_messages():
    source = event("now", "请找一些海豹表情包，要PNG。")
    older = event("unrelated", "上次找五张猫照片。")
    tx = Sources(source, older)
    assert normalize(tx, source, count=5).count is None
    legacy = work(source, public_image_count=5, source_fingerprints=tx.snapshot())
    assert projected_image_quantity(tx, legacy).count is None
    assert legacy.scope["public_image_count"] == 5


@pytest.mark.parametrize("number,words", [(2, "两"), (5, "5")])
def test_current_human_quantity_wins_over_a_different_planner_number(number, words):
    source = event("now", f"请找{words}张海豹表情包，发PNG。")
    tx = Sources(source)
    result = normalize(tx, source, count=3)
    assert result.count == number and result.evidence.event_id == source.id
    assert projected_image_quantity(tx, work(source, **result.fields())).count == number


def test_wakeup_carries_two_only_with_an_exact_authorized_human_citation():
    original, wake = event("original", "找两张海豹表情包发到群里"), event("wake", "Pot！")
    tx = Sources(original, wake)
    assert normalize(tx, wake, count=5).count is None
    proof = RequirementEvidence(event_id=original.id, quote="找两张海豹表情包")
    assert normalize(tx, wake, count=2, evidence=proof).count == 2
    with pytest.raises(DomainError):
        normalize(tx, wake, count=5, evidence=proof)


def test_unrelated_update_preserves_verified_two_and_explicit_cancellation_removes_it():
    original = event("original", "找两张海豹表情包")
    update, clear = event("update", "格式改为PNG。"), event("clear", "数量不限，合适就好。")
    tx = Sources(original, update, clear)
    initial = normalize(tx, original, count=2)
    previous = work(original, **initial.fields(), source_fingerprints=tx.snapshot())
    assert normalize(tx, update, previous=previous).count == 2
    result = normalize(
        tx,
        clear,
        previous=previous,
        evidence=RequirementEvidence(event_id=clear.id, quote="数量不限"),
    )
    assert result.count is None and result.evidence.event_id == clear.id
    cleared = work(original, **result.fields(), source_fingerprints=tx.snapshot())
    assert projected_image_quantity(tx, cleared).count is None


@pytest.mark.parametrize(
    "case",
    [
        "outside_snapshot",
        "other_human",
        "bot_echo",
        "wrong_quote",
        "untrusted_origin",
        "wrong_count",
    ],
)
def test_model_cannot_forge_quantity_authority(case):
    original = event(
        "original",
        "找两张海豹表情包",
        actor="other" if case == "other_human" else "requester",
        platform="echo" if case == "bot_echo" else None,
        origin="untrusted" if case == "untrusted_origin" else "verified_channel_metadata",
    )
    wake = event("wake", "Pot！")
    tx = Sources(original, wake, bots=("echo",))
    proof = RequirementEvidence(
        event_id=original.id, quote="找五张" if case == "wrong_quote" else "找两张海豹表情包"
    )
    with pytest.raises(DomainError):
        normalize(
            tx,
            wake,
            count=5 if case == "wrong_count" else 2,
            evidence=proof,
            authorized={wake.id: tx.event_fingerprint(wake.id)}
            if case == "outside_snapshot"
            else None,
        )


def test_legacy_explicit_original_projects_two_without_rewriting_legacy_planning_five():
    source = event("original", "找两张海豹表情包")
    tx = Sources(source)
    legacy = work(source, public_image_count=5)
    assert projected_image_quantity(tx, legacy).count == 2
    assert legacy.scope["public_image_count"] == 5


def test_quantity_correction_uses_the_positive_clause_without_clipping_off_negation():
    source = event("correction", "不要五张，改成两张，写3条说明。")
    wake = event("wake", "Pot！")
    tx = Sources(source, wake)
    assert normalize(tx, source, count=5).count is None
    assert (
        normalize(
            tx, wake, count=2, evidence=RequirementEvidence(event_id=source.id, quote="改成两张")
        ).count
        == 2
    )
    with pytest.raises(DomainError):
        normalize(tx, wake, count=5, evidence=RequirementEvidence(event_id=source.id, quote="五张"))
    mixed = event("mixed", "找2张图片写3条说明。")
    assert normalize(Sources(mixed), mixed, count=5).count == 2


@pytest.mark.parametrize(
    "text,expected",
    [
        ("昨天我发过两张照片，今天帮我找一些海豹表情包。", None),
        ("我手里已有5张图，帮我找海豹表情包。", None),
        ("请找5张海豹表情包，上次我有两张。", 5),
    ],
)
def test_historical_or_owned_quantities_are_not_current_requirements(text, expected):
    source = event("request", text)
    tx = Sources(source)
    result = normalize(tx, source, count=5)
    assert result.count is None
    if expected is not None:
        assert (
            normalize(
                tx,
                source,
                count=expected,
                evidence=RequirementEvidence(event_id=source.id, quote="请找5张海豹表情包"),
            ).count
            == expected
        )
    if expected is None:
        match = "两张" if "两张" in text else "5张"
        with pytest.raises(DomainError):
            normalize(
                tx,
                source,
                count=2 if match == "两张" else 5,
                evidence=RequirementEvidence(event_id=source.id, quote=match),
            )


@pytest.mark.parametrize("invalid", [False, "different_sender", "bot_audio", "wrong_kind"])
def test_authenticated_transcription_reuses_verified_audio_origin_guard(invalid):
    audio = event(
        "audio",
        "",
        actor="other" if invalid == "different_sender" else "requester",
        platform="audio-platform",
    ).model_copy(update={"kind": EventKind.AUDIO})
    transcript = event(
        "transcript",
        "请找两张海豹表情包",
        origin="authenticated_audio_transcription",
    ).model_copy(
        update={
            "kind": EventKind.TEXT if invalid == "wrong_kind" else EventKind.QUOTE,
            "reply_to": audio.id,
        }
    )
    wake = event("wake", "Pot！")
    tx = Sources(
        audio, transcript, wake, bots=("audio-platform",) if invalid == "bot_audio" else ()
    )
    proof = RequirementEvidence(event_id=transcript.id, quote="找两张海豹表情包")
    if invalid:
        with pytest.raises(DomainError):
            normalize(tx, wake, count=2, evidence=proof)
    else:
        assert normalize(tx, wake, count=2, evidence=proof).count == 2


def test_native_quoted_reply_uses_the_current_human_body_and_ignores_quoted_data():
    current = event(
        "reply", "改成两张。", origin="agentwechat_authenticated_message_metadata"
    ).model_copy(
        update={
            "kind": EventKind.QUOTE,
            "attachments": [{"text": "机器人说要五张", "kind": "quoted_text"}],
        }
    )
    tx = Sources(current)
    assert (
        normalize(
            tx,
            current,
            count=2,
            evidence=RequirementEvidence(event_id=current.id, quote="改成两张"),
        ).count
        == 2
    )
    with pytest.raises(DomainError):
        normalize(
            tx, current, count=5, evidence=RequirementEvidence(event_id=current.id, quote="五张")
        )


def test_cancellation_quote_cannot_clip_a_historical_statement_or_a_negation():
    for text in ("以前我说数量不限，现在请找五张。", "不要数量不限，请找五张。"):
        source = event("request", text)
        tx = Sources(source)
        with pytest.raises(DomainError):
            normalize(
                tx, source, evidence=RequirementEvidence(event_id=source.id, quote="数量不限")
            )


def test_nonpublic_review_keeps_page_and_chart_quantities_even_with_null_image_payload():
    payload = {
        "goal": "Create 5 charts on 10 slides",
        "criteria": ["5 charts", "10 slides"],
        "image_quantity_requirement": {"public_image_count": None},
    }
    ordinary = review_request(payload, ())
    assert "image_quantity_requirement" not in ordinary.messages[0].content
    assert "逐项核对数量" in ordinary.messages[0].content
    public = review_request(payload, (), public_image_workflow=True)
    assert (
        "takes precedence over generated goals and criteria for public-image delivery count only"
        in public.messages[0].content
    )
    assert "does not change Office page/slide counts, chart counts" in public.messages[0].content
