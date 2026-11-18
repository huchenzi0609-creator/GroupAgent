"""Scope and canonicalize authorized attachments without creating new grants."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from pydantic import ValidationError

from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.media import MediaSource
from groupagent.persistence import UnitOfWork

from .artifacts import ArtifactRegistry

_IMAGE_FORMATS = frozenset({"png", "jpeg", "jpg", "gif", "webp"})
_ATTACHMENT_EVENT_KINDS = frozenset({EventKind.IMAGE, EventKind.AUDIO, EventKind.FILE})
_VERIFIED_CHANNEL_ORIGINS = frozenset(
    {"verified_channel_metadata", "agentwechat_authenticated_message_metadata"}
)
_SENT_IMAGES = re.compile(
    r"(?:已经|刚才|刚刚|之前|先前|已)\s*(?:发送|发)(?:过|了)?(?:到|在)?"
    r"(?:本?群(?:里|中|内)?|这里|这儿|出来)?的?(?:那|这)?\s*"
    r"([1-9][0-9]?|[一二两三四五六七八九十]{1,3})\s*张"
    r"[^，。！？\n]{0,16}?(?:图片|图像|照片|截图|表情包|图)"
)


def _sent_image_count(text: str) -> int | None:
    digits = {value: index for index, value in enumerate("零一二三四五六七八九")}
    digits["两"] = 2
    counts = set()
    for match in _SENT_IMAGES.finditer(text):
        token = match[1]
        if token.isdecimal():
            counts.add(int(token))
        elif token in digits:
            counts.add(digits[token])
        elif token.count("十") == 1:
            tens, units = token.split("十")
            if tens in {"", *digits} and units in {"", *digits}:
                counts.add(10 * (digits[tens] if tens else 1) + (digits[units] if units else 0))
    if len(counts) > 1:
        raise DomainError("ambiguous_reference", "Sent-image request has conflicting quantities")
    return next(iter(counts)) if counts else None


def _image_identity(tx: UnitOfWork, row: dict[str, Any]) -> tuple[Any, ...] | None:
    if row["format"] not in _IMAGE_FORMATS:
        return None
    provenance = row["body"].get("provenance", {})
    try:
        source = MediaSource.model_validate(
            {key: provenance[key] for key in MediaSource.model_fields if key in provenance}
        )
    except (ValidationError, TypeError):
        return None  # Missing origin evidence never establishes equivalence.
    if (
        source.group_id != tx.group_id
        or source.source_event_id not in row["source_fingerprints"]
        or tx.get_event(source.source_event_id).actor_id != source.member_id
    ):
        raise DomainError("scope_violation", "Image origin differs from its catalog evidence")
    return (
        row["sha256"],
        source.group_id,
        source.source_event_id,
        source.member_id,
        source.origin_kind,
        source.source_url,
        source.source_message_id,
        source.attachment_id,
        source.quote_message_id,
        source.sequence,
    )


def _preference(row: dict[str, Any]) -> tuple[bool, bool, bool]:
    provenance = row["body"].get("provenance", {})
    public = provenance.get("public_image")
    citations = bool(
        isinstance(public, dict)
        and public.get("sha256") == row["sha256"]
        and public.get("final_url") == provenance.get("source_url")
        and all(
            isinstance(public.get(key), str) and public[key]
            for key in ("page_final_url", "page_retrieved_at", "retrieved_at")
        )
    )
    return citations, bool(row["shared"]), not bool(provenance.get("input_version_id"))


def direct_attachment_events(
    tx: UnitOfWork, event: GroupEvent, allowed_sources: set[str]
) -> list[GroupEvent]:
    """Resolve current inputs and the direct, authorized reply target."""
    anchors = []
    for identifier in dict.fromkeys((event.id, event.reply_to)):
        if identifier is None or identifier not in allowed_sources:
            continue
        source = tx.get_event(identifier)
        if source.kind in _ATTACHMENT_EVENT_KINDS or any(
            part.get("kind") in ("image", "audio", "file") for part in source.attachments
        ):
            anchors.append(source)
    return anchors


def anchored_start_inputs(
    tx: UnitOfWork,
    registry: ArtifactRegistry | None,
    event: GroupEvent,
    allowed_sources: set[str],
    event_ids: Sequence[str],
    version_ids: Sequence[str],
    visible_versions: set[str],
    quoted_message_id: str | None,
) -> tuple[list[str], list[str]] | None:
    """Return the anchored subset only when this proposal expands beyond it."""
    anchors = {row.id: row for row in direct_attachment_events(tx, event, allowed_sources)}
    native_ids = {
        row.platform_message_id
        for row in anchors.values()
        if row.platform_message_id and row.trusted_actor_origin in _VERIFIED_CHANNEL_ORIGINS
    }
    if quoted_message_id:
        native_ids.add(quoted_message_id)
    receipt_versions: dict[str, set[str]] = {}
    if native_ids:
        receipts = tx.conn.execute(
            "SELECT DISTINCT d.payload_ref, d.payload_hash FROM ga_delivery_parts p "
            "JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id "
            "WHERE p.group_id=%s AND p.platform_message_id=ANY(%s) AND p.state='accepted' "
            "AND left(d.payload_ref,9)='artifact:'",
            (tx.group_id, sorted(native_ids)),
        ).fetchall()
        for receipt in receipts:
            version_id = receipt["payload_ref"].removeprefix("artifact:")
            if version_id in visible_versions:
                receipt_versions.setdefault(version_id, set()).add(receipt["payload_hash"])
    if not anchors and not receipt_versions:
        return None
    scoped_versions = []
    if registry is not None:
        for version_id in dict.fromkeys((*version_ids, *sorted(receipt_versions))):
            try:
                row = registry._row(tx, version_id)
            except DomainError as error:
                if error.code not in {"not_found", "source_unavailable", "permission_denied"}:
                    raise
                continue
            provenance = row["body"].get("provenance", {})
            primary = provenance.get("source_event_id")
            if (
                isinstance(primary, str)
                and primary in anchors
                and primary in row["source_fingerprints"]
                and provenance.get("origin_kind") == "channel"
                and provenance.get("group_id") == tx.group_id
                and provenance.get("member_id") == anchors[primary].actor_id
            ) or row["sha256"] in receipt_versions.get(version_id, ()):
                scoped_versions.append(version_id)
    if set(event_ids).issubset(anchors) and set(version_ids).issubset(scoped_versions):
        return None
    return list(anchors), scoped_versions


def canonicalize_input_selection(
    tx: UnitOfWork,
    registry: ArtifactRegistry,
    current_event: GroupEvent,
    version_ids: Sequence[str],
    channel_event_ids: Sequence[str],
) -> tuple[list[str], list[str]]:
    """Map native image receipts and merge only identical bytes and origins.

    The caller must first authorize every supplied selection through _inputs
    and retain that full source/task fingerprint closure. It must then admit
    mapped versions through the same _inputs checks before creating the work.
    This helper neither searches for replacements nor edits the caller's grants.
    """
    if (current_event.group_id, current_event.actor_id) != (
        tx.group_id,
        tx.actor.member_id,
    ):
        raise DomainError("scope_violation", "Input request differs from its trusted actor")
    current = tx.get_event(current_event.id)
    if current.content_state != "active":
        raise DomainError("source_unavailable", "Input request is no longer available")
    if current != current_event:
        raise DomainError("memory_changed", "Input request changed before selection")
    selected = list(dict.fromkeys(version_ids))
    remaining = []
    mapped_hashes: dict[str, set[str]] = {}
    mapped_posts: set[tuple[str | None, int | None]] = set()
    for identifier in dict.fromkeys(channel_event_ids):
        event = tx.get_event(identifier)
        if event.content_state != "active":
            raise DomainError("source_unavailable", "Selected channel input was removed")
        if (
            event.kind != EventKind.IMAGE
            or not event.platform_message_id
            or event.trusted_actor_origin not in _VERIFIED_CHANNEL_ORIGINS
        ):
            remaining.append(identifier)
            continue
        receipts = tx.conn.execute(
            "SELECT DISTINCT d.payload_ref,d.payload_hash,d.work_id,d.goal_revision "
            "FROM ga_delivery_parts p "
            "JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id "
            "WHERE p.group_id=%s AND p.platform_message_id=%s AND p.state='accepted' "
            "AND left(d.payload_ref,9)='artifact:'",
            (tx.group_id, event.platform_message_id),
        ).fetchall()
        if not receipts:
            remaining.append(identifier)
            continue
        if len(receipts) != 1:
            raise DomainError("ambiguous_reference", "Image receipt identifies multiple artifacts")
        receipt = receipts[0]
        version_id = receipt["payload_ref"][9:]
        mapped_hashes.setdefault(version_id, set()).add(receipt["payload_hash"])
        mapped_posts.add((receipt["work_id"], receipt["goal_revision"]))
        if version_id not in selected:
            selected.append(version_id)
    rows = {}
    for version_id in selected:
        registry.consume_in_tx(tx, version_id)
        row = registry._row(tx, version_id)
        registry._check_stored_version(row)
        if version_id in mapped_hashes and (
            row["format"] not in _IMAGE_FORMATS
            or mapped_hashes[version_id] != {row["sha256"]}
            or not row["shared"]
        ):
            raise DomainError("validation_failed", "Image receipt differs from its shared artifact")
        rows[version_id] = row
    identities = {version_id: _image_identity(tx, row) for version_id, row in rows.items()}
    requested_count = _sent_image_count(current.text) if mapped_hashes else None
    if requested_count is not None:
        mapped_identities = {
            identities[version_id] or ("version", version_id) for version_id in mapped_hashes
        }
        if (
            len(mapped_hashes) != requested_count
            or len(mapped_identities) != requested_count
            or len(mapped_posts) != 1
            or any(work_id is None or revision is None for work_id, revision in mapped_posts)
        ):
            raise DomainError(
                "ambiguous_reference",
                "Sent-image quantity or producing work is not uniquely proven",
            )
        # Exact accepted native image references resolve this narrow anaphora.
        # All original candidates were checked above, including discarded copies'
        # dependencies. Other media/files, such as a Word template, remain inputs.
        rows = {
            version_id: row
            for version_id, row in rows.items()
            if row["format"] not in _IMAGE_FORMATS or version_id in mapped_hashes
        }
    canonical: list[str] = []
    positions: dict[tuple[Any, ...], int] = {}
    for version_id, row in rows.items():
        identity = identities[version_id]
        if identity is None or identity not in positions:
            if identity is not None:
                positions[identity] = len(canonical)
            canonical.append(version_id)
        else:
            position = positions[identity]
            if _preference(row) > _preference(rows[canonical[position]]):
                canonical[position] = version_id
    return canonical, remaining
