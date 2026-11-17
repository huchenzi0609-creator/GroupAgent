"""Ground image quantities in exact human requests, independently of execution plans."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from groupagent.coordinator import RequirementEvidence
from groupagent.domain import DomainError, EventKind, GroupEvent, WorkOrder
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity
from groupagent.persistence import UnitOfWork
from groupagent.persistence.postgres import _VERIFIED_REPLY_ORIGINS

from .intent import _page_count, main_request, public_image_request_count
from .participation import outside_quotes

_COUNT = re.compile(
    r"([0-9]{1,3}|[一二两三四五六七八九十]{1,3})\s*(?:张|幅|个(?:图片|表情包|照片)|images?\b|pictures?\b)",
    re.I,
)
_INEXACT = re.compile(
    r"最多|不超过|至多|大约|左右|任选|[0-9一二两三四五六七八九十]\s*(?:到|至|[-~～])"
)
_UNFIXED = re.compile(
    r"(?:数量|张数|图片数)\s*(?:不(?:再)?限|不限|随意|你定|自行决定)"
    r"|(?:不限|不限制|不用固定|无需固定)\s*(?:数量|张数|图片数)"
    r"|\b(?:any number|no fixed (?:number|count))\b",
    re.I,
)
_CLAUSE = re.compile(r"[，,。；;!?！？\n]")
_NEGATED = re.compile(r"^(?:我(?:们)?)?(?:不|别|勿|无需|无须|not\b|no\b)", re.I)
_REQUEST = re.compile(
    r"^(?:(?:现在|今天|这次|先|再|只|就|还是)\s*)*"
    r"(?:找|寻找|搜索|搜|检索|下载|获取|交付|发送|发|提供|给我|需要|要|想要|我想要|"
    r"改成|改为|换成|调整为|保留|\b(?:find|search|send|deliver|provide|give|need|want|change)\b)",
    re.I,
)
IMAGE_QUANTITY_INSTRUCTION = (
    "For public images, only the host-verified image_quantity_requirement defines a fixed "
    "delivery quantity. A null public_image_count means no fixed quantity was requested. "
    "This contract takes precedence over generated goals and criteria for public-image "
    "delivery count only. It does not change Office page/slide counts, chart counts, "
    "requested formats or content requirements. "
    "Numbers introduced by planning, generated goals/criteria, tool batches or earlier "
    "validation diagnostics are not additional acceptance requirements. Keep relevance, "
    "public-source, format and nonempty-result checks."
)


def _quantity(text: str, *, require_request: bool = True) -> int | None:
    outside = outside_quotes(text)
    if _INEXACT.search(outside) or _UNFIXED.search(outside):
        return None
    matches: list[re.Match[str]] = []
    for clause in _CLAUSE.split(outside):
        statement = main_request(clause.strip())
        # A requested count, correction or stand-alone quantity answer is not
        # the same as a count mentioned in a historical/ownership statement.
        if _NEGATED.match(statement) or (
            require_request and not (_REQUEST.match(statement) or _COUNT.fullmatch(statement))
        ):
            continue
        matches.extend(_COUNT.finditer(statement))
    if len(matches) != 1:
        return None
    count = _page_count(matches[0][1])
    return count if count is not None and 1 <= count <= 10 else None


def _quoted_quantity(source: str, quote: str, count: int) -> bool:
    """Keep a quote's enclosing clause, so cutting off a negation cannot grant it."""
    positions = [match.start() for match in re.finditer(re.escape(quote), source)]
    cited = main_request(outside_quotes(quote).strip())
    span = _COUNT.search(cited)
    for position in positions:
        before = list(_CLAUSE.finditer(source[:position]))
        start = before[-1].end() if before else 0
        after = _CLAUSE.search(source, position + len(quote))
        stop = after.start() if after else len(source)
        clause = source[start:stop]
        if _quantity(clause) == _quantity(quote) == count:
            continue
        # A source clause can express the directive with an unfamiliar verb.
        # An extracted numeral alone cannot ground a new requirement.
        if (
            span is None
            or not cited[: span.start()].strip()
            or _CLAUSE.search(cited)
            or _quantity(clause, require_request=False) != count
            or _quantity(quote, require_request=False) != count
        ):
            return False
    return bool(positions)


def _current_quantity(text: str, identity: BotIdentity) -> int | None:
    """The fast path stays a stand-alone request, not a second semantic router."""
    outside = outside_quotes(text)
    if len(list(_COUNT.finditer(outside))) != 1:
        return None
    known = public_image_request_count(text, identity=identity)
    if known is not None:
        return known
    first = main_request(_CLAUSE.split(outside)[0].strip(), identity=identity)
    return _quantity(text) if _REQUEST.match(first) or _COUNT.fullmatch(first) else None


def _quoted_cancellation(source: str, quote: str) -> bool:
    if not _UNFIXED.search(outside_quotes(quote)):
        return False
    positions = [match.start() for match in re.finditer(re.escape(quote), source)]
    for position in positions:
        before = list(_CLAUSE.finditer(source[:position]))
        start = before[-1].end() if before else 0
        after = _CLAUSE.search(source, position + len(quote))
        stop = after.start() if after else len(source)
        if not _UNFIXED.match(main_request(outside_quotes(source[start:stop]).strip())):
            return False
    return bool(positions)


@dataclass(frozen=True)
class ImageQuantity:
    count: int | None = None
    evidence: RequirementEvidence | None = None
    author_id: str | None = None

    def fields(self) -> dict[str, Any]:
        return {
            "public_image_count": self.count,
            "public_image_count_evidence": self.evidence.model_dump(mode="json")
            if self.evidence
            else None,
            "public_image_count_author_id": self.author_id,
        }


def is_public_image_work(work: WorkOrder) -> bool:
    """Legacy planning counts may identify a workflow, never its acceptance quantity."""
    capabilities = work.scope.get("work_capabilities", [])
    tools = work.scope.get("required_tools", [])
    return (
        isinstance(capabilities, list)
        and "public_images" in capabilities
        or isinstance(tools, list)
        and "web_image_import" in tools
        or work.scope.get("public_image_count") is not None
    )


def _verify(
    tx: UnitOfWork,
    evidence: RequirementEvidence,
    count: int | None,
    *,
    requester_id: str,
    authorized: Mapping[str, Any],
) -> ImageQuantity:
    fingerprint = authorized.get(evidence.event_id)
    if not isinstance(fingerprint, str) or tx.event_fingerprint(evidence.event_id) != fingerprint:
        raise DomainError(
            "invalid_input", "Image quantity evidence is outside the authorized snapshot"
        )
    source = tx.get_event(evidence.event_id)
    if (
        source.actor_id != requester_id
        or source.kind not in {EventKind.TEXT, EventKind.QUOTE}
        or source.trusted_actor_origin not in _VERIFIED_REPLY_ORIGINS
        or source.content_state in {"forgotten", "recalled"}
        or source.platform_message_id
        and tx.is_bot_message(source.platform_message_id)
        or evidence.quote not in source.text
    ):
        raise DomainError(
            "invalid_input", "Image quantity evidence is not an exact requester quotation"
        )
    tx._validate_transcription_origin(source)
    if source.trusted_actor_origin == "authenticated_audio_transcription":
        parent = source.reply_to
        if (
            parent is None
            or parent not in authorized
            or tx.event_fingerprint(parent) != authorized[parent]
        ):
            raise DomainError(
                "invalid_input", "Image quantity audio source is outside the snapshot"
            )
    if count is None:
        matches = _quoted_cancellation(source.text, evidence.quote)
    else:
        matches = _quoted_quantity(source.text, evidence.quote, count)
    if not matches:
        raise DomainError("invalid_input", "Image quantity does not match its human source")
    return ImageQuantity(count, evidence, requester_id)


def projected_image_quantity(tx: UnitOfWork, work: WorkOrder) -> ImageQuantity:
    """Read old work without rewriting it or trusting a legacy planner count.

    A stored proof must still match its immutable source. Legacy work without a
    proof can use only its own original human request, not arbitrary old context,
    generated goals or an integer in scope. Referenced older requests need proof.
    """
    sources = work.scope.get("source_fingerprints", {})
    authorized = dict(sources) if isinstance(sources, dict) else {}
    original = tx.get_event(work.request_event_id)
    authorized.setdefault(original.id, tx.event_fingerprint(original.id))
    if original.trusted_actor_origin == "authenticated_audio_transcription" and original.reply_to:
        authorized.setdefault(original.reply_to, tx.event_fingerprint(original.reply_to))
    raw_evidence = work.scope.get("public_image_count_evidence")
    if raw_evidence is not None:
        try:
            evidence = RequirementEvidence.model_validate(raw_evidence)
        except ValueError:
            raise DomainError(
                "invalid_input", "Stored image quantity evidence is invalid"
            ) from None
        count = work.scope.get("public_image_count")
        if count is not None and (type(count) is not int or not 1 <= count <= 10):
            raise DomainError("invalid_input", "Stored image quantity is invalid")
        return _verify(
            tx,
            evidence,
            count,
            requester_id=str(work.scope.get("public_image_count_author_id") or work.requester_id),
            authorized=authorized,
        )
    if not is_public_image_work(work) or original.actor_id != work.requester_id:
        return ImageQuantity()
    count = _quantity(original.text)
    if count is not None and len(original.text) <= 2000:
        return _verify(
            tx,
            RequirementEvidence(event_id=original.id, quote=original.text),
            count,
            requester_id=work.requester_id,
            authorized=authorized,
        )
    return ImageQuantity()


def normalize_image_quantity(
    tx: UnitOfWork,
    event: GroupEvent,
    *,
    requester_id: str,
    authorized: Mapping[str, Any],
    count: int | None,
    evidence: RequirementEvidence | None,
    previous: WorkOrder | None = None,
    image_work: bool = True,
    identity: BotIdentity = DEFAULT_IDENTITY,
) -> ImageQuantity:
    """The same admission boundary handles a new plan and a revision.

    Only the current request is automatically interpreted. Context carry-forward
    requires the candidate's exact citation or an existing verified requirement.
    An ungrounded model number remains planning/audit data, not a hard constraint.
    """
    if evidence is not None:
        return _verify(tx, evidence, count, requester_id=requester_id, authorized=authorized)
    if image_work:
        text = identity.strip_address(event.text)
        current = _current_quantity(text, identity)
        if len(event.text) <= 2000 and current is not None:
            return _verify(
                tx,
                RequirementEvidence(event_id=event.id, quote=event.text),
                current,
                requester_id=requester_id,
                authorized=authorized,
            )
    return projected_image_quantity(tx, previous) if previous is not None else ImageQuantity()
