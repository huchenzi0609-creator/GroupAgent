"""Scoped received evidence and one deterministic projection for readers and reviewers.

Provider reads are paid observations. Persist their received body once; clipping a
model view must never destroy the only copy or require another external read.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from groupagent.domain import DomainError
from groupagent.tools import ToolContext
from groupagent.tools.paths import open_parent, scoped_path

from .models import EvidenceRef, ResearchSource

_OMISSION = "\n[... omitted ...]\n"


def scope_key(context: ToolContext) -> str:
    return hashlib.sha256(
        json.dumps(
            [context.group_id, context.work_id, context.actor.member_id],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _snapshot_id(context: ToolContext, source: ResearchSource, content_hash: str) -> str:
    return hashlib.sha256(
        json.dumps(
            [
                scope_key(context),
                source.url,
                source.provider,
                source.operation,
                source.evidence_kind,
                source.retrieved_at.isoformat(),
                content_hash,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class EvidenceStore:
    """Host-only UTF-8 blobs, indexed by receipt manifests rather than a new database.

    An omitted root uses the already isolated work workspace. An explicit shared
    root still gets a directory per group/work/member digest. Model arguments
    select a source ID; they never choose a path or a scope.
    """

    def __init__(
        self,
        root: Path | None = None,
        *,
        max_source_bytes: int = 2_000_000,
        max_scope_bytes: int = 32_000_000,
    ) -> None:
        if not 0 < max_source_bytes <= 2_000_000 or max_scope_bytes < max_source_bytes:
            raise ValueError("Evidence bounds must fit the provider response guard")
        self.root = root.resolve() if root is not None else None
        self.max_source_bytes = max_source_bytes
        self.max_scope_bytes = max_scope_bytes

    def _root(self, context: ToolContext) -> Path:
        base = self.root or scoped_path(context.workspace, ".research-evidence")
        if base.is_symlink():
            raise DomainError("scope_violation", "Evidence storage cannot follow a symlink")
        base.mkdir(parents=True, exist_ok=True, mode=0o700)
        return base

    def save(self, context: ToolContext, source: ResearchSource) -> ResearchSource:
        content = source.content.encode("utf-8")
        if len(content) > self.max_source_bytes:
            raise DomainError("research_evidence_too_large", "Received evidence exceeds its bound")
        digest = hashlib.sha256(content).hexdigest()
        source_id = _snapshot_id(context, source, digest)
        reference = EvidenceRef(
            source_id=source_id,
            scope_key=scope_key(context),
            content_sha256=digest,
            content_bytes=len(content),
        )
        base = self._root(context)
        directory = scoped_path(base, reference.scope_key)
        directory.mkdir(exist_ok=True, mode=0o700)
        relative = reference.scope_key + "/" + source_id + ".txt"
        with open_parent(base, relative) as (parent, name):
            try:
                descriptor = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent,
                )
            except FileExistsError:
                self.load(context, reference)
            else:
                try:
                    existing = sum(
                        entry.lstat().st_size
                        for entry in directory.iterdir()
                        if stat.S_ISREG(entry.lstat().st_mode)
                    )
                    if existing + len(content) > self.max_scope_bytes:
                        raise DomainError(
                            "research_evidence_limit",
                            "This work's saved evidence reached its bound",
                        )
                    with os.fdopen(descriptor, "wb", closefd=False) as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                except BaseException:
                    os.unlink(name, dir_fd=parent)
                    raise
                finally:
                    os.close(descriptor)
        return source.model_copy(update={"source_id": source_id, "evidence_ref": reference})

    def load(self, context: ToolContext, reference: EvidenceRef) -> str:
        if reference.scope_key != scope_key(context):
            raise DomainError("scope_violation", "Evidence belongs to another work or actor")
        base = self._root(context)
        relative = reference.scope_key + "/" + reference.source_id + ".txt"
        try:
            with open_parent(base, relative) as (parent, name):
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    metadata = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_size > self.max_source_bytes
                    ):
                        raise DomainError("research_evidence_invalid", "Saved evidence is invalid")
                    with os.fdopen(descriptor, "rb", closefd=False) as stream:
                        content = stream.read(self.max_source_bytes + 1)
                finally:
                    os.close(descriptor)
        except OSError:
            raise DomainError(
                "research_evidence_unavailable", "Saved evidence is unavailable"
            ) from None
        if (
            len(content) != reference.content_bytes
            or hashlib.sha256(content).hexdigest() != reference.content_sha256
        ):
            raise DomainError(
                "research_evidence_invalid", "Saved evidence no longer matches its receipt"
            )
        try:
            return content.decode("utf-8")
        except UnicodeError:
            raise DomainError("research_evidence_invalid", "Saved evidence is not UTF-8") from None

    def source_content(self, context: ToolContext, source: ResearchSource) -> str:
        reference = source.evidence_ref
        if reference is not None and reference.scope_key != scope_key(context):
            raise DomainError("scope_violation", "Evidence belongs to another work or actor")
        if (
            reference is None
            or source.source_id != reference.source_id
            or _snapshot_id(context, source, reference.content_sha256) != reference.source_id
        ):
            raise DomainError(
                "research_evidence_invalid", "Source identity does not match its evidence"
            )
        return self.load(context, reference)


def query_terms(question: str) -> frozenset[str]:
    """Identifiers and CJK bigrams; no network, model, or hidden intent classifier."""
    result = set(re.findall(r"[a-z0-9_][a-z0-9_.-]{1,}", question.casefold()))
    for phrase in re.findall(r"[\u3400-\u9fff]{2,}", question):
        result.update(phrase[index : index + 2] for index in range(len(phrase) - 1))
    return frozenset(result)


def relevance(text: str, terms: frozenset[str]) -> int:
    lowered = text.casefold()
    return sum(term in lowered for term in terms)


def utf8_prefix(text: str, limit: int) -> str:
    return text.encode("utf-8")[: max(0, limit)].decode("utf-8", errors="ignore")


def project_text(text: str, question: str, max_bytes: int) -> str:
    """Return exact source windows, preserving source order and explicit omissions."""
    if max_bytes <= 0:
        return ""
    if len(text.encode("utf-8")) <= max_bytes:
        return text
    terms = query_terms(question)
    chunks = [(position, text[position : position + 512]) for position in range(0, len(text), 512)]
    ranked = sorted(chunks, key=lambda item: (-relevance(item[1], terms), item[0]))
    selected: list[tuple[int, str]] = []
    remaining = max_bytes
    for position, chunk in ranked:
        separator = len(_OMISSION.encode()) if selected else 0
        allowance = remaining - separator
        if allowance <= 0:
            break
        if len(chunk.encode()) > allowance:
            matches = [chunk.casefold().find(term) for term in terms if term in chunk.casefold()]
            if matches:
                focus = min(matches)
                prefix_bytes = len(chunk[:focus].encode())
                raw = chunk.encode()
                start = max(0, prefix_bytes - allowance // 3)
                chunk = raw[start : start + allowance].decode("utf-8", errors="ignore")
                position += len(raw[:start].decode("utf-8", errors="ignore"))
            else:
                chunk = utf8_prefix(chunk, allowance)
        used = len(chunk.encode())
        if not used:
            continue
        selected.append((position, chunk))
        remaining -= used + separator
    return _OMISSION.join(chunk for _, chunk in sorted(selected))


def project_research_sources(
    sources: Iterable[Mapping[str, Any] | ResearchSource],
    question: str,
    answer: str = "",
    *,
    max_bytes: int = 12_000,
    load_content: Callable[[ResearchSource], str] | None = None,
) -> list[dict[str, Any]]:
    """The same source ranking/windowing used in generation and independent review."""
    query = question + " " + answer
    terms = query_terms(query)
    unique: dict[tuple[str, str], tuple[int, tuple[bool, bool], dict[str, Any], str]] = {}
    for item in sources:
        source = item.model_dump(mode="json") if isinstance(item, ResearchSource) else dict(item)
        content = source.get("content", "")
        if load_content is not None and source.get("evidence_ref"):
            content = load_content(ResearchSource.model_validate(source))
        url = source.get("url")
        if not isinstance(content, str) or not content or not isinstance(url, str):
            continue
        identity = (url, hashlib.sha256(content.encode()).hexdigest())
        strength = (
            source.get("operation") == "fetch",
            source.get("evidence_kind") == "provider_fetched_page",
        )
        previous = unique.get(identity)
        # A search observation and an actual page read can return identical
        # text. Deduplication must retain the stronger real observation, never
        # make provenance depend on which receipt happened to arrive first.
        if previous is None or strength > previous[1]:
            unique[identity] = (relevance(content, terms), strength, source, content)
    ranked = list(unique.values())
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    remaining = max_bytes
    budgets = []
    sizes = [len(content.encode()) for _, _, _, content in ranked]
    for index, size in enumerate(sizes):
        allowance = max(1, remaining // min(3, len(ranked) - index)) if remaining > 0 else 0
        used = min(size, allowance)
        budgets.append(used)
        remaining -= used
    # A short source keeps its complete evidence. Return its unused fair share
    # to the highest-ranked longer sources rather than silently wasting context.
    for index, size in enumerate(sizes):
        extra = min(size - budgets[index], max(0, remaining))
        budgets[index] += extra
        remaining -= extra
    evidence = []
    for (_, _, source, content), allowance in zip(ranked, budgets, strict=True):
        if allowance <= 0:
            continue
        excerpt = project_text(content, query, allowance)
        complete = excerpt == content
        evidence.append(
            {
                "source_id": source.get("source_id"),
                "url": source.get("url"),
                "citation": source.get("citation"),
                "operation": source.get("operation"),
                "evidence_kind": source.get("evidence_kind"),
                "received_content_excerpt": excerpt,
                "excerpt_complete_for_received_content": complete,
                "omitted_received_content": not complete,
                "source_truncated_fields": source.get("truncated_fields", []),
            }
        )
    return evidence
