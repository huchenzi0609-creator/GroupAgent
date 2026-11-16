from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from groupagent.domain import ActorContext, DomainError, GroupEvent, Role
from groupagent.persistence import PostgresStore, UnitOfWork

from .causality import causal_sources

Row = dict[str, Any]


class BoundStore(PostgresStore):
    """A service view of an existing transaction; never opens or commits another one."""

    def __init__(self, tx: UnitOfWork) -> None:
        self.tx = tx

    def connect(self):
        raise DomainError("invalid_transaction", "A bound service cannot open another connection")

    @contextmanager
    def transaction(
        self, actor: ActorContext, *, trace_id: str | None = None
    ) -> Iterator[UnitOfWork]:
        if actor != self.tx.actor or self.tx.conn.closed:
            raise DomainError(
                "scope_violation", "Bound service requires its original active transaction"
            )
        yield self.tx


def memory_lock(tx: UnitOfWork) -> int:
    tx.conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("memory:" + tx.group_id,)
    )
    tx.conn.execute(
        "INSERT INTO ga_memory_epochs(group_id) VALUES (%s) ON CONFLICT DO NOTHING", (tx.group_id,)
    )
    row = tx.conn.execute(
        "SELECT epoch FROM ga_memory_epochs WHERE group_id=%s", (tx.group_id,)
    ).fetchone()
    assert row is not None
    return int(row["epoch"])


def technical(tx: UnitOfWork) -> None:
    if tx.actor.role not in (Role.OWNER, Role.MAINTAINER):
        raise DomainError("permission_denied", "A trusted maintenance identity is required")


def topic_row(tx: UnitOfWork, topic_id: str | None) -> Row | None:
    if topic_id is None:
        return None
    row = tx.conn.execute(
        "SELECT * FROM ga_memory_topics WHERE group_id=%s AND id=%s", (tx.group_id, topic_id)
    ).fetchone()
    if row is None:
        raise DomainError("not_found", "Topic was not found in this group")
    if row["visibility"] == "participants" and not (
        tx.actor.role == Role.OWNER
        or row["created_by"] == tx.actor.member_id
        or tx.actor.member_id in row["participant_ids"]
    ):
        raise DomainError("permission_denied", "Topic is outside this member's authorized scope")
    return row


def source_rows(
    tx: UnitOfWork,
    event_ids: list[str],
    *,
    shared: bool = False,
    allow_expired: bool = False,
    require_complete: bool = False,
) -> list[Row]:
    if not event_ids or len(set(event_ids)) != len(event_ids) or len(event_ids) > 500:
        raise DomainError("invalid_input", "One to 500 distinct source events are required")
    rows = tx.conn.execute(
        "SELECT * FROM ga_events WHERE group_id=%s AND id=ANY(%s) ORDER BY sequence",
        (tx.group_id, event_ids),
    ).fetchall()
    if len(rows) != len(event_ids):
        raise DomainError("not_found", "A source event was not found in this group")
    parents = causal_sources(tx, set(event_ids)) - set(event_ids)
    if parents:
        source_rows(
            tx,
            sorted(parents),
            shared=shared,
            allow_expired=allow_expired,
            require_complete=require_complete,
        )
    for row in rows:
        if tx.get_event(row["id"]).content_state not in (
            {"active", "expired"} if allow_expired else {"active"}
        ):
            raise DomainError("source_unavailable", "Canonical source was withdrawn or expired")
        if row["memory_state"] != "active" and not (
            allow_expired and row["memory_state"] == "expired"
        ):
            raise DomainError("source_unavailable", "A source was forgotten, recalled or expired")
        if require_complete:
            canonical = tx._sanitize_quote(GroupEvent.model_validate(row["body"]))
            if any(
                attachment.get("privacy_redacted")
                and not (allow_expired and attachment.get("reason") == "expired")
                for attachment in canonical.attachments
            ):
                raise DomainError(
                    "source_unavailable",
                    "A quoted source was removed; provide fresh independent evidence",
                )
        topics = tx.conn.execute(
            "SELECT t.* FROM ga_memory_topics t JOIN ga_topic_events e "
            "ON t.group_id=e.group_id AND t.id=e.topic_id WHERE e.group_id=%s AND e.event_id=%s",
            (tx.group_id, row["id"]),
            # A generic plan badly underestimates large shared groups and may
            # rescan event membership once per topic. Keep this lookup custom;
            # the relationship, authorization checks and other queries are unchanged.
            prepare=False,
        ).fetchall()
        for topic in topics:
            if topic["visibility"] == "participants":
                if shared:
                    raise DomainError(
                        "scope_violation", "Restricted sources cannot become shared memory"
                    )
                topic_row(tx, topic["id"])
    return rows


def invalidate_summaries(tx: UnitOfWork, event_ids: list[str], *, forgotten: bool = False) -> int:
    affected = tx.conn.execute(
        "SELECT DISTINCT s.topic_id, s.first_sequence FROM ga_memory_summaries s "
        "JOIN ga_summary_sources r ON r.group_id=s.group_id AND r.summary_id=s.id "
        "WHERE r.group_id=%s AND r.event_id=ANY(%s) AND s.state='active'",
        (tx.group_id, event_ids),
    ).fetchall()
    count = 0
    for row in affected:
        key = row["topic_id"] or "__group__"
        result = tx.conn.execute(
            "UPDATE ga_memory_summaries SET text='', state=%s WHERE group_id=%s "
            "AND topic_id IS NOT DISTINCT FROM %s AND last_sequence >= %s AND state='active'",
            (
                "forgotten" if forgotten else "invalidated",
                tx.group_id,
                row["topic_id"],
                row["first_sequence"],
            ),
        )
        count += result.rowcount
        tx.conn.execute(
            "UPDATE ga_memory_checkpoints SET through_sequence=LEAST(through_sequence,%s), "
            "revision=revision+1, lease_token=NULL, lease_until=NULL WHERE group_id=%s AND topic_key=%s",
            (row["first_sequence"] - 1, tx.group_id, key),
        )
    tx.conn.execute("UPDATE ga_memory_epochs SET epoch=epoch+1 WHERE group_id=%s", (tx.group_id,))
    return count
