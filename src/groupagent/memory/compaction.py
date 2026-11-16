from __future__ import annotations

import json
from hashlib import sha256
from uuid import uuid4

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory.common import BoundStore, Row, memory_lock, source_rows, topic_row
from groupagent.memory.models import MemorySummary, SummaryInput
from groupagent.memory.retrieval import visible_events
from groupagent.memory.summarizers import InvalidSummaryError, Summarizer
from groupagent.persistence import PostgresStore


class CompactionMethods:
    store: PostgresStore
    summarizer: Summarizer | None

    def checkpoint(self, actor: ActorContext, *, topic_id: str | None = None) -> dict:
        with self.store.transaction(actor) as tx:
            topic_row(tx, topic_id)
            row = tx.conn.execute(
                "SELECT through_sequence,revision FROM ga_memory_checkpoints WHERE group_id=%s AND topic_key=%s",
                (actor.group_id, topic_id or "__group__"),
            ).fetchone()
            return dict(row) if row else {"through_sequence": 0, "revision": 0}

    async def compact(
        self,
        actor: ActorContext,
        *,
        topic_id: str | None = None,
        token_budget: int = 4000,
        trigger_ratio: float = 0.75,
        max_source_events: int = 200,
        force: bool = False,
        lease_seconds: int = 60,
    ) -> MemorySummary | None:
        if isinstance(self.store, BoundStore):
            raise DomainError(
                "invalid_transaction", "Summarization must run outside the caller's transaction"
            )
        if actor.role == Role.OBSERVER:
            raise DomainError(
                "permission_denied", "Observers cannot initiate paid memory compaction"
            )
        if (
            not 100 <= token_budget <= 64000
            or not 0 < trigger_ratio <= 1
            or not 1 <= max_source_events <= 500
            or not 1 <= lease_seconds <= 300
        ):
            raise DomainError("invalid_input", "Compaction budget or lease is invalid")
        if self.summarizer is None:
            raise DomainError("provider_unavailable", "An explicit summarizer provider is required")
        # Providers that must echo source coverage reserve output space for it.
        # Remaining sources stay behind the same atomic checkpoint for a later pass.
        provider_limit = getattr(self.summarizer, "max_source_events", max_source_events)
        if type(provider_limit) is int and provider_limit > 0:
            max_source_events = min(max_source_events, provider_limit)
        token = uuid4().hex
        key = topic_id or "__group__"
        failure_ref = "memory-compaction:" + sha256(key.encode()).hexdigest()
        with self.store.transaction(actor) as tx:
            epoch = memory_lock(tx)
            topic = topic_row(tx, topic_id)
            tx.conn.execute(
                "INSERT INTO ga_memory_checkpoints(group_id,topic_key) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (actor.group_id, key),
            )
            checkpoint = tx.conn.execute(
                "SELECT *,clock_timestamp() AS now FROM ga_memory_checkpoints WHERE group_id=%s AND topic_key=%s FOR UPDATE",
                (actor.group_id, key),
            ).fetchone()
            assert checkpoint is not None
            if checkpoint["lease_until"] and checkpoint["lease_until"] > checkpoint["now"]:
                raise DomainError(
                    "lease_busy", "Another compactor owns this source window", retryable=True
                )
            rows = visible_events(
                tx,
                topic_id=topic_id,
                after_sequence=checkpoint["through_sequence"],
                limit=max_source_events + 1,
                public_only=topic is None or topic["visibility"] == "group",
            )
            if not rows:
                return None

            def estimate(row: Row) -> int:
                return max(1, (len(row["body"].get("text", "").encode("utf-8")) + 2) // 3) + 24

            total = sum(estimate(row) for row in rows)
            if (
                not force
                and total < token_budget * trigger_ratio
                and len(rows) <= max_source_events
            ):
                return None
            selected: list[Row] = []
            used = 0
            for row in rows[:max_source_events]:
                cost = estimate(row)
                if used + cost > token_budget:
                    if not selected:
                        raise DomainError(
                            "context_too_large",
                            "A source message exceeds this compaction budget; use a larger permitted context",
                        )
                    break
                selected.append(row)
                used += cost
            source_ids = [row["id"] for row in selected]
            request = SummaryInput(
                group_id=actor.group_id,
                topic_id=topic_id,
                estimated_tokens=used,
                events=[
                    {
                        "id": row["id"],
                        "actor_id": row["actor_id"],
                        "kind": row["kind"],
                        "occurred_at": row["occurred_at"].isoformat(),
                        "text": row["body"].get("text", ""),
                    }
                    for row in selected
                ],
            )
            # Hash-only retry state uses the existing audit journal; no raw
            # messages or rejected model output are persisted as diagnostics.
            window_hash = sha256(
                json.dumps(
                    {
                        "request": request.model_dump(mode="json"),
                        "epoch": epoch,
                        "through_sequence": checkpoint["through_sequence"],
                        "provider": type(self.summarizer).__qualname__,
                        "configuration": getattr(self.summarizer, "configuration_key", ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            last_failure = tx.conn.execute(
                "SELECT details,created_at,clock_timestamp() AS now FROM ga_audit_events "
                "WHERE group_id=%s AND action='memory_compaction_failed' AND resource_ref=%s "
                "AND details->>'window_hash'=%s "
                "AND created_at>clock_timestamp()-interval '2 hours' "
                "ORDER BY created_at DESC,id DESC LIMIT 1",
                (actor.group_id, failure_ref, window_hash),
            ).fetchone()
            failures = 0
            if last_failure:
                failures = int(last_failure["details"]["consecutive_failures"])
                elapsed = (last_failure["now"] - last_failure["created_at"]).total_seconds()
                if elapsed < last_failure["details"]["retry_after_seconds"]:
                    # force bypasses the source-size trigger, never this cost
                    # control. A changed source window/provider can retry now.
                    return None
            tx.conn.execute(
                "UPDATE ga_memory_checkpoints SET lease_token=%s,lease_until=clock_timestamp()+%s*interval '1 second' WHERE group_id=%s AND topic_key=%s",
                (token, lease_seconds, actor.group_id, key),
            )
        try:
            draft = await self.summarizer.summarize(request)
            if not draft.text.strip():
                raise InvalidSummaryError("invalid_schema")
            if len(draft.covered_event_ids) != len(source_ids) or set(
                draft.covered_event_ids
            ) != set(source_ids):
                raise InvalidSummaryError("coverage_mismatch")
            with self.store.transaction(actor) as tx:
                current_epoch = memory_lock(tx)
                current = tx.conn.execute(
                    "SELECT *,clock_timestamp() AS now FROM ga_memory_checkpoints WHERE group_id=%s AND topic_key=%s FOR UPDATE",
                    (actor.group_id, key),
                ).fetchone()
                if (
                    current is None
                    or current["lease_token"] != token
                    or current["lease_until"] <= current["now"]
                ):
                    raise DomainError("stale_lease", "Compaction lease expired or was invalidated")
                if (
                    current_epoch != epoch
                    or current["through_sequence"] != checkpoint["through_sequence"]
                ):
                    raise DomainError(
                        "memory_changed", "Memory changed while the summary provider was running"
                    )
                source_rows(tx, source_ids, shared=topic is None or topic["visibility"] == "group")
                summary = tx.conn.execute(
                    "INSERT INTO ga_memory_summaries(id,group_id,topic_id,text,source_event_ids,first_sequence,last_sequence,provider,model,usage,created_by) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        uuid4().hex,
                        actor.group_id,
                        topic_id,
                        draft.text,
                        Jsonb(source_ids),
                        selected[0]["sequence"],
                        selected[-1]["sequence"],
                        draft.provider,
                        draft.model,
                        Jsonb(draft.usage),
                        actor.member_id,
                    ),
                ).fetchone()
                assert summary is not None
                for source_id in source_ids:
                    tx.conn.execute(
                        "INSERT INTO ga_summary_sources(group_id,summary_id,event_id) VALUES (%s,%s,%s)",
                        (actor.group_id, summary["id"], source_id),
                    )
                tx.conn.execute(
                    "UPDATE ga_memory_checkpoints SET through_sequence=%s,revision=revision+1,lease_token=NULL,lease_until=NULL WHERE group_id=%s AND topic_key=%s",
                    (selected[-1]["sequence"], actor.group_id, key),
                )
                tx._audit(
                    "memory_compacted",
                    summary["id"],
                    source_event_ids=source_ids,
                    provider=draft.provider,
                    model=draft.model,
                    usage=json.loads(draft.model_dump_json())["usage"],
                )
                return MemorySummary.model_validate(summary)
        except BaseException as error:
            # Cancellation and provider failure cannot acknowledge source input.
            # A concurrent forget may already have revoked the token.
            try:
                with self.store.transaction(actor) as tx:
                    memory_lock(tx)
                    released = tx.conn.execute(
                        "UPDATE ga_memory_checkpoints SET lease_token=NULL,lease_until=NULL WHERE group_id=%s AND topic_key=%s AND lease_token=%s RETURNING topic_key",
                        (actor.group_id, key, token),
                    ).fetchone()
                    if (
                        released
                        and isinstance(error, DomainError)
                        and error.code == "invalid_summary"
                    ):
                        attempts = min(failures + 1, 3)
                        details = {
                            "window_hash": window_hash,
                            "consecutive_failures": attempts,
                            "retry_after_seconds": min(900 * 2 ** (attempts - 1), 3600),
                            "source_count": len(source_ids),
                            "estimated_input_tokens": request.estimated_tokens,
                            "reason": (
                                error.reason
                                if isinstance(error, InvalidSummaryError)
                                else error.code
                            ),
                        }
                        if isinstance(error, InvalidSummaryError):
                            details["finish_reason"] = error.finish_reason
                            details["usage"] = error.usage
                        output_capacity = getattr(self.summarizer, "max_output_tokens", None)
                        if type(output_capacity) is int:
                            details["max_output_tokens"] = output_capacity
                        tx._audit("memory_compaction_failed", failure_ref, **details)
            except DomainError:
                pass  # Revocation still prevents a stale commit; the bounded lease expires.
            raise
