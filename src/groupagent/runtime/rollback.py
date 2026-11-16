"""Trusted maintenance guard before restoring a reader without continuous-state support."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from groupagent.config import AppConfig
from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore


def park_for_legacy_rollback(store: PostgresStore, actor: ActorContext) -> int:
    """Preserve incompatible checkpoints; an older executable must not resume them."""
    if actor.role != Role.OWNER:
        raise ValueError("Only trusted owner maintenance may prepare a release rollback")
    count = 0
    with store.transaction(actor) as tx:
        rows = tx.conn.execute(
            "SELECT id,state,pause_reason,scope FROM ga_work_orders WHERE group_id=%s "
            "AND state IN ('queued','running','validating','delivering','paused') FOR UPDATE",
            (actor.group_id,),
        ).fetchall()
        for row in rows:
            if row["state"] == "paused" and row["pause_reason"] in {
                "user_request",
                "group_paused",
                "legacy_release_rollback",
            }:
                continue
            scope = dict(row["scope"])
            scope["_runtime_release_rollback"] = {
                "state": row["state"],
                "reason": row["pause_reason"],
                "parked_at": datetime.now(UTC).isoformat(),
            }
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='paused',pause_reason='legacy_release_rollback',"
                "scope=%s,revision=revision+1,lease_owner=NULL,lease_expires_at=NULL "
                "WHERE group_id=%s AND id=%s",
                (Jsonb(scope), actor.group_id, row["id"]),
            )
            tx.conn.execute(
                "UPDATE ga_execution_attempts SET state='stopped' "
                "WHERE group_id=%s AND work_id=%s AND state='active'",
                (actor.group_id, row["id"]),
            )
            tx._audit(
                "legacy_release_rollback_parked",
                row["id"],
                checkpoint_preserved=True,
                usage_and_fees_preserved=True,
            )
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--confirmed-quiesced", action="store_true", required=True)
    args = parser.parse_args()
    config = AppConfig.model_validate_json(args.config.read_text())
    dsn = args.dsn_file.read_text().strip()
    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT policy_revision FROM ga_groups WHERE id=%s", (config.group.internal_id,)
        ).fetchone()
    if row is None:
        raise ValueError("Configured group does not exist")
    actor = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=row[0],
    )
    count = park_for_legacy_rollback(PostgresStore(dsn), actor)
    print(f"POT_ROLLBACK_WORK_PARKED={count}")


if __name__ == "__main__":
    main()
