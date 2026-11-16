"""Trusted test_2 operator: database status and one idempotent welcome outbox item."""

import argparse
import hashlib
import json
from pathlib import Path

from groupagent.billing import BudgetLedger
from groupagent.config import AppConfig
from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "welcome"))
    parser.add_argument("--config", type=Path, default=Path("/etc/pot-test2/config.json"))
    parser.add_argument("--dsn", type=Path, default=Path("/etc/pot-test2/database.dsn"))
    args = parser.parse_args()
    config = AppConfig.model_validate_json(args.config.read_text())
    store = PostgresStore(args.dsn.read_text().strip())
    actor = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=config.group.policy_revision,
    )
    with store.transaction(actor) as tx:
        if args.action == "welcome":
            text = (
                "Pot 测试实例已就绪。接下来会在本群进行少量功能验收，测试文件和分工均仅用于验收。"
            )
            tx.enqueue_delivery(
                None,
                "text:" + text,
                hashlib.sha256(text.encode()).hexdigest(),
                "server-test2-20261116:welcome",
            )
        works = tx.conn.execute(
            "SELECT state,count(*) n FROM ga_work_orders WHERE group_id=%s GROUP BY state",
            (actor.group_id,),
        ).fetchall()
        deliveries = tx.conn.execute(
            "SELECT state,count(*) n FROM ga_deliveries WHERE group_id=%s GROUP BY state",
            (actor.group_id,),
        ).fetchall()
        events = tx.conn.execute(
            "SELECT count(*) n FROM ga_events WHERE group_id=%s", (actor.group_id,)
        ).fetchone()["n"]
        decisions = tx.conn.execute(
            "SELECT count(*) n FROM ga_decisions WHERE group_id=%s", (actor.group_id,)
        ).fetchone()["n"]
        starts = tx.conn.execute(
            "SELECT count(*) n FROM ga_audit_events WHERE group_id=%s AND action='runtime_started'",
            (actor.group_id,),
        ).fetchone()["n"]
    budget = BudgetLedger(store).report(actor)
    print(
        json.dumps(
            {
                "work_states": works,
                "delivery_states": deliveries,
                "events": events,
                "decisions": decisions,
                "runtime_starts": starts,
                "budget_upper_cny": budget["accounting"]["total"],
                "budget_held_cny": budget["accounting"]["held"],
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
