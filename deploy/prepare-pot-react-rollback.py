#!/usr/bin/env python3
"""Build a public-source compatibility overlay for the reviewed pre-ReAct image."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

BASE_COMMIT = "6dcac4ab3d5cfd9c415bc9b071a4c62b5140eb0d"
BASE_PYTHON_SHA256 = "bee867288ed755495603b616876172ac8388aaff6c0fe5e77ed0480e5184f017"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def replace(source: str, old: str, new: str, *, count: int = 1) -> str:
    if source.count(old) != count:
        raise ValueError("Reviewed rollback rewrite anchor changed")
    return source.replace(old, new)


def scheduler(source: str) -> str:
    # Keep old strict validation after projecting the known additive columns.
    source = source.replace("Schedule.model_validate(", "_legacy_schedule(")
    source = source.replace("ScheduleOccurrence.model_validate(", "_legacy_occurrence(")
    source = replace(
        source,
        "class Scheduler:\n",
        """def _legacy_schedule(row: Row) -> Schedule:
    return Schedule.model_validate({key: value for key, value in row.items() if key != "execution_kind"})


def _legacy_occurrence(row: Row) -> ScheduleOccurrence:
    return ScheduleOccurrence.model_validate({
        key: value for key, value in row.items()
        if key not in {"delivery_id", "delivery_purpose", "delivery_sources"}
    })


def _requires_react(tx: UnitOfWork, row: Row) -> bool:
    if row.get("execution_kind", "goal") != "goal" or row["scope"].get("allow_silent_result") is True:
        return True
    return tx.conn.execute(
        "SELECT 1 FROM ga_schedule_occurrences o WHERE o.group_id=%s AND o.schedule_id=%s "
        "AND to_jsonb(o)->>'delivery_id' IS NOT NULL LIMIT 1",
        (tx.group_id, row["id"]),
    ).fetchone() is not None


class Scheduler:
""",
    )
    source = replace(
        source,
        '            tx._controller(row, "creator_id")\n'
        "            tx._revision(row, expected_revision)\n",
        '            tx._controller(row, "creator_id")\n'
        "            tx._revision(row, expected_revision)\n"
        "            if _requires_react(tx, row):\n"
        '                raise DomainError("feature_disabled", "This rule requires the current scheduler")\n',
        count=3,
    )
    return replace(
        source,
        '                "SELECT s.* FROM ga_schedules s WHERE s.group_id = %s "\n',
        '                "SELECT s.* FROM ga_schedules s WHERE s.group_id = %s "\n'
        """                "AND COALESCE(to_jsonb(s)->>'execution_kind','goal')='goal' "
                "AND s.scope->'allow_silent_result' IS DISTINCT FROM 'true'::jsonb "
                "AND NOT EXISTS (SELECT 1 FROM ga_schedule_occurrences sealed "
                "WHERE sealed.group_id=s.group_id AND sealed.schedule_id=s.id "
                "AND to_jsonb(sealed)->>'delivery_id' IS NOT NULL) "
""",
    )


def exclude_react_intents(source: str, anchor: str, column: str, *, indent: int) -> str:
    prefix = " " * indent
    return replace(
        source,
        anchor,
        anchor
        + f"""{prefix}"AND NOT starts_with({column},'schedule-occurrence:') "\n"""
        + f"""{prefix}"AND NOT starts_with({column},'react-observer:') "\n""",
    )


def dispatcher(source: str) -> str:
    return exclude_react_intents(
        source,
        """                "WHERE d.group_id=%s AND d.state IN ('pending','dispatching','unknown') "\n""",
        "d.idempotency_key",
        indent=16,
    )


def persistence(source: str) -> str:
    for statement in (
        "SELECT id, work_id FROM ga_deliveries WHERE group_id = %s AND state = 'pending' ",
        "SELECT id FROM ga_deliveries WHERE group_id = %s AND state = 'dispatching' ",
    ):
        source = exclude_react_intents(
            source, f'            "{statement}"\n', "idempotency_key", indent=12
        )
    return replace(
        source,
        '        work_id = row["id"]\n        self._revoke(work_id, state="expired")\n',
        '        if row["scope"].get("allow_silent_result") is True:\n'
        '            raise DomainError("feature_disabled", "This work requires the current scheduler")\n'
        '        work_id = row["id"]\n        self._revoke(work_id, state="expired")\n',
    )


def notices(source: str) -> str:
    return replace(
        source,
        """                "('waiting_input','paused','failed') ORDER BY updated_at LIMIT 100",\n""",
        """                "('waiting_input','paused','failed') "
                "AND scope->'allow_silent_result' IS DISTINCT FROM 'true'::jsonb "
                "ORDER BY updated_at LIMIT 100",
""",
    )


def privacy(source: str) -> str:
    source = replace(
        source,
        "class ForgetMethods:\n",
        """def _rollback_bound_deliveries(
    tx: UnitOfWork, event_ids: list[str], *, include_prepared: bool
) -> list[str]:
    if not event_ids:
        return []
    result = []
    if include_prepared:
        result.extend(row["delivery_id"] for row in tx.conn.execute(
            "SELECT to_jsonb(o)->>'delivery_id' AS delivery_id FROM ga_schedule_occurrences o "
            "WHERE o.group_id=%s AND to_jsonb(o)->>'delivery_id' IS NOT NULL "
            "AND to_jsonb(o)->'delivery_sources' ?| %s::text[]",
            (tx.group_id, event_ids),
        ).fetchall())
    feature = tx.conn.execute(
        "SELECT to_regclass('ga_react_observations') IS NOT NULL AS present"
    ).fetchone()
    if feature and feature["present"]:
        result.extend(row["delivery_id"] for row in tx.conn.execute(
            "SELECT delivery_id FROM ga_react_observations WHERE group_id=%s "
            "AND delivery_id IS NOT NULL AND source_fingerprints ?| %s::text[]",
            (tx.group_id, event_ids),
        ).fetchall())
    return result


class ForgetMethods:
""",
    )
    source = replace(
        source,
        "            deliveries = tx.conn.execute(\n",
        """            notice_ids.extend(_rollback_bound_deliveries(
                tx, sorted(changed), include_prepared=include_human_requests
            ))
            deliveries = tx.conn.execute(
""",
    )
    return replace(
        source,
        "            tx.conn.execute(\n"
        """                "UPDATE ga_deliveries SET state=CASE WHEN state='pending' THEN 'rejected' ELSE state END, "\n""",
        """            outgoing_sources = sorted(
                set(ids) | set(referenced_event_ids or ()) | {quote["id"] for quote in quotes}
            )
            notice_ids.extend(_rollback_bound_deliveries(
                tx, outgoing_sources, include_prepared=forget_facts
            ))
            tx.conn.execute(
"""
        """                "UPDATE ga_deliveries SET state=CASE WHEN state='pending' THEN 'rejected' ELSE state END, "\n""",
    )


PATCHES = {
    "memory/forget.py": privacy,
    "persistence/postgres.py": persistence,
    "runtime/dispatch.py": dispatcher,
    "runtime/notices.py": notices,
    "scheduling/service.py": scheduler,
}


def prepare(package: Path, overlay: Path) -> dict:
    if overlay.exists() or overlay.is_symlink() or package.resolve() in overlay.resolve().parents:
        raise ValueError("The rollback overlay needs a new directory outside the base package")
    originals = {}
    for path in sorted(package.rglob("*.py")):
        if not path.is_file() or path.is_symlink():
            raise ValueError("The reviewed base package must contain regular Python sources")
        originals[path.relative_to(package).as_posix()] = path.read_bytes()
    manifest = {f"src/groupagent/{name}": digest(data) for name, data in originals.items()}
    base_hash = digest(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    if base_hash != BASE_PYTHON_SHA256:
        raise ValueError("Prior package does not match the reviewed Pot rollback base")

    prepared = {}
    for name, patch in PATCHES.items():
        candidate = patch(originals[name].decode("utf-8"))
        compile(candidate, name, "exec")
        prepared[name] = candidate.encode("utf-8")

    overlay.mkdir(mode=0o755)
    for name, data in prepared.items():
        path = overlay / "groupagent" / name
        path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        path.parent.parent.chmod(0o755)
        path.parent.chmod(0o755)
        path.write_bytes(data)
        path.chmod(0o644)
    return {
        "base_commit": BASE_COMMIT,
        "base_python_sha256": base_hash,
        "overlay": {
            name: {"before": digest(originals[name]), "after": digest(data)}
            for name, data in prepared.items()
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_package", type=Path)
    parser.add_argument("overlay", type=Path)
    options = parser.parse_args()
    try:
        evidence = prepare(options.base_package, options.overlay)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Rollback overlay rejected: {error}\n")
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
