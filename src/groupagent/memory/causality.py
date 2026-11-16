"""Causal order for controller-proven atomic TaskBatch parts.

Only this same-turn host derivative inherits its parent's order. Native quotes,
asynchronous tool callbacks and scheduled occurrences remain their own roots.
"""

from __future__ import annotations

import re

from groupagent.persistence import UnitOfWork


def causal_sequence_sql(alias: str) -> str:
    """Build SQL from a code-owned alias; no user text becomes SQL syntax."""
    if re.fullmatch(r"[a-z][a-z0-9_]*", alias) is None:
        raise ValueError("Expected a static SQL alias")
    e = alias
    part = f"{e}.body->'attachments'->0"
    return (
        f"CASE WHEN {e}.kind='quote' AND {e}.id={e}.source_key "
        f"AND {e}.source_key LIKE 'action-part:%%' AND {e}.body->>'text'='' "
        f"AND {e}.body->>'platform_message_id' IS NULL "
        f"AND jsonb_array_length({e}.body->'attachments')=1 "
        f"AND {part}->>'kind'='action_part' "
        f"AND jsonb_typeof({part}->'index')='number' AND {part}->>'index' ~ '^[0-9]$' "
        "THEN COALESCE((SELECT causal_parent.sequence FROM ga_events causal_parent "
        f"WHERE causal_parent.group_id={e}.group_id AND causal_parent.actor_id={e}.actor_id "
        f"AND causal_parent.id={e}.body->>'reply_to' "
        f"AND {part}->>'source_event_id'=causal_parent.id "
        f"AND {e}.id='action-part:'||encode(sha256(convert_to(causal_parent.id||':'||({part}->>'index'),'UTF8')),'hex') "
        "AND EXISTS (SELECT 1 FROM ga_decisions causal_decision "
        f"WHERE causal_decision.group_id={e}.group_id AND causal_decision.event_id={e}.id "
        f"AND causal_decision.actor_id={e}.actor_id "
        "AND causal_decision.body->'result'->'decision'->>'reason_code'='atomic_action_part' "
        f"AND causal_decision.body->'result'->>'event_id'={e}.id "
        "AND causal_decision.body->'source_event_ids'=jsonb_build_array(causal_parent.id))),"
        f"{e}.sequence) ELSE {e}.sequence END"
    )


def after_boundary_sql(alias: str) -> str:
    """Two cutoff parameters; ordinary earlier rows need no derivative lookup."""
    return f"(CASE WHEN {alias}.sequence>%s THEN ({causal_sequence_sql(alias)})>%s ELSE FALSE END)"


def causal_sources(tx: UnitOfWork, identifiers: set[str]) -> set[str]:
    """Expand only proven host parents so ordinary privacy checks cover them."""
    candidates = {value for value in identifiers if value.startswith("action-part:")}
    if not candidates:
        return set(identifiers)
    rows = tx.conn.execute(
        "SELECT e.body->>'reply_to' AS parent_id FROM ga_events e "
        "WHERE e.group_id=%s AND e.id=ANY(%s) AND (" + causal_sequence_sql("e") + ")<>e.sequence",
        (tx.group_id, sorted(candidates)),
    ).fetchall()
    return identifiers | {row["parent_id"] for row in rows}
