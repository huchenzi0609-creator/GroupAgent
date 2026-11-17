"""Privacy of frozen transport text without changing delivery receipt identity."""

from __future__ import annotations

REMOVED_SOURCE_REASONS = ("source_expired", "source_recalled", "source_forgotten")


def has_rendered_text(conn) -> bool:
    """The candidate maintenance reader also supports a pre-migration archive."""
    row = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_attribute "
        "WHERE attrelid=to_regclass('ga_delivery_parts') AND attname='rendered_text' "
        "AND NOT attisdropped) AS present"
    ).fetchone()
    return bool(row and row["present"])


def redact_removed_text(conn, group_id: str) -> int:
    if not has_rendered_text(conn):
        return 0
    result = conn.execute(
        "UPDATE ga_delivery_parts p SET rendered_text=NULL FROM ga_deliveries d "
        "WHERE p.group_id=%s AND d.group_id=p.group_id AND d.id=p.intent_id "
        "AND d.reason=ANY(%s) AND p.rendered_text IS NOT NULL",
        (group_id, list(REMOVED_SOURCE_REASONS)),
    )
    return result.rowcount


def removed_text_is_redacted(conn, group_id: str) -> bool:
    if not has_rendered_text(conn):
        return True
    return (
        conn.execute(
            "SELECT 1 FROM ga_delivery_parts p JOIN ga_deliveries d "
            "ON d.group_id=p.group_id AND d.id=p.intent_id "
            "WHERE p.group_id=%s AND d.reason=ANY(%s) "
            "AND p.rendered_text IS NOT NULL LIMIT 1",
            (group_id, list(REMOVED_SOURCE_REASONS)),
        ).fetchone()
        is None
    )
