"""Durable payload identity shared by admission and dispatch."""

import hashlib

from groupagent.domain import DeliveryIntent, DomainError
from groupagent.persistence import UnitOfWork

DeliveryPayload = tuple[str, str, bytes | None]


def delivery_part_hash(payload: DeliveryPayload) -> str:
    kind, value, data = payload
    return hashlib.sha256(
        kind.encode() + b"\0" + value.encode() + b"\0" + (data or b"")
    ).hexdigest()


def freeze_delivery_parts(
    tx: UnitOfWork, intent: DeliveryIntent, payloads: tuple[DeliveryPayload, ...]
) -> None:
    """Bind exact rendered bytes to an already authorized pending intent."""
    part_key = "part:" + hashlib.sha256(intent.idempotency_key.encode()).hexdigest() + ":"
    for index, payload in enumerate(payloads):
        digest = delivery_part_hash(payload)
        tx.conn.execute(
            "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash,rendered_text) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                tx.group_id,
                intent.id,
                index,
                part_key + str(index),
                digest,
                payload[1] if payload[0] == "text" else None,
            ),
        )
        stored = tx.conn.execute(
            "SELECT payload_hash,rendered_text FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s AND position=%s",
            (tx.group_id, intent.id, index),
        ).fetchone()
        if (
            stored is None
            or stored["payload_hash"] != digest
            or (stored["rendered_text"] is not None and stored["rendered_text"] != payload[1])
        ):
            raise DomainError("idempotency_conflict", "Outgoing segment contents changed")
    row = tx.conn.execute(
        "SELECT count(*) AS n FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s",
        (tx.group_id, intent.id),
    ).fetchone()
    if row is None or row["n"] != len(payloads):
        raise DomainError("idempotency_conflict", "Outgoing segment count changed")
