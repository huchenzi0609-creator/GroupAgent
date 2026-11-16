"""Project durable native-part evidence without changing send/reconcile identity."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from groupagent.domain import DeliveryState


def projected_delivery_state(
    state: DeliveryState | str, parts: Iterable[Mapping[str, object]]
) -> DeliveryState:
    """A known running native job is still dispatching, never accepted.

    UNKNOWN remains the durable parent state used by receipt reconciliation.
    Only its work-supervision meaning changes when every part is accounted for
    and a submitted part has a native job, or all parts have real acceptances.
    """
    state = DeliveryState(state)
    if state != DeliveryState.UNKNOWN:
        return state
    native_job_running = False
    all_accepted = True
    seen = False
    for part in parts:
        seen = True
        if part.get("state") == "accepted":
            message = part.get("platform_message_id")
            if not isinstance(message, str) or not message.strip():
                return DeliveryState.UNKNOWN
            continue
        all_accepted = False
        if part.get("state") == "pending":
            continue
        job = part.get("platform_job_id")
        if part.get("state") == "submitted" and isinstance(job, str) and job.strip():
            native_job_running = True
            continue
        # Includes needs-review/unknown, a mixed rejection, or a submission
        # without positive native evidence. Never infer safety from a parent ID.
        return DeliveryState.UNKNOWN
    return (
        DeliveryState.DISPATCHING
        if native_job_running or (seen and all_accepted)
        else DeliveryState.UNKNOWN
    )
