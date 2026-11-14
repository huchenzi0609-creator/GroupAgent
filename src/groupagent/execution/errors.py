"""Provider admission failures with an explicitly safe automatic retry boundary."""

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import isfinite

from groupagent.domain import DomainError


class ModelRejected(DomainError):
    """The provider confirmed rejection (currently HTTP 429 only), not a lost reply."""

    def __init__(self, retry_after_seconds: float | None = None):
        super().__init__("rate_limited", "The model provider rejected this request", retryable=True)
        self.retry_after_seconds = retry_after_seconds


def retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            stamp = parsedate_to_datetime(value)
            if stamp.tzinfo is None:
                return None
            seconds = (stamp - datetime.now(UTC)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0, seconds) if isfinite(seconds) else None
