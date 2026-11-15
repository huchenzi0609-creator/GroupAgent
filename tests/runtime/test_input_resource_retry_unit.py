"""Conservative attempt bookkeeping at an explicit in-memory transaction boundary."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from groupagent.runtime.inputs import InputProcessor


class CacheBoundary:
    def __init__(self, body, reservation_id=None):
        self.body, self.reservation_id = body, reservation_id
        self.writes = []

    @contextmanager
    def transaction(self, actor):
        yield SimpleNamespace(conn=self)

    def execute(self, query, parameters):
        if "FROM ga_media_inputs" in query:
            value = {"body": self.body} if self.body is not None else None
        elif "FROM ga_budget_reservations" in query:
            value = {"id": self.reservation_id} if self.reservation_id else None
        else:
            raise AssertionError("Unexpected transaction boundary")
        return SimpleNamespace(fetchone=lambda: value)

    def put(self, tx, event, attachment, body):
        self.body = body
        self.writes.append(body)


def processor(monkeypatch, cache):
    value = InputProcessor.__new__(InputProcessor)
    value.store, value._put = cache, cache.put
    monkeypatch.setattr("groupagent.runtime.inputs.memory_lock", lambda tx: None)
    return value


def failed(value, *, attempt=2):
    return value._failed(
        SimpleNamespace(group_id="synthetic"),
        SimpleNamespace(id="source"),
        "attachment",
        "claim",
        "resource_deferred",
        unstarted_attempt=(attempt, "synthetic-attempt-" + str(attempt)),
    )


def test_unpaid_refund_preserves_prior_attempt_and_cannot_refund_twice(monkeypatch):
    cache = CacheBoundary({"claim": "claim", "attempts": 2, "reference": {"cached": True}})
    value = processor(monkeypatch, cache)
    assert failed(value) is None
    assert cache.body["attempts"] == 1 and cache.body["reference"] == {"cached": True}
    assert cache.body["error_code"] == "resource_deferred" and cache.body["claim_until"] == 0
    assert failed(value) is None and cache.body["attempts"] == 1


def test_any_durable_reservation_prevents_refund_and_requests_reconciliation(monkeypatch):
    cache = CacheBoundary({"claim": "claim", "attempts": 2}, "durable-reservation")
    assert failed(processor(monkeypatch, cache)) == "durable-reservation"
    assert cache.body["attempts"] == 2
    assert cache.body["error_code"] == "provider_reconciliation_required"


@pytest.mark.parametrize("body", [None, {"claim": "newer-claim", "attempts": 2}])
def test_deferral_never_restores_invalidated_cache_or_overwrites_a_new_claim(monkeypatch, body):
    cache = CacheBoundary(body)
    assert failed(processor(monkeypatch, cache)) is None
    assert cache.writes == [] and cache.body == body


def test_deferral_cannot_refund_a_different_attempt_number(monkeypatch):
    cache = CacheBoundary({"claim": "claim", "attempts": 1})
    assert failed(processor(monkeypatch, cache)) is None
    assert cache.body["attempts"] == 1
