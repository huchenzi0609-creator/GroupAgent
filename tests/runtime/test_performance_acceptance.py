"""Performance instrumentation checks; deterministic fixture is not live model latency."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from groupagent.config import AppConfig, GroupConfig

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def performance(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "performance_acceptance_test", ROOT / "scripts/local_performance_acceptance.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_p95_is_nearest_rank_and_missing_or_failures_do_not_disappear(performance):
    assert performance.p95(list(range(1, 31))) == 29
    assert performance.p95([]) is None
    rows = [
        {"kind": "cancel_fence", "status": "PASS", "latency_upper_seconds": 0.1} for _ in range(30)
    ]
    assert performance.summarize(rows)["cancel_fence"]["status"] == "PASS"
    rows[0].update(status="TIMEOUT", latency_upper_seconds=None)
    result = performance.summarize(rows)["cancel_fence"]
    assert result["samples"] == 30 and result["status"] == "FAIL"
    assert result["p95_conservative_upper_seconds"] is None
    assert performance.summarize(rows[:29])["cancel_fence"]["status"] == "NOT_RUN"


@pytest.mark.postgres
async def test_real_daemon_local_db_cancel_rejects_old_tool_lease(
    runtime_env, performance, tmp_path
):
    env = runtime_env
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=env.actor.group_id,
            external_group_id=env.config.external_group_id,
            owner_ids=[env.actor.member_id],
        ),
        runtime={
            "channel": "local",
            "channel_account_id": "performance:offline-fixture",
            "poll_seconds": 0.01,
        },
        conversation={"idle_batch_seconds": 1, "max_batch_wait_seconds": 1},
    )
    root = tmp_path / "performance"
    root.mkdir()
    app = performance.deterministic_app(env.store, config, root)
    performance.ensure_idle(env.store, env.actor)
    samples = await performance.sample_app(
        app,
        env.actor,
        root,
        mode="cancel",
        short_count=0,
        long_count=0,
        cancel_count=2,
        timeout=20,
        poll_seconds=0.01,
    )
    assert len(samples) == 2
    for sample in samples:
        assert sample["status"] == "PASS" and sample["deterministic_provider"] is True
        assert sample["handler_calls_before_after"] == [1, 1]
        assert sample["fence_receipt"]["error_code"] == "stale_lease"
        assert (
            sample["received_lower_ns"]
            <= sample["received_upper_ns"]
            <= sample["fence_observed_ns"]
        )
        assert sample["latency_upper_seconds"] >= sample["latency_lower_seconds"] >= 0
    # Two offline fixtures validate instrumentation, not the required 30-case SLO.
    assert performance.summarize(samples)["cancel_fence"]["status"] == "NOT_RUN"
    performance.ensure_idle(env.store, env.actor)
    with env.store.transaction(env.actor) as tx:
        assert all(work.state.value == "cancelled" for work in tx.list_work())
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s",
                (env.actor.group_id,),
            ).fetchone()["n"]
            == 0
        )


async def test_commit_brackets_use_last_absent_start_and_first_visible_end(
    performance, monkeypatch
):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    first = datetime(2030, 1, 1, tzinfo=UTC)
    replies = iter(
        [
            (100, 110, None),
            (200, 210, {"id": "e", "event_db_at": first, "delivery_id": None}),
            (
                300,
                310,
                {
                    "id": "e",
                    "event_db_at": first,
                    "delivery_id": "d",
                    "outbox_db_at": first + timedelta(seconds=1),
                    "decision": {},
                },
            ),
        ]
    )
    monkeypatch.setattr(performance, "observe", lambda *args: next(replies))
    row = await performance.await_outbox(
        SimpleNamespace(store=None),
        None,
        SimpleNamespace(id="e"),
        submitted_ns=0,
        timeout=1,
        poll_seconds=0.001,
    )
    assert row["received_lower_ns"] == 100 and row["received_upper_ns"] == 210
    assert row["outbox_lower_ns"] == 200 and row["outbox_upper_ns"] == 310
    assert row["latency_upper_seconds"] == 210 / 1e9 and row["latency_lower_seconds"] == 0
    assert row["db_recorded_seconds"] == 1


@pytest.mark.postgres
def test_preflight_never_takes_over_other_pending_work(runtime_env, performance):
    from .conftest import source

    env = runtime_env
    event = source(env, "unresolved unrelated work")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("another run", event.id)
    with pytest.raises(RuntimeError, match="do not take them over"):
        performance.ensure_idle(env.store, env.actor)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).state.value == "queued"


def test_new_run_identity_and_sampling_thresholds_are_not_reusable_or_tuned(
    performance, tmp_path, monkeypatch
):
    monkeypatch.setattr(performance, "ROOT", tmp_path)
    root, manifest = performance.create_run("fixed-run", {"synthetic": True}, "cancel")
    assert manifest["thresholds"] == {
        "short_answer": 20.0,
        "long_admission": 5.0,
        "cancel_fence": 2.0,
    }
    with pytest.raises(FileExistsError):
        performance.create_run("fixed-run", {}, "cancel")
    assert (root / "manifest.json").is_file()
