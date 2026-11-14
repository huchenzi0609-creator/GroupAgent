"""Runner recovery only: never bootstrap secrets, providers, or paid work."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from groupagent.domain import ActorContext, EventKind, GroupEvent, Role, WorkState

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def matrix(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "acceptance_matrix_test", ROOT / "scripts/live_runtime_matrix.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def forbidden(*args, **kwargs):
        raise AssertionError("Tests must never bootstrap credentials or live providers")

    monkeypatch.setattr(module, "setup", forbidden)
    monkeypatch.setattr(module, "build_app", forbidden)
    return module


def run_inputs(module, tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    dataset = {
        "cases": [{"id": "CASE-01", "category": "chart", "checks": {"required_text": ["result"]}}]
    }
    dataset["cases_sha256"] = module.canonical_hash(dataset["cases"])
    args = SimpleNamespace(
        run_id="run",
        worker_image="sha256:" + "1" * 64,
        harness_image="sha256:" + "2" * 64,
        docker_host="unix:///synthetic.sock",
    )
    config = {
        "runtime": {
            "channel": "local",
            "channel_account_id": "matrix:run",
            "worker_image": args.worker_image,
            "harness_image": args.harness_image,
            "docker_host": args.docker_host,
        }
    }
    manifest = {
        "dataset_sha256": dataset["cases_sha256"],
        "config_sha256": module.canonical_hash(config),
        "worker_image": args.worker_image,
        "harness_image": args.harness_image,
        "channel": "local_only",
    }
    for name, data in [
        ("dataset", dataset),
        ("config", config),
        ("manifest", manifest),
        ("results", {"cases": []}),
    ]:
        module.save(root / (name + ".json"), data)
    return root, dataset, args


@pytest.mark.parametrize("change", ["gold", "worker", "harness", "channel", "config", "duplicate"])
def test_resume_rejects_changed_frozen_inputs_without_bootstrap(matrix, tmp_path, change):
    root, data, args = run_inputs(matrix, tmp_path)
    if change == "gold":
        data["cases"][0]["checks"]["required_text"] = ["changed"]
    elif change in {"worker", "harness"}:
        setattr(args, change + "_image", "sha256:" + "3" * 64)
    elif change in {"channel", "config"}:
        path = root / "config.json"
        value = json.loads(path.read_text())
        value["runtime"]["channel" if change == "channel" else "docker_host"] = "changed"
        matrix.save(path, value)
    else:
        matrix.save(root / "results.json", {"cases": [{"id": "CASE-01"}, {"id": "CASE-01"}]})
    with pytest.raises(RuntimeError):
        matrix.validate_resume(root, data, args)


def test_resume_validation_preserves_original_bytes(matrix, tmp_path):
    root, data, args = run_inputs(matrix, tmp_path)
    original = {p.name: p.read_bytes() for p in root.iterdir()}
    matrix.validate_resume(root, data, args)
    assert {p.name: p.read_bytes() for p in root.iterdir()} == original


def recovered_runner(
    module, tmp_path, *, state=WorkState.COMPLETED, prior_state="error", started=True
):
    run = module.MatrixRun.__new__(module.MatrixRun)
    run.root = tmp_path
    run.evidence_root = tmp_path / "rechecks" / "0001"
    run.revision = 1
    run.args = SimpleNamespace(run_id="run", limit=0)
    run.actor = ActorContext(group_id="g", member_id="m", role=Role.OWNER)
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id="event",
        source_key="matrix:run:CASE-01",
        group_id="g",
        actor_id="m",
        kind=EventKind.TEXT,
        text="synthetic",
        occurred_at=stamp,
        received_at=stamp,
    )
    work = SimpleNamespace(id="work", state=state, request_event_id="event")
    case = {"id": "CASE-01", "category": "chart", "checks": {"required_text": ["result"]}}
    run.dataset = {"cases": [case]}
    original = {
        "scope": {"group_id": "g", "work_id": "work", "event_id": "event"},
        "answer_text": "result",
        "artifacts": [],
        "input_media": [],
        "before_after": {"works_before": 0, "works_after": 1},
        "scheduling": {},
    }
    run.events = {"CASE-01": event} if started else {}
    run.records = (
        {
            "CASE-01": {
                "id": "CASE-01",
                "execution_state": prior_state,
                "evaluation": {"status": "FAIL"},
            }
        }
        if started
        else {}
    )
    run.original_manifest = {"source_sha256": "old-build"}
    run.ledger = SimpleNamespace(report=lambda actor: {"synthetic": True})
    run._evidence = lambda identifier: deepcopy(original) if started else {}
    run.collected = []

    async def collect(*args, **kwargs):
        assert kwargs["allow_paid_verification"] is False
        run.collected.append(kwargs)
        return deepcopy(original)

    run.collect = collect

    class Cursor:
        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class Tx:
        group_id = "g"
        conn = SimpleNamespace(execute=lambda *args: Cursor())

        def get_decision(self, identifier):
            return {"result": {"work_id": "work"}}

        def get_work(self, identifier):
            return work

    class Store:
        @contextmanager
        def transaction(self, actor):
            yield Tx()

    run.store = Store()
    return run, case, original


async def test_completed_database_work_rechecks_after_checker_error_without_new_paid_call(
    matrix, tmp_path
):
    run, case, original = recovered_runner(matrix, tmp_path)
    assert await run.resume_case(case)
    assert len(run.collected) == 1 and run.records[case["id"]]["execution_state"] == "completed"
    assert run.records[case["id"]]["evaluation"]["status"] == "PASS"
    assert run.records[case["id"]]["paid_calls_during_recheck"] == 0
    assert original["answer_text"] == "result"
    assert (run.evidence_root / "evidence" / "CASE-01.json").exists()


@pytest.mark.parametrize(
    "state", [WorkState.PAUSED, WorkState.FAILED, WorkState.CANCELLED, WorkState.WAITING_INPUT]
)
async def test_stopped_cases_keep_original_failure_without_retry(matrix, tmp_path, state):
    run, case, _ = recovered_runner(matrix, tmp_path, state=state, prior_state=state.value)
    old = deepcopy(run.records)
    assert await run.resume_case(case)
    assert run.records == old and run.collected == []


async def test_started_unknown_is_not_treated_as_an_unstarted_paid_case(matrix, tmp_path):
    run, case, _ = recovered_runner(matrix, tmp_path, state=WorkState.RUNNING)
    assert await run.resume_case(case) and run.collected == []


async def test_only_truly_unstarted_case_is_eligible_to_continue(matrix, tmp_path):
    run, case, _ = recovered_runner(matrix, tmp_path, started=False)
    assert await run.resume_case(case) is False and run.collected == []


async def test_independent_review_is_merged_only_into_new_evidence(matrix, tmp_path):
    run, case, original = recovered_runner(matrix, tmp_path)
    case["checks"] = {"semantic_claims": {"result_correct": True}}
    matrix.save(
        tmp_path / "reviews" / "CASE-01.json",
        {
            "semantic_claims": {
                "passed": True,
                "evidence_ref": "synthetic-independent-review",
                "reviewer_kind": "assistant",
            }
        },
    )
    assert await run.resume_case(case)
    updated = json.loads((run.evidence_root / "evidence" / "CASE-01.json").read_text())
    assert updated["review_sources"][0]["sha256"]
    assert "reviews" not in original
    assert run.records["CASE-01"]["evaluation"]["status"] == "PASS"


def test_resume_archives_prior_results_manifest_and_raw_evidence_before_bootstrap(
    matrix, tmp_path, monkeypatch
):
    base = tmp_path / "runtime" / "live"
    base.mkdir(parents=True)
    root, data, args = run_inputs(matrix, base)
    matrix.save(root / "evidence" / "CASE-01.json", {"synthetic": "original evidence"})
    matrix.save(tmp_path / "tests" / "fixtures" / "runtime_acceptance_cases.json", data)
    old = {
        name: (root / name).read_bytes()
        for name in ("results.json", "manifest.json", "config.json", "dataset.json")
    }
    args.resume = True
    args.recheck_only = True
    monkeypatch.setattr(matrix, "ROOT", tmp_path)
    with pytest.raises(AssertionError, match="bootstrap"):
        matrix.MatrixRun(args)
    archive = root / "revisions" / "0001"
    for name, content in old.items():
        assert (root / name).read_bytes() == content
        assert (archive / name).read_bytes() == content
    assert (archive / "evidence" / "CASE-01.json").read_bytes() == (
        root / "evidence" / "CASE-01.json"
    ).read_bytes()
