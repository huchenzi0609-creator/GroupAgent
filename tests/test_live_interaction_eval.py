"""Evaluation-run provenance only; all provider/bootstrap paths are forbidden."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runner(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "interaction_recording_test", ROOT / "scripts/live_interaction_eval.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("No credentials or provider bootstrap in these tests")

    monkeypatch.setattr(module, "setup", forbidden)
    monkeypatch.setattr(module, "DeepSeekHTTPModel", forbidden)
    dataset = json.loads((ROOT / "tests/fixtures/interaction_cases.json").read_text())
    target = tmp_path / "tests" / "fixtures" / "interaction_cases.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(dataset))
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "synthetic.py").write_text("synthetic_version = 1\n")
    return module, dataset, SimpleNamespace(run_id="new-frozen-run", limit=0, concurrency=2)


async def test_existing_run_is_rejected_before_any_secret_or_provider_bootstrap(runner):
    module, _, args = runner
    path = module.ROOT / "runtime" / "live" / args.run_id
    path.mkdir(parents=True)
    original = path / "cases.jsonl"
    original.write_text("preserve original\n")
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await module.run(args)
    assert original.read_text() == "preserve original\n"


async def test_completed_run_has_frozen_dataset_source_provider_and_manifest_link(
    runner, monkeypatch
):
    module, dataset, args = runner

    async def fake(args, destination, frozen, manifest):
        assert frozen == dataset
        manifest["provider_config"].update(
            actual_model="deepseek-flash", actual_client_name="synthetic-no-api"
        )
        return {"count": 264, "statuses": {"PASS": 264}, "synthetic_unit_test": True}

    monkeypatch.setattr(module, "_run_cases", fake)
    result = await module.run(args)
    root = module.ROOT / "runtime" / "live" / args.run_id
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["dataset_sha256"] == dataset["cases_sha256"]
    assert (
        manifest["source_sha256"] == module._source_hash()
        and not manifest["source_changed_during_run"]
    )
    assert manifest["started_at"] and manifest["ended_at"] and manifest["state"] == "completed"
    assert len(manifest["selected_case_ids"]) == 264
    assert manifest["provider_config"]["model"] == "deepseek-flash"
    assert (
        result["manifest_sha256"]
        == hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    )
    assert json.loads((root / "dataset.json").read_text()) == dataset
    assert (root / "manifest.json").stat().st_mode & 0o077 == 0


async def test_aborted_run_has_ended_time_and_cannot_be_reused(runner, monkeypatch):
    module, _, args = runner

    async def failing(*args):
        raise RuntimeError("synthetic stopped test")

    monkeypatch.setattr(module, "_run_cases", failing)
    with pytest.raises(RuntimeError, match="synthetic stopped"):
        await module.run(args)
    root = module.ROOT / "runtime" / "live" / args.run_id
    manifest = json.loads((root / "manifest.json").read_text())
    assert (
        manifest["state"] == "aborted"
        and manifest["ended_at"]
        and manifest["error_type"] == "RuntimeError"
    )
    assert not (root / "result.json").exists()
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await module.run(args)


async def test_source_change_during_run_is_explicitly_marked(runner, monkeypatch):
    module, _, args = runner

    async def fake(*args):
        (module.ROOT / "src" / "synthetic.py").write_text("synthetic_version = 2\n")
        return {"count": 0, "synthetic_unit_test": True}

    monkeypatch.setattr(module, "_run_cases", fake)
    result = await module.run(args)
    assert result["source_changed_during_run"] is True


def test_changed_frozen_labels_are_rejected_before_run_creation(runner):
    module, dataset, args = runner
    dataset["cases"][0]["text"] = "changed fixture"
    with pytest.raises(RuntimeError, match="checksum"):
        module.prepare_run(args, dataset)
    assert not (module.ROOT / "runtime" / "live" / args.run_id).exists()
