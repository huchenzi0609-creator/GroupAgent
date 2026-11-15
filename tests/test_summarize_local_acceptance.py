"""Saved report aggregation only; no providers, application setup or gold rewrites."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def aggregate(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "summary_test", ROOT / "scripts/summarize_local_acceptance.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2))


@pytest.fixture
def run(aggregate, tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    dataset = json.loads((ROOT / "tests/fixtures/runtime_acceptance_cases.json").read_text())
    config = {
        "group": {"internal_id": "g"},
        "runtime": {
            "channel": "local",
            "channel_account_id": "matrix:run",
            "worker_image": "sha256:" + "1" * 64,
            "harness_image": "sha256:" + "2" * 64,
        },
    }
    manifest = {
        "source_sha256": "a" * 64,
        "dataset_sha256": dataset["cases_sha256"],
        "config_sha256": aggregate.digest(config),
        "worker_image": config["runtime"]["worker_image"],
        "harness_image": config["runtime"]["harness_image"],
        "channel": "local_only",
        "paid_budget_group": "g",
    }
    for name, value in [
        ("dataset", dataset),
        ("config", config),
        ("manifest", manifest),
        ("results", {"cases": []}),
    ]:
        save(root / (name + ".json"), value)
    return root


def md_result(aggregate, root, *, saved_status="PASS", state="completed", revision=0):
    identifier = "RA-MD-01"
    directory = root if revision == 0 else root / "rechecks" / f"{revision:04d}"
    content = (
        "# 合成交接说明\n## 准备\n确认输入来源\n## 执行\n使用受控工具\n## 核对\n检查产物和回执\n"
    )
    output = directory / "outputs" / identifier / "result.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content)
    checksum = aggregate.file_digest(output)
    evidence = {
        "scope": {"group_id": "g", "work_id": "w"},
        "artifacts": [
            {
                "group_id": "g",
                "work_id": "w",
                "file_id": "f" * 32,
                "version_id": "e" * 32,
                "format": "md",
                "sha256": checksum,
                "content_path": str(output),
                "validation": {"passed": True, "sha256": checksum},
                "decoded": {"source_sha256": checksum, "text": content},
            }
        ],
        "delivery": {
            "parts": [{"state": "accepted", "updated_at": "2030-01-01T00:00:00Z"}],
            "work_completed_at": "2030-01-01T00:00:01Z",
        },
    }
    path = directory / "evidence" / (identifier + ".json")
    save(path, evidence)
    record = {
        "id": identifier,
        "category": "md",
        "execution_state": state,
        "work_id": "w",
        "evaluation": {"status": saved_status},
        "evidence_path": str(path),
    }
    if revision:
        original = json.loads((root / "manifest.json").read_text())
        verification = {
            **original,
            "revision": revision,
            "original_source_sha256": original["source_sha256"],
            "source_sha256": "b" * 64,
            "checker_sha256": "c" * 64,
            "completed_rechecks_allow_paid_calls": False,
            "recheck_only": True,
        }
        save(directory / "verification-manifest.json", verification)
        record["verification_revision"] = revision
    return record, evidence, path


def test_fixed_thirty_denominator_does_not_count_unstarted_missing_cases(aggregate, run):
    record, _, _ = md_result(aggregate, run)
    save(run / "results.json", {"cases": [record]})
    result = aggregate.summarize_run(run)
    assert result["denominator"] == 30 and result["successes"] == 1
    assert result["statuses"] == {"PASS": 1, "FAIL": 0, "NOT_RUN": 29, "NEEDS_REVIEW": 0}
    assert result["by_family"]["documents"]["denominator"] == 14
    assert sum(row["denominator"] for row in result["by_family"].values()) == 30
    assert result["provider_calls"] == result["business_writes_applied"] == 0


@pytest.mark.parametrize("state", ["paused", "failed", "cancelled", "waiting_input", "timeout"])
def test_partial_valid_artifact_never_promotes_a_stopped_work(aggregate, run, state):
    record, _, _ = md_result(aggregate, run, state=state)
    save(run / "results.json", {"cases": [record]})
    result = aggregate.summarize_run(run)
    assert result["successes"] == 0
    assert next(row for row in result["cases"] if row["id"] == "RA-MD-01")["status"] == "FAIL"


def test_old_checker_failure_requires_saved_recheck_before_promotion(aggregate, run):
    record, _, _ = md_result(aggregate, run, saved_status="FAIL")
    save(run / "results.json", {"cases": [record]})
    original = (run / "results.json").read_bytes()
    first = aggregate.summarize_run(run)
    assert first["successes"] == 0 and first["statuses"]["NEEDS_REVIEW"] == 1
    _, _, _ = md_result(aggregate, run, saved_status="FAIL", revision=1)
    second = aggregate.summarize_run(run)
    assert second["successes"] == 1 and second["mixed_product_sources"]
    assert (run / "results.json").read_bytes() == original


@pytest.mark.parametrize(
    "damage",
    [
        "artifact",
        "decoded_hash",
        "config",
        "gold",
        "image",
        "recheck_config",
        "review_hash",
        "foreign_scope",
    ],
)
def test_integrity_mismatches_are_rejected_not_averaged_into_success(aggregate, run, damage):
    record, evidence, path = md_result(aggregate, run, revision=1)
    save(run / "results.json", {"cases": [record]})
    if damage == "artifact":
        Path(evidence["artifacts"][0]["content_path"]).write_text("changed bytes")
    elif damage == "decoded_hash":
        evidence["artifacts"][0]["decoded"]["source_sha256"] = "0" * 64
        save(path, evidence)
    elif damage == "config":
        value = json.loads((run / "config.json").read_text())
        value["changed"] = True
        save(run / "config.json", value)
    elif damage == "gold":
        value = json.loads((run / "dataset.json").read_text())
        value["cases"][0]["prompt"] = "changed"
        save(run / "dataset.json", value)
    elif damage == "image":
        value = json.loads((run / "manifest.json").read_text())
        value["worker_image"] = "sha256:" + "9" * 64
        save(run / "manifest.json", value)
    elif damage == "recheck_config":
        p = run / "rechecks" / "0001" / "verification-manifest.json"
        value = json.loads(p.read_text())
        value["config_sha256"] = "0" * 64
        save(p, value)
    elif damage == "review_hash":
        p = run / "reviews" / "RA-MD-01.json"
        save(p, {"synthetic": True})
        evidence["review_sources"] = [{"path": str(p), "sha256": "0" * 64}]
        save(path, evidence)
    else:
        evidence["scope"]["group_id"] = "foreign"
        save(path, evidence)
    with pytest.raises(aggregate.EvidenceIntegrityError):
        aggregate.summarize_run(run)


def test_missing_artifact_bytes_or_configuration_anchor_cannot_pass(aggregate, run):
    record, evidence, _ = md_result(aggregate, run)
    save(run / "results.json", {"cases": [record]})
    Path(evidence["artifacts"][0]["content_path"]).unlink()
    assert aggregate.summarize_run(run)["successes"] == 0
    md_result(aggregate, run)
    value = json.loads((run / "manifest.json").read_text())
    value.pop("config_sha256")
    save(run / "manifest.json", value)
    assert aggregate.summarize_run(run)["config_hash_verified"] is False
    assert aggregate.summarize_run(run)["successes"] == 0


def test_original_archives_and_mixed_source_revisions_are_retained(aggregate, run):
    record, _, _ = md_result(aggregate, run)
    save(run / "results.json", {"cases": [record]})
    for name in ("results", "manifest", "dataset", "config"):
        save(
            run / "revisions" / "0001" / (name + ".json"),
            json.loads((run / (name + ".json")).read_text()),
        )
    revised, _, _ = md_result(aggregate, run, revision=1)
    save(run / "results.json", {"cases": [revised]})
    result = aggregate.summarize_run(run)
    assert result["source_versions"] == ["a" * 64, "b" * 64]
    assert result["prior_results"][0]["path"].endswith("revisions/0001/results.json")
    assert result["verification_manifests"][0]["manifest"]["revision"] == 1


def test_unaccepted_parts_cannot_count_as_success_even_with_correct_content(aggregate, run):
    record, evidence, path = md_result(aggregate, run)
    evidence["delivery"]["parts"][0]["state"] = "unknown"
    save(path, evidence)
    save(run / "results.json", {"cases": [record]})
    result = aggregate.summarize_run(run)
    assert result["successes"] == 0 and result["statuses"]["FAIL"] == 1


def test_execution_saved_under_resume_directory_is_not_itself_a_completed_recheck(aggregate, run):
    record, _, _ = md_result(aggregate, run, saved_status="FAIL", revision=1)
    record.pop("verification_revision")
    record["execution_source_sha256"] = "b" * 64
    path = run / "rechecks" / "0001" / "verification-manifest.json"
    manifest = json.loads(path.read_text())
    manifest["recheck_only"] = False
    save(path, manifest)
    save(run / "results.json", {"cases": [record]})
    result = aggregate.summarize_run(run)
    assert result["successes"] == 0 and result["statuses"]["NEEDS_REVIEW"] == 1
    record.update(verification_revision=1, paid_calls_during_recheck=0)
    save(run / "results.json", {"cases": [record]})
    assert aggregate.summarize_run(run)["successes"] == 1


def test_empty_review_file_is_not_review_evidence(aggregate, run):
    record, _, _ = md_result(aggregate, run, saved_status="NEEDS_REVIEW")
    save(run / "results.json", {"cases": [record]})
    save(run / "reviews" / "RA-MD-01.json", {})
    assert aggregate.summarize_run(run)["successes"] == 0
