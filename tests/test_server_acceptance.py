"""Runner parameter/report guards only; never start Docker, PG or providers."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/server_acceptance.py"
SPEC = importlib.util.spec_from_file_location("server_acceptance", SCRIPT)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


async def test_plan_only_writes_private_matrix_and_cannot_execute_external_phases(
    tmp_path, monkeypatch
):
    async def external(*args):
        pytest.fail("Plan invoked an external phase")

    for name in ("worker_phase", "state_phase", "live_phase"):
        monkeypatch.setattr(runner, name, external)
    args = runner.parser().parse_args(["plan", "--run-id", "synthetic", "--output", str(tmp_path)])
    assert await runner.run(args) == 0
    path = tmp_path / "synthetic/plan/report.json"
    report = json.loads(path.read_text())
    assert report["status"] == "PASS" and report["provider_calls_by_runner"] == 0
    assert {row["id"] for row in report["matrix"]} == {
        "W01",
        "W02",
        "W03",
        "W04",
        "D01",
        "L01",
        "L02",
        "L03",
    }
    assert not path.stat().st_mode & 0o077


@pytest.mark.parametrize("value", ["../redirect", "a/b", ".", "name with spaces"])
def test_run_id_cannot_redirect_evidence(value):
    with pytest.raises(SystemExit):
        runner.parser().parse_args(["plan", "--run-id", value])


def test_allocated_remainder_does_not_require_the_historical_group_or_a_new_50_cap():
    allocation = {
        "parent_limit_cny": "50",
        "source_upper_cny": "17.266165",
        "allocated_cap_cny": "20",
        "source_paid_entry_frozen": True,
        "target_group_sha256": hashlib.sha256(b"synthetic-new-test2-group").hexdigest(),
    }
    result = runner.allocation_check(allocation, "synthetic-new-test2-group", Decimal(20))
    assert result["allocated_cap_cny"] == "20" and result["source_upper_cny"] == "17.266165"
    for changed, expected in (
        ({"allocated_cap_cny": "40"}, "allocation_exceeds_parent_budget"),
        ({"source_paid_entry_frozen": False}, "allocation_not_frozen_or_cap_mismatch"),
        ({"target_group_sha256": "0" * 64}, "allocation_group_mismatch"),
    ):
        with pytest.raises(runner.CheckFailed) as error:
            runner.allocation_check(
                {**allocation, **changed}, "synthetic-new-test2-group", Decimal(20)
            )
        assert error.value.code == expected


async def test_failure_report_excludes_exception_text_and_distinguishes_review(tmp_path):
    args = runner.parser().parse_args(["plan", "--run-id", "failure", "--output", str(tmp_path)])
    report = runner.Report(args)

    async def secret_failure():
        raise ValueError("synthetic-secret-that-must-not-be-logged")

    async def missing_review():
        raise runner.CheckFailed("human_content_review_missing")

    await report.case("SYNTHETIC", secret_failure)
    await report.case("L01", missing_review)
    assert report.finish() == 2
    encoded = (report.root / "report.json").read_text()
    assert "synthetic-secret" not in encoded
    assert report.body["cases"][0]["code"] == "operation_failed"
    assert report.body["cases"][1]["status"] == "NEEDS_REVIEW"


def test_private_inputs_refuse_public_permissions_and_symlinks(tmp_path):
    path = tmp_path / "synthetic.dsn"
    path.write_text("synthetic-placeholder")
    path.chmod(0o644)
    with pytest.raises(runner.CheckFailed, match="private_file_permissions"):
        runner.private_file(path)
    path.chmod(0o600)
    assert runner.private_file(path) == b"synthetic-placeholder"
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(runner.CheckFailed, match="private_file_invalid"):
        runner.private_file(link)
