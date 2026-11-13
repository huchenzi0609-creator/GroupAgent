from groupagent.execution.harness_probe import (
    HARNESS_REFERENCE_COMMIT,
    installed_cli_metadata,
    probe,
)


def test_probe_does_not_claim_runtime_or_isolation_acceptance() -> None:
    report = probe()
    assert report["reference_commit"] == HARNESS_REFERENCE_COMMIT
    assert report["credentials_read"] is False and report["model_requests"] == 0
    assert report["runtime_started"] is False
    assert report["adoption_gate_passed"] is False
    assert "process_per_work_cancel_and_recovery_not_verified" in report["blockers"]


def test_cli_metadata_probe_reads_manifest_without_launching_binary(tmp_path, monkeypatch) -> None:
    package = tmp_path / "package"
    binary = package / "lib" / "bin.js"
    binary.parent.mkdir(parents=True)
    binary.write_text("This is not executable and must never be launched.")
    (package / "package.json").write_text('{"name":"@deepseek-ai/dsh","version":"0.1.5-rc.1"}')
    monkeypatch.setattr("groupagent.execution.harness_probe.shutil.which", lambda _: str(binary))
    assert installed_cli_metadata() == {
        "executable": str(binary),
        "npm_package_version": "0.1.5-rc.1",
    }
