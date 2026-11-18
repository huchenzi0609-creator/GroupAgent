"""Run and score mandatory local Pot acceptance before any test_2 deployment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
CATALOG = FIXTURES / "pot_acceptance_catalog.v1.json"
COVERAGE = FIXTURES / "pot_replay_coverage.v1.json"
BLIND_LOCK = FIXTURES / "pot_blind_lock.v1.json"
STATUSES = {"passed", "failed", "skipped", "blocked", "missing", "review_required"}
NODE = re.compile(r"^tests/[a-zA-Z0-9_/]+\.py::test_[a-zA-Z0-9_]+$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_bytes())
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path.name}")
    return data


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _worker_source_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "files": len(manifest["files"]),
        "sha256": hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }


def _expected_worker_manifest() -> dict[str, Any]:
    package = ROOT / "src" / "groupagent"
    return {
        "files": {
            path.relative_to(package).as_posix(): sha256(path)
            for path in package.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
        },
        "supervisor": sha256(package / "sandbox" / "supervisor.py"),
    }


def observe_worker_source(worker_image: str, docker_host: str | None) -> dict[str, Any]:
    """Look through the actual isolated interpreter rather than trusting an image label."""
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", worker_image):
        raise ValueError("Worker image must be an immutable SHA-256 image ID")
    command = ["docker"]
    if docker_host:
        command.extend(["--host", docker_host])
    probe = """
import hashlib, json
from pathlib import Path
import groupagent

package = Path(groupagent.__file__).resolve().parent
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

print(json.dumps({
    "files": {
        path.relative_to(package).as_posix(): sha(path)
        for path in package.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    },
    "supervisor": sha(Path("/opt/groupagent-worker.py")),
}))
"""
    try:
        result = subprocess.run(
            [
                *command,
                "run",
                "--rm",
                "--pull=never",
                "--network=none",
                "--read-only",
                "--entrypoint",
                "python3",
                worker_image,
                "-I",
                "-c",
                probe,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=45,
        )
        actual = json.loads(result.stdout)
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as error:
        raise ValueError("Unable to verify code loaded by the pinned worker image") from error
    expected = _expected_worker_manifest()
    if actual != expected:
        raise ValueError("Pinned worker code differs from the candidate; rebuild before acceptance")
    return _worker_source_summary(expected)


def write_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(".pending")
    with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        json.dump(data, output, indent=2, ensure_ascii=False, default=str)
        output.write("\n")
    tmp.replace(path)


def local_source() -> dict[str, Any]:
    files = [
        *ROOT.joinpath("src").rglob("*"),
        *ROOT.joinpath("deploy").rglob("*"),
        *ROOT.joinpath(".github", "workflows").glob("*"),
        ROOT / "pyproject.toml",
        ROOT / "uv.lock",
    ]
    digest = hashlib.sha256()
    for path in sorted(
        path
        for path in files
        if path.is_file() and "__pycache__" not in path.parts and not path.name.startswith(".")
    ):
        digest.update(str(path.relative_to(ROOT)).encode("utf-8") + b"\0")
        digest.update(bytes.fromhex(sha256(path)))
    return {
        "sha256": digest.hexdigest(),
        "git_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
    }


def coverage() -> dict[str, dict[str, Any]]:
    catalog = read_json(CATALOG)
    data = read_json(COVERAGE)
    ids = [row["id"] for row in catalog["cases"]]
    rows = {row["id"]: row for row in data["cases"]}
    if data["catalog_sha256"] != sha256(CATALOG):
        raise ValueError("The approved catalog changed")
    if len(ids) != 25 or ids != list(rows) or len(rows) != len(data["cases"]):
        raise ValueError("All 25 approved catalog IDs must occur exactly once and in catalog order")
    for row in rows.values():
        nodes = row["replay"]
        checks = row["blind_checks"]
        if (
            not nodes
            or len(set(nodes)) != len(nodes)
            or any(not NODE.fullmatch(node) for node in nodes)
            or not checks
            or len(set(checks)) != len(checks)
            or row["blind_level"] not in {"deepseek_local", "isolated_infrastructure"}
        ):
            raise ValueError(f"Invalid acceptance bindings for {row['id']}")
    return rows


def bindings() -> dict[str, Any]:
    return {
        "catalog_sha256": sha256(CATALOG),
        "coverage_sha256": sha256(COVERAGE),
        "source": local_source(),
    }


def junit_status(path: Path) -> dict[str, list[dict[str, str]]]:
    results: dict[str, list[dict[str, str]]] = {}
    if not path.exists():
        return results
    for test in ET.parse(path).iter("testcase"):
        path_from_pytest = test.get("file")
        if not path_from_pytest:
            continue
        name = test.get("name", "")
        node = path_from_pytest + "::" + name.split("[", 1)[0]
        status = "passed"
        if test.find("error") is not None or test.find("failure") is not None:
            status = "failed"
        elif test.find("skipped") is not None:
            status = "skipped"
        results.setdefault(node, []).append(
            {"name": name, "status": status, "seconds": test.get("time", "0")}
        )
    return results


def summarize_status(values: list[str]) -> str:
    if not values:
        return "missing"
    for candidate in ("failed", "blocked", "skipped", "missing", "review_required"):
        if candidate in values:
            return candidate
    return "passed"


def replay(args: argparse.Namespace) -> int:
    rows = coverage()
    requested = args.cases.split(",") if args.cases else list(rows)
    if not requested or not set(requested) <= rows.keys() or len(set(requested)) != len(requested):
        raise ValueError("Provide comma-separated, unique catalog IDs")
    worker_source = (
        observe_worker_source(args.worker_image, args.docker_host) if args.worker_image else None
    )
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    start = bindings()
    nodes = list(dict.fromkeys(node for identity in requested for node in rows[identity]["replay"]))
    report_path = args.output / "pytest.xml"
    env = dict(os.environ)
    env["GROUPAGENT_TEST_DSN"] = args.dsn_file.read_text().strip()
    if not env["GROUPAGENT_TEST_DSN"]:
        raise ValueError("A dedicated isolated replay PostgreSQL DSN is required")
    if args.worker_image:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", args.worker_image):
            raise ValueError("Worker image must be an immutable SHA-256 image ID")
        env["GROUPAGENT_WORKER_IMAGE"] = args.worker_image
    if args.docker_host:
        env["GROUPAGENT_DOCKER_HOST"] = args.docker_host
    version = postgres_identity(env["GROUPAGENT_TEST_DSN"])
    evaluation_files = [Path(__file__), COVERAGE, *(ROOT / x.split("::", 1)[0] for x in nodes)]
    fixture_before = fixture_hashes(evaluation_files)
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--tb=short",
        "-o",
        "junit_family=xunit1",
        f"--junitxml={report_path.absolute()}",
        *nodes,
    ]
    with os.fdopen(
        os.open(args.output / "pytest.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
    ) as stream:
        run = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT)
    if report_path.exists():
        report_path.chmod(0o600)
    by_node = junit_status(report_path)
    result_rows = []
    for identity, row in rows.items():
        node_results = {node: by_node.get(node, []) for node in row["replay"]}
        values = [summarize_status([r["status"] for r in value]) for value in node_results.values()]
        result_rows.append(
            {"id": identity, "status": summarize_status(values), "nodes": node_results}
        )
    consistent = start == bindings() and fixture_before == fixture_hashes(evaluation_files)
    successful = (
        run.returncode == 0 and consistent and all(row["status"] == "passed" for row in result_rows)
    )
    write_json(
        args.output / "replay.json",
        {
            "format_version": 1,
            "layer": "replay",
            **start,
            "environment": {
                "postgres": version,
                "execution": "deterministic_model_fixtures_and_applicable_real_tools",
                "channel": "local_spool_and_explicit_transport_fault_fixtures",
                "platform": platform.platform(),
                "worker_image": args.worker_image,
                "worker_source": worker_source,
                "python": platform.python_version(),
            },
            "fixture_sha256": fixture_before,
            "pytest": {
                "returncode": run.returncode,
                "xml_sha256": sha256(report_path) if report_path.exists() else None,
                "log_sha256": sha256(args.output / "pytest.log"),
            },
            "consistent": consistent,
            "passed": successful,
            "cases": result_rows,
        },
    )
    print_summary("replay", result_rows)
    return 0 if successful else 1


def fixture_hashes(paths: list[Path]) -> dict[str, str]:
    return {str(path.relative_to(ROOT)): sha256(path) for path in sorted(set(paths))}


def postgres_identity(dsn: str) -> dict[str, str]:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    parsed = conninfo_to_dict(dsn)
    if not str(parsed.get("host", "")).startswith("/") or parsed.get("hostaddr"):
        raise ValueError("Evaluation PostgreSQL must use its dedicated private Unix socket")
    if not re.fullmatch(r"pot_(replay|blind)", str(parsed.get("dbname", ""))):
        raise ValueError("Use the dedicated pot_replay or pot_blind database")
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        version = connection.execute("SHOW server_version").fetchone()[0]
    return {"server_version": version, "database": parsed["dbname"], "transport": "private_unix"}


def check_status(row: dict[str, Any] | None, expected: set[str]) -> str:
    if row is None:
        return "missing"
    checks = row.get("checks")
    if not isinstance(checks, dict) or set(checks) != expected:
        return "blocked"
    states = []
    for value in checks.values():
        if not isinstance(value, dict) or value.get("status") not in STATUSES:
            return "blocked"
        state = value["status"]
        if state == "passed" and (
            not isinstance(value.get("evidence"), list)
            or not value["evidence"]
            or any(not isinstance(item, dict) or not item for item in value["evidence"])
        ):
            return "blocked"
        states.append(state)
    return summarize_status(states)


def blind_status(row: dict[str, Any] | None, expected: set[str]) -> str:
    status = check_status(row, expected)
    if status != "passed" or row is None:
        return status
    if "runner_error" in row or "cleanup_error" in row:
        return "blocked"
    if row.get("level") == "deepseek_local":
        if not any(item.get("status") == "returned" for item in row.get("model_rounds", [])):
            return "blocked"
        for turn in row.get("turns", []):
            outcome = (turn.get("foreground") or {}).get("state")
            if outcome in {"blocked", "pending", "observation_timeout"}:
                return "blocked"
            if turn.get("latency_required"):
                seconds = turn.get("first_receipt_seconds")
                if (
                    type(seconds) not in (int, float)
                    or not 0 <= seconds <= 10
                    or not turn.get("receipts")
                ):
                    return "failed"
    return status


def print_summary(layer: str, rows: list[dict[str, Any]]) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    print(json.dumps({"layer": layer, "counts": counts}, ensure_ascii=False), flush=True)


def read_bound_report(path: Path | None, layer: str, expected: dict[str, Any]) -> dict:
    if path is None:
        return {}
    report = read_json(path)
    if not (
        report.get("format_version") == 1
        and report.get("layer") == layer
        and all(report.get(key) == value for key, value in expected.items())
    ):
        raise ValueError(f"{layer} report does not match the current source and approved catalog")
    if report.get("consistent") is not True:
        raise ValueError(f"{layer} was not collected on a stable source tree")
    return report


def indexed_cases(report: dict, expected: dict) -> dict[str, dict]:
    rows = report.get("cases", [])
    indexed = {row["id"]: row for row in rows}
    if len(indexed) != len(rows) or not indexed.keys() <= expected.keys():
        raise ValueError("Report contains a duplicate or unapproved case ID")
    return indexed


def score(args: argparse.Namespace) -> int:
    expected = coverage()
    binding = bindings()
    replay_report = read_bound_report(args.replay, "replay", binding)
    blind_report = read_bound_report(args.blind, "blind", binding)
    replay_rows = indexed_cases(replay_report, expected)
    blind_rows = indexed_cases(blind_report, expected)
    if replay_report:
        results = junit_status(args.replay.parent / "pytest.xml")
        expected_xml = replay_report["pytest"].get("xml_sha256")
        if expected_xml is not None and sha256(args.replay.parent / "pytest.xml") != expected_xml:
            raise ValueError("Replay evidence checksum mismatch")
        if not replay_report.get("fixture_sha256") or any(
            sha256(ROOT / relative) != digest
            for relative, digest in replay_report["fixture_sha256"].items()
        ):
            raise ValueError("Replay fixtures changed since the recorded run")
    else:
        results = {}
    if blind_report:
        if blind_report.get("dataset_sha256") != read_json(BLIND_LOCK)["dataset_sha256"]:
            raise ValueError("Blind evidence is for a different held-out dataset")
        if blind_report.get("runner_sha256") != sha256(
            ROOT / "scripts" / "pot_blind_acceptance.py"
        ):
            raise ValueError("Blind evaluator changed since the recorded run")
        if blind_report.get("inspector_sha256") != sha256(
            ROOT / "scripts" / "runtime_acceptance_checks.py"
        ):
            raise ValueError("Independent artifact inspector changed since the recorded run")
        environment = blind_report.get("environment", {})
        if (
            environment.get("channel") != "LocalChannel"
            or environment.get("postgres", {}).get("database") != "pot_blind"
            or environment.get("model", {}).get("provider") != "api.deepseek.com"
            or not environment.get("model", {}).get("model")
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", environment.get("worker_image", ""))
        ):
            raise ValueError(
                "Blind report is missing reviewed provider, worker or test-environment identity"
            )
        if replay_report and (
            environment["worker_image"] != replay_report["environment"].get("worker_image")
        ):
            raise ValueError("Blind and replay worker image pins differ")
        required_worker_source = _worker_source_summary(_expected_worker_manifest())
        if environment.get("worker_source") != required_worker_source or (
            replay_report
            and replay_report["environment"].get("worker_source") != required_worker_source
        ):
            raise ValueError("Acceptance worker byte identity does not match the candidate")
        for identity, row in blind_rows.items():
            evidence = {key: value for key, value in row.items() if key != "evidence_sha256"}
            if row.get("evidence_sha256") is None:
                if row.get("status") != "missing":
                    raise ValueError(f"Blind case {identity} has no recorded evidence")
                continue
            path = args.blind.parent / identity / "evidence.json"
            if sha256(path) != row["evidence_sha256"] or read_json(path) != evidence:
                raise ValueError(f"Blind case {identity} evidence checksum mismatch")
    local = []
    for identity, row in expected.items():
        pytest_statuses = [
            summarize_status([item["status"] for item in results.get(node, ())])
            for node in row["replay"]
        ]
        replay_status = summarize_status(pytest_statuses) if identity in replay_rows else "missing"
        blind_row = blind_rows.get(identity)
        held_out_status = blind_status(blind_row, set(row["blind_checks"]))
        if blind_row and blind_row.get("level") != row["blind_level"]:
            held_out_status = "blocked"
        local.append(
            {
                "id": identity,
                "replay": replay_status,
                "blind": held_out_status,
                "status": summarize_status([replay_status, held_out_status]),
            }
        )
    permitted = (
        all(row["status"] == "passed" for row in local)
        and replay_report.get("passed") is True
        and blind_report.get("passed") is True
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(
        args.output,
        {
            "format_version": 1,
            "layer": "pre_test_2",
            **binding,
            "test_2_eligible": permitted,
            "replay_report_sha256": sha256(args.replay) if args.replay else None,
            "blind_report_sha256": sha256(args.blind) if args.blind else None,
            "cases": local,
        },
    )
    print_summary("pre_test_2", local)
    return 0 if permitted else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("replay", help="Run the mandatory deterministic case bindings")
    run.add_argument("--dsn-file", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--worker-image")
    run.add_argument("--docker-host")
    run.add_argument("--cases", help="Select smoke-test IDs; omitted cases block release")
    status = commands.add_parser(
        "score", help="Require passing current evidence at both local layers"
    )
    status.add_argument("--replay", type=Path)
    status.add_argument("--blind", type=Path)
    status.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        return replay(args) if args.command == "replay" else score(args)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
