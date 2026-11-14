"""Read-only, fixed-denominator aggregation of saved local acceptance evidence.

No application bootstrap, credentials, database writes, provider calls or gold
changes. Current checker results cannot silently promote an old failed checker
record without a saved, scoped verification revision (or a new explicit review).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from runtime_acceptance_checks import evaluate_case

ROOT = Path(__file__).resolve().parents[1]
STATUSES = ("PASS", "FAIL", "NOT_RUN", "NEEDS_REVIEW")
STOPPED = {"failed", "paused", "cancelled", "waiting_input", "timeout"}
FAMILIES = {
    "documents": {"docx", "pptx", "pdf", "md", "version_edit"},
    "charts_and_diagrams": {"programmatic_chart", "flowchart"},
    "code_execution": {"python", "shell"},
    "image_understanding": {"image_analysis"},
    "web_research": {"web_research"},
    "speech": {"asr", "tts"},
    "scheduling": {"schedule_create", "schedule_update", "schedule_fire"},
}


class EvidenceIntegrityError(ValueError):
    pass


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def file_digest(path: Path, *, limit=64 * 1024**2) -> str:
    if (
        not path.is_file()
        or path.is_symlink()
        or path.resolve() != path.absolute()
        or path.stat().st_size > limit
    ):
        raise EvidenceIntegrityError(
            "Evidence path is missing, redirected, or oversized: " + str(path)
        )
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def read(path: Path) -> dict:
    file_digest(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise EvidenceIntegrityError("Evidence JSON must be an object")
    return value


def scoped_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    if not path.is_relative_to(root) or path.resolve() != path.absolute():
        raise EvidenceIntegrityError("Evidence reference escaped its run")
    return path


def valid_hash(value):
    return isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None


def _manifest(root, manifest, config, dataset, *, original=None, revision=None):
    if manifest.get("dataset_sha256") != dataset["cases_sha256"]:
        raise EvidenceIntegrityError("Manifest dataset hash differs from frozen gold")
    if manifest.get("config_sha256") and manifest["config_sha256"] != digest(config):
        raise EvidenceIntegrityError("Manifest configuration checksum differs")
    runtime = config.get("runtime", {})
    for key in ("worker_image", "harness_image"):
        if manifest.get(key) != runtime.get(key) or not re.fullmatch(
            r"(?:[^\s]+@)?sha256:[a-f0-9]{64}", str(manifest.get(key))
        ):
            raise EvidenceIntegrityError("Pinned image/configuration differs: " + key)
    if not valid_hash(manifest.get("source_sha256")):
        raise EvidenceIntegrityError("Manifest has no valid product source hash")
    if original is not None:
        if manifest.get("original_source_sha256") != original["source_sha256"]:
            raise EvidenceIntegrityError("Verification revision refers to another original source")
        if (
            manifest.get("revision") != revision
            or manifest.get("completed_rechecks_allow_paid_calls") is not False
        ):
            raise EvidenceIntegrityError("Verification revision has no non-paying recheck contract")
        if not valid_hash(manifest.get("checker_sha256")):
            raise EvidenceIntegrityError("Verification revision has no checker checksum")
    elif manifest.get("channel") != "local_only" or runtime.get("channel") != "local":
        raise EvidenceIntegrityError("This report only accepts the declared local channel run")
    if runtime.get("channel_account_id") != "matrix:" + root.name:
        raise EvidenceIntegrityError("Run directory and local channel identity differ")


def _artifact_integrity(root, evidence):
    unavailable = []
    for artifact in evidence.get("artifacts") or []:
        if not isinstance(artifact, dict):
            unavailable.append("artifact metadata")
            continue
        checksum = artifact.get("sha256")
        decoded = artifact.get("decoded") or {}
        validation = artifact.get("validation") or {}
        if valid_hash(checksum):
            for label, value in (
                ("decoded", decoded.get("source_sha256")),
                ("validation", validation.get("sha256")),
            ):
                if value is not None and value != checksum:
                    raise EvidenceIntegrityError(label + " bytes differ from the selected artifact")
        else:
            unavailable.append("artifact sha256")
            continue
        path = None
        if isinstance(artifact.get("content_path"), str):
            path = scoped_path(root, artifact["content_path"])
        if path is None or not path.exists():
            file_id, version_id = artifact.get("file_id"), artifact.get("version_id")
            if all(
                isinstance(value, str) and re.fullmatch("[a-f0-9]{32}", value)
                for value in (file_id, version_id)
            ):
                candidate = (
                    root / "app" / "artifacts" / file_id / "versions" / version_id / "content"
                )
                if candidate.exists():
                    path = candidate
        if path is None or not path.exists():
            unavailable.append("artifact bytes:" + str(artifact.get("version_id")))
            continue
        if file_digest(path, limit=30 * 1024**2) != checksum:
            raise EvidenceIntegrityError("Actual artifact file hash differs")
    return unavailable


def _reviews(root, case, evidence):
    current = dict(evidence)
    used = []
    for source in evidence.get("review_sources") or []:
        if not isinstance(source, dict) or not valid_hash(source.get("sha256")):
            raise EvidenceIntegrityError("Saved review reference has no checksum")
        path = scoped_path(root, source["path"])
        if file_digest(path) != source["sha256"]:
            raise EvidenceIntegrityError("Saved review source changed since verification")
        used.append({**source, "path": str(path)})
    path = root / "reviews" / (case["id"] + ".json")
    if path.exists():
        value = read(path)
        annotations = value.get("reviews", value)
        if not isinstance(annotations, dict):
            raise EvidenceIntegrityError("Review payload is not a mapping")
        annotations = {key: item for key, item in annotations.items() if key in case["checks"]}
        hashes = {a.get("sha256") for a in evidence.get("artifacts", []) if isinstance(a, dict)}
        hashes.add(hashlib.sha256(str(evidence.get("answer_text", "")).encode()).hexdigest())
        for annotation in annotations.values():
            if not isinstance(annotation, dict):
                continue
            reference = annotation.get("evidence_ref", "")
            match = re.fullmatch(
                r"(?:decoded-output|artifact|sha256|actual-image|answer):([a-f0-9]{64})",
                str(reference),
            )
            if match and match[1] not in hashes:
                raise EvidenceIntegrityError("Review describes different artifact/answer bytes")
        current["reviews"] = {**(evidence.get("reviews") or {}), **annotations}
        if not any(item["path"] == str(path) for item in used):
            used.append(
                {"path": str(path), "sha256": file_digest(path), "type": "saved_explicit_review"}
            )
    return current, used


def summarize_run(run_root: Path, *, dataset_path: Path | None = None) -> dict:
    root = run_root.absolute()
    if root.resolve() != root or root.is_symlink():
        raise EvidenceIntegrityError("Run root is not canonical")
    dataset = read(
        (dataset_path or ROOT / "tests/fixtures/runtime_acceptance_cases.json").absolute()
    )
    if len(dataset.get("cases", [])) != 30 or digest(dataset["cases"]) != dataset.get(
        "cases_sha256"
    ):
        raise EvidenceIntegrityError(
            "Exactly the frozen 30 cases and their original checksum are required"
        )
    if read(root / "dataset.json") != dataset:
        raise EvidenceIntegrityError("Saved run changed the frozen dataset")
    case_ids = [case["id"] for case in dataset["cases"]]
    if len(set(case_ids)) != 30:
        raise EvidenceIntegrityError("Duplicated frozen case identity")
    config = read(root / "config.json")
    original = read(root / "manifest.json")
    _manifest(root, original, config, dataset)
    if config.get("group", {}).get("internal_id") != original.get("paid_budget_group"):
        raise EvidenceIntegrityError("Manifest and configured group identity differ")
    records_file = root / "results.json"
    results = read(records_file) if records_file.exists() else {"cases": []}
    results_hash = file_digest(records_file) if records_file.exists() else None
    records = results.get("cases", [])
    if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
        raise EvidenceIntegrityError("Results must contain case records")
    mapped = {row.get("id"): row for row in records}
    if len(mapped) != len(records) or not set(mapped) <= set(case_ids):
        raise EvidenceIntegrityError("Results contain duplicated or foreign case identities")
    revisions = {}
    for folder in sorted((root / "rechecks").glob("[0-9]*")):
        if not folder.is_dir() or not folder.name.isdigit():
            continue
        path = folder / "verification-manifest.json"
        if not path.exists():
            if (folder / "evidence").exists():
                raise EvidenceIntegrityError("Recheck evidence has no verification manifest")
            continue
        revision = int(folder.name)
        manifest = read(path)
        _manifest(root, manifest, config, dataset, original=original, revision=revision)
        revisions[revision] = {"manifest": manifest, "path": str(path), "sha256": file_digest(path)}
    config_verified = bool(original.get("config_sha256")) or any(
        info["manifest"].get("config_sha256") == digest(config) for info in revisions.values()
    )
    archives = []
    for folder in sorted((root / "revisions").glob("[0-9]*")):
        if not folder.is_dir():
            continue
        for name, value in (
            ("manifest.json", original),
            ("config.json", config),
            ("dataset.json", dataset),
        ):
            if (folder / name).exists() and read(folder / name) != value:
                raise EvidenceIntegrityError("Archived original metadata differs: " + name)
        if (folder / "results.json").exists():
            archives.append(
                {
                    "path": str(folder / "results.json"),
                    "sha256": file_digest(folder / "results.json"),
                }
            )
    report_cases = []
    source_versions = {original["source_sha256"]}
    source_versions.update(info["manifest"]["source_sha256"] for info in revisions.values())
    for case in dataset["cases"]:
        identifier = case["id"]
        record = mapped.get(identifier)
        item = {
            "id": identifier,
            "category": case["category"],
            "status": "NOT_RUN",
            "execution_state": record.get("execution_state") if record else "not_started",
            "saved_status": ((record or {}).get("evaluation") or {}).get("status"),
            "verification_revision": 0,
        }
        if record is None:
            item["reason"] = "No saved case result"
            report_cases.append(item)
            continue
        if record.get("category") not in (None, case["category"]):
            raise EvidenceIntegrityError("Recorded case category changed")
        candidates = []
        base = root / "evidence" / (identifier + ".json")
        if base.exists():
            candidates.append((0, base))
        if record.get("evidence_path"):
            selected = scoped_path(root, record["evidence_path"])
            if selected.name != identifier + ".json":
                raise EvidenceIntegrityError("Result evidence points to another case")
            if selected.exists():
                relative = selected.relative_to(root)
                revision = (
                    int(relative.parts[1])
                    if len(relative.parts) > 3
                    and relative.parts[0] == "rechecks"
                    and relative.parts[1].isdigit()
                    else 0
                )
                if revision and revision not in revisions:
                    raise EvidenceIntegrityError("Result references an unverified revision")
                candidates.append((revision, selected))
        for revision in revisions:
            path = root / "rechecks" / f"{revision:04d}" / "evidence" / (identifier + ".json")
            if path.exists():
                candidates.append((revision, path))
        if not candidates:
            item["status"] = "FAIL" if item["execution_state"] in STOPPED else "NOT_RUN"
            item["reason"] = "Saved evidence is absent"
            report_cases.append(item)
            continue
        revision, path = max(candidates, key=lambda value: value[0])
        saved_recheck = bool(
            revision
            and (
                revisions[revision]["manifest"].get("recheck_only") is True
                or (
                    record.get("verification_revision") == revision
                    and record.get("paid_calls_during_recheck") == 0
                )
            )
        )
        evidence = read(path)
        scope = evidence.get("scope") or {}
        if scope.get("group_id") != original["paid_budget_group"]:
            raise EvidenceIntegrityError("Case evidence belongs to another group")
        if record.get("work_id") and scope.get("work_id") != record["work_id"]:
            raise EvidenceIntegrityError("Case evidence belongs to another work")
        if evidence.get("case_id") not in (None, identifier):
            raise EvidenceIntegrityError("Evidence case identity changed")
        unavailable = _artifact_integrity(root, evidence)
        reviewed, review_sources = _reviews(root, case, evidence)
        evaluation = evaluate_case(case, reviewed)
        common_checks = {"all_delivery_parts_accepted": True}
        if case["category"] not in {"schedule_create", "schedule_update"}:
            common_checks["work_completed_after_accepted_delivery"] = True
        delivery_checks = evaluate_case({"id": identifier, "checks": common_checks}, reviewed)
        for key, check in delivery_checks["checks"].items():
            evaluation["checks"]["_common_" + key] = check
        for artifact in evidence.get("artifacts") or []:
            if isinstance(artifact, dict) and artifact.get("format") in {"docx", "pptx", "pdf"}:
                validation = artifact.get("validation") or {}
                previews = validation.get("preview_sha256")
                observed = all(
                    key in validation
                    for key in ("rendered", "layout_passed", "page_count", "preview_sha256")
                )
                passed = (
                    validation.get("rendered") is True
                    and validation.get("layout_passed") is True
                    and type(validation.get("page_count")) is int
                    and validation["page_count"] > 0
                    and isinstance(previews, (list, tuple))
                    and len(previews) == validation["page_count"]
                    and all(valid_hash(value) for value in previews)
                )
                evaluation["checks"]["_common_render_" + str(artifact.get("version_id"))] = {
                    "status": ("PASS" if passed else "FAIL") if observed else "NOT_RUN",
                    "detail": "Saved rendered/layout evidence and nonempty page hashes must cover the actual document",
                }
        evaluation["status"] = next(
            (
                status
                for status in ("FAIL", "NOT_RUN", "NEEDS_REVIEW")
                if any(check["status"] == status for check in evaluation["checks"].values())
            ),
            "PASS",
        )
        status = evaluation["status"]
        if status not in STATUSES:
            raise EvidenceIntegrityError("Checker returned an unknown status")
        complete_proof = bool(
            scope.get("work_id") and (evidence.get("delivery") or {}).get("work_completed_at")
        )
        completed = item["execution_state"] == "completed" or (
            saved_recheck and complete_proof and item["execution_state"] not in STOPPED
        )
        no_work_case = (
            case["category"] in {"schedule_create", "schedule_update"}
            and item["execution_state"] == "reply"
        )
        reason = "Current checks of latest saved evidence"
        if item["execution_state"] in STOPPED:
            status = "FAIL"
            reason = "Stopped execution is retained; partial outputs do not count as success"
        elif not (completed or no_work_case):
            status = "NOT_RUN"
            reason = "No saved completed-work or eligible rule-response evidence"
        elif unavailable or not config_verified:
            status = "NOT_RUN"
            reason = "Reproduction evidence is incomplete"
        elif status == "PASS" and item["saved_status"] != "PASS" and not saved_recheck:
            explicit_review = any(
                check.get("status") == "PASS"
                and check.get("detail", "").startswith("Explicit review: ")
                for check in evaluation["checks"].values()
            )
            if not (item["saved_status"] == "NEEDS_REVIEW" and explicit_review):
                status = "NEEDS_REVIEW"
                reason = "Old nonpassing checker record needs a saved verification revision before promotion"
        item.update(
            status=status,
            reason=reason,
            verification_revision=revision,
            saved_recheck=saved_recheck,
            evidence_path=str(path),
            evidence_sha256=file_digest(path),
            checks=evaluation["checks"],
            missing_evidence=unavailable,
            review_sources=review_sources,
            execution_source_sha256=record.get("original_execution_source_sha256")
            or record.get("execution_source_sha256")
            or original["source_sha256"],
        )
        if (
            not valid_hash(item["execution_source_sha256"])
            or item["execution_source_sha256"] not in source_versions
        ):
            raise EvidenceIntegrityError(
                "Case execution source has no matching original/revision manifest"
            )
        source_versions.add(item["execution_source_sha256"])
        report_cases.append(item)
    if results_hash is not None and file_digest(records_file) != results_hash:
        raise EvidenceIntegrityError(
            "Run results advanced while reading; retry the read-only summary"
        )

    def counts(rows):
        counter = Counter(row["status"] for row in rows)
        return {
            "denominator": len(rows),
            "statuses": {status: counter[status] for status in STATUSES},
            "successes": counter["PASS"],
            "success_rate": counter["PASS"] / len(rows) if rows else 0,
        }

    total = counts(report_cases)
    return {
        "schema_version": "groupagent.saved-acceptance-summary.v1",
        "run_id": root.name,
        "generated_at": datetime.now(UTC).isoformat(),
        **total,
        "status": next(
            (value for value in ("FAIL", "NOT_RUN", "NEEDS_REVIEW") if total["statuses"][value]),
            "PASS",
        ),
        "dataset_sha256": dataset["cases_sha256"],
        "config_sha256": digest(config),
        "config_hash_verified": config_verified,
        "manifest_sha256": file_digest(root / "manifest.json"),
        "results_sha256": results_hash,
        "checker_sha256": file_digest(ROOT / "scripts/runtime_acceptance_checks.py"),
        "source_versions": sorted(source_versions),
        "mixed_product_sources": len(source_versions) > 1,
        "verification_manifests": [revisions[key] for key in sorted(revisions)],
        "prior_results": archives,
        "by_category": {
            category: counts([row for row in report_cases if row["category"] == category])
            for category in sorted({case["category"] for case in dataset["cases"]})
        },
        "by_family": {
            family: counts([row for row in report_cases if row["category"] in categories])
            for family, categories in FAMILIES.items()
        },
        "cases": report_cases,
        "provider_calls": 0,
        "business_writes_applied": 0,
        "limitations": [
            "A saved local-channel result is not a real WeChat acceptance result.",
            "FAIL, NOT_RUN and NEEDS_REVIEW remain in the fixed denominator of 30.",
            "Mixed product/checker revisions are retained, not labeled a single-build unseen benchmark.",
            "Review attestations retain their stated reviewer kind; they are not assumed to be human adjudication.",
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        report = summarize_run(args.run)
    except (EvidenceIntegrityError, FileNotFoundError, json.JSONDecodeError) as error:
        parser.exit(2, str(error) + "\n")
    serialized = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        if args.output.resolve().is_relative_to(args.run.resolve()) and args.output.name in {
            "results.json",
            "manifest.json",
            "config.json",
            "dataset.json",
        }:
            parser.exit(2, "Refusing to replace original run metadata\n")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as output:
            output.write(serialized + "\n")
    else:
        print(serialized)
