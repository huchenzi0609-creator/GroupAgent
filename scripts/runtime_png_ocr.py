"""Optional independent OCR of completed, validated acceptance PNGs.

Dry inspection is the default and never loads credentials. Only the operator
invokes --execute after freezing this verifier. The existing cumulative 50 CNY
ledger is reused; completed product work is never reopened or modified. No gold,
expected label, artifact name, or prompt is sent to the OCR provider.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from groupagent.billing.models import Reservation
from groupagent.domain import DomainError
from groupagent.media.models import Capability, ProviderRoute
from groupagent.media.providers import MediaHTTPProvider
from groupagent.media.validation import inspect_image

ROOT = Path(__file__).resolve().parents[1]
PRICE_ID = "deepseek-20261114-media.ocr"


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _file(path: Path, root: Path, limit: int = 64 * 1024**2) -> bytes:
    if (
        not path.is_relative_to(root)
        or path.resolve() != path.absolute()
        or not path.is_file()
        or path.stat().st_size > limit
    ):
        raise RuntimeError("OCR input must be a bounded regular file in this run")
    return path.read_bytes()


def _json(path: Path, root: Path) -> dict:
    value = json.loads(_file(path, root))
    if not isinstance(value, dict):
        raise RuntimeError("OCR metadata must be an object")
    return value


def _ref(path: Path, root: Path) -> dict:
    return {"path": str(path), "sha256": hashlib.sha256(_file(path, root)).hexdigest()}


def select_png(root: Path, case_id: str, file_id: str, version_id: str) -> tuple[dict, bytes]:
    """Read actual immutable PNG bytes; neither dataset nor gold is opened."""
    root = root.absolute()
    if root.resolve() != root or not re.fullmatch(r"[A-Za-z0-9_-]+", case_id):
        raise RuntimeError("Invalid acceptance run/case path")
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", value) for value in (file_id, version_id)):
        raise RuntimeError("Invalid immutable artifact identity")
    evidence_path, progress_path = (
        root / "evidence" / (case_id + ".json"),
        root / "progress" / (case_id + ".json"),
    )
    evidence, progress = _json(evidence_path, root), _json(progress_path, root)
    manifest = _json(root / "manifest.json", root)
    scope = evidence.get("scope", {})
    if (
        progress.get("execution_state") != "completed"
        or progress.get("case_id") != case_id
        or not all(scope.get(key) for key in ("group_id", "work_id", "event_id", "requester_id"))
        or any(progress.get(key) != scope[key] for key in ("work_id", "event_id"))
        or progress.get("source_sha256") != manifest.get("source_sha256")
        or manifest.get("channel") != "local_only"
        or manifest.get("paid_budget_group") != scope["group_id"]
        or manifest.get("total_cny_limit") != 50
        or not evidence.get("delivery", {}).get("work_completed_at")
    ):
        raise RuntimeError("Only this run's completed local acceptance work can be observed")
    candidates = [
        item
        for item in evidence.get("artifacts", [])
        if item.get("file_id") == file_id and item.get("version_id") == version_id
    ]
    if len(candidates) != 1:
        raise RuntimeError("Select exactly one existing immutable output version")
    artifact = candidates[0]
    validation, decoded = artifact.get("validation", {}), artifact.get("decoded", {})
    if (
        artifact.get("format") != "png"
        or any(artifact.get(key) != scope[key] for key in ("group_id", "work_id"))
        or validation.get("passed") is not True
        or validation.get("sha256") != artifact.get("sha256")
        or decoded.get("source_sha256") != artifact.get("sha256")
        or decoded.get("image", {}).get("decoded") is not True
    ):
        raise RuntimeError("PNG needs matching original scope, validation, and decoded evidence")
    version_root = root / "app/artifacts" / file_id / "versions" / version_id
    version_path = version_root / "version.json"
    version = _json(version_path, root)
    for key in ("file_id", "version_id", "sha256", "size_bytes", "group_id", "work_id", "format"):
        if version.get(key) != artifact.get(key):
            raise RuntimeError("Immutable artifact record differs from acceptance evidence")
    validation_path = (
        root / "app/artifacts" / file_id / "validations" / (validation["validation_id"] + ".json")
    )
    if _json(validation_path, root) != validation:
        raise RuntimeError("Original artifact validation record differs")
    raw = _file(version_root / "content", root, 15 * 1024**2)
    if (
        hashlib.sha256(raw).hexdigest() != artifact["sha256"]
        or len(raw) != artifact["size_bytes"]
        or _file(Path(artifact["content_path"]), root, 15 * 1024**2) != raw
    ):
        raise RuntimeError("Actual immutable/exported PNG bytes differ")
    info, clean_png = inspect_image(raw)
    if (
        info.format != "png"
        or info.frame_count != 1
        or info.pixel_sha256 != decoded["image"].get("pixel_sha256")
    ):
        raise RuntimeError("Actual PNG pixels differ from the original decoded observation")
    selected = {
        "case_id": case_id,
        "scope": scope,
        "source_evidence": _ref(evidence_path, root),
        "source_progress": _ref(progress_path, root),
        "source_manifest": _ref(root / "manifest.json", root),
        "immutable_version_record": _ref(version_path, root),
        "source": {
            **{key: artifact[key] for key in ("file_id", "version_id", "sha256", "size_bytes")},
            "pixel_sha256": info.pixel_sha256,
            "validation_id": validation["validation_id"],
        },
        "input_sha256": hashlib.sha256(clean_png).hexdigest(),
        "input_pixel_sha256": info.pixel_sha256,
        "input_filename": "image.png",
        "original_local_ocr_text": decoded["image"].get("ocr_text"),
    }
    return selected, clean_png


async def observe(selected, content, actor, ledger, provider) -> dict:
    """One admission and one OCR attempt; failures retain their reservation."""
    scope, source = selected["scope"], selected["source"]
    if actor.group_id != scope["group_id"] or actor.member_id != scope["requester_id"]:
        raise RuntimeError("OCR operator differs from the original acceptance requester")
    if hashlib.sha256(content).hexdigest() != selected["input_sha256"]:
        raise RuntimeError("OCR input changed after selection")
    if getattr(ledger, "fixture_only", False) and not provider.fixture_transport:
        raise RuntimeError("Fixture accounting cannot authorize a real provider")
    route = ProviderRoute(
        route_id="independent-acceptance-ocr",
        provider="deepseek",
        capability=Capability.OCR,
        model="tools/ocr",
        price_id=PRICE_ID,
    )
    key = f"acceptance-ocr:{selected['case_id']}:{source['file_id']}:{source['version_id']}:{source['sha256']}"
    result = {
        **selected,
        "kind": "independent_machine_ocr",
        "observer": "scripts/runtime_png_ocr.py",
        "method": "deepseek_ocr",
        "provider": "deepseek",
        "model": "tools/ocr",
        "transport": "offline_fixture" if provider.fixture_transport else "live_http",
        "request_contains_gold_or_prompt": False,
        "observed_at": datetime.now(UTC).isoformat(),
        "status": "failed",
        "untrusted_content": True,
        "business_writes_applied": False,
    }
    reservation = ledger.reserve(
        actor,
        key,
        "media.ocr",
        PRICE_ID,
        {"pages": Decimal(1)},
        # A completed work cannot authorize further execution. This separately
        # authorized read-only observation uses the SAME group ledger/source
        # event and retains the completed work solely as immutable provenance.
        source_event_id=scope["event_id"],
    )
    if not reservation.is_new or not ledger.begin(actor, reservation.id):
        raise RuntimeError("Existing OCR attempt requires reconciliation; do not replay it")
    result["reservation_id"] = reservation.id
    try:
        response = await provider.ocr(route, content, "image.png")
        payload = response.model_dump(mode="json", exclude={"content"})
        result["provider_response"] = payload
        result["provider_response_sha256"] = canonical_hash(payload)
        if (
            not response.request_id
            or len(response.pages) != 1
            or response.pages[0].method != "deepseek_ocr"
            or not response.pages[0].complete
            or response.usage != {"pages": 1.0}
        ):
            raise DomainError(
                "invalid_ocr_observation", "OCR response lacks a complete page/receipt"
            )
        settled = ledger.settle(
            actor, reservation.id, {"pages": Decimal(1)}, "deepseek:" + response.request_id
        )
        if settled.overrun:
            raise DomainError("budget_overrun", "OCR exceeded its admitted amount")
        result.update(
            status="completed",
            text=response.pages[0].text,
            usage=response.usage,
            usage_method="complete_page_count_from_provider_response",
            budget_receipt=settled.model_dump(mode="json"),
            budget_evidence_ref="deepseek:" + response.request_id,
            cost_is_conservative_ledger_estimate_not_provider_debit=True,
        )
    except BaseException as error:
        ledger.unknown(actor, reservation.id, "independent_ocr_incomplete")
        result["error_code"] = (
            error.code if isinstance(error, DomainError) else type(error).__name__
        )
        if isinstance(error, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            raise
    return result


def save_new(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.resolve() != path.parent.absolute():
        raise RuntimeError("OCR output cannot traverse a symlink")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.flush()
        os.fsync(output.fileno())


def attach_observations(root: Path, case_id: str, evidence: dict) -> dict:
    """Load saved observations without calls; preserve original decoded OCR."""
    paths = sorted((root / "ocr_observations" / case_id).glob("*.json"))
    if not paths:
        return evidence
    observations, references = [], []
    for path in paths:
        observation = _json(path, root)
        source = observation.get("source", {})
        selected, _ = select_png(
            root, case_id, source.get("file_id", ""), source.get("version_id", "")
        )
        if any(observation.get(key) != value for key, value in selected.items()):
            raise RuntimeError("Saved OCR observation no longer matches its immutable source")
        if observation["scope"] != evidence.get("scope"):
            raise RuntimeError("Saved OCR observation belongs to another case scope")
        observations.append(observation)
        references.append({**_ref(path, root), "payload_sha256": canonical_hash(observation)})
    return {
        **evidence,
        "independent_ocr_observations": observations,
        "independent_ocr_observation_sources": references,
    }


async def main(args) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id):
        raise RuntimeError("Invalid run ID")
    root = ROOT / "runtime/live" / args.run_id
    selected, content = select_png(root, args.case_id, args.file_id, args.version_id)
    path = root / "ocr_observations" / args.case_id / (args.version_id + ".json")
    if path.exists():
        raise RuntimeError("OCR observation already exists; preserve it and reconcile")
    if not args.execute:
        print(json.dumps({"mode": "dry_inspection_no_credentials", **selected}, ensure_ascii=False))
        return
    from live_preflight import setup

    store, ledger, actor, key = setup()
    provider = MediaHTTPProvider("deepseek", key.get_secret_value())
    try:
        result = await observe(selected, content, actor, ledger, provider)
        if result.get("reservation_id"):
            # Persist the actual stored receipt, including its usage evidence ref.
            with store.transaction(actor) as tx:
                row = tx.conn.execute(
                    "SELECT * FROM ga_budget_reservations WHERE group_id=%s AND id=%s",
                    (actor.group_id, result["reservation_id"]),
                ).fetchone()
            if row:
                receipt = Reservation.model_validate(
                    {name: row[name] for name in Reservation.model_fields if name in row}
                ).model_dump(mode="json")
                receipt["evidence_ref"] = row["evidence_ref"]
                result["budget_receipt"] = receipt
                result["budget_receipt_read_from_database"] = True
        result["budget_after"] = ledger.report(actor)
        save_new(path, result)
        print(json.dumps({"status": result["status"], "path": str(path)}))
    finally:
        await provider.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--file-id", required=True)
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--execute", action="store_true", help="Operator-only paid OCR attempt")
    asyncio.run(main(parser.parse_args()))
