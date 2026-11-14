"""Offline verifier fixtures only: no credentials, database, Docker, or paid calls."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image, PngImagePlugin
from pydantic import SecretStr

from groupagent.billing.models import Reservation
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.media.models import OCRPage, ProviderResponse
from groupagent.media.providers import MediaHTTPProvider
from groupagent.media.validation import inspect_image

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


OCR = module("runtime_png_ocr")
CHECKS = module("runtime_acceptance_checks")
CASE = {"id": "CASE-01", "checks": {"image_ocr_required_text": ["Alpha", "Beta"]}}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def run(tmp_path):
    root = tmp_path / "runtime/live/run"
    raster = Image.new("RGB", (600, 400), "white")
    raster.putpixel((0, 0), (0, 0, 0))
    metadata = PngImagePlugin.PngInfo()
    metadata.add_text("untrusted_metadata", "expected text must never be sent")
    buffer = io.BytesIO()
    raster.save(buffer, format="PNG", pnginfo=metadata)
    raw = buffer.getvalue()
    info, _ = inspect_image(raw)
    scope = {"group_id": "g", "work_id": "w", "event_id": "e", "requester_id": "m"}
    validation = {
        "validation_id": "validation",
        "passed": True,
        "sha256": info.source_sha256,
        "file_id": "f",
        "version_id": "v",
    }
    version = {
        "file_id": "f",
        "version_id": "v",
        "sha256": info.source_sha256,
        "size_bytes": len(raw),
        "group_id": "g",
        "work_id": "w",
        "format": "png",
    }
    content = root / "app/artifacts/f/versions/v/content"
    content.parent.mkdir(parents=True)
    content.write_bytes(raw)
    exported = root / "outputs/CASE-01/output.png"
    exported.parent.mkdir(parents=True)
    exported.write_bytes(raw)
    artifact = {
        **version,
        "name": "untrusted-name.png",
        "content_path": str(exported),
        "validation": validation,
        "decoded": {
            "source_sha256": info.source_sha256,
            "image": {"decoded": True, "pixel_sha256": info.pixel_sha256, "ocr_text": ""},
        },
    }
    evidence = {
        "scope": scope,
        "artifacts": [artifact],
        "delivery": {"work_completed_at": "2030-01-01T00:00:00Z"},
    }
    write(content.parent / "version.json", version)
    write(root / "app/artifacts/f/validations/validation.json", validation)
    write(root / "evidence/CASE-01.json", evidence)
    write(
        root / "progress/CASE-01.json",
        {
            "case_id": "CASE-01",
            "execution_state": "completed",
            "work_id": "w",
            "event_id": "e",
            "source_sha256": "1" * 64,
        },
    )
    write(
        root / "manifest.json",
        {
            "source_sha256": "1" * 64,
            "channel": "local_only",
            "paid_budget_group": "g",
            "total_cny_limit": 50,
        },
    )
    return root, evidence


class OfflineLedgerFixture:
    def __init__(self):
        self.reservation = None
        self.attempted = self.settlements = self.unknowns = 0

    def reserve(self, actor, key, operation, price, estimates, **kwargs):
        assert kwargs == {"source_event_id": "e"}  # Never reopen completed work.
        self.reservation = Reservation(
            id="offline-receipt",
            group_id=actor.group_id,
            actor_id=actor.member_id,
            key=key,
            operation=operation,
            price_id=price,
            currency="CNY",
            state="reserved",
            reserved_amount=Decimal("0.0015"),
            estimated_units=estimates,
            source_event_id="e",
            policy_revision=1,
            created_at=datetime.now(UTC),
            is_new=True,
        )
        return self.reservation

    def begin(self, actor, identifier):
        self.attempted += 1
        return True

    def settle(self, actor, identifier, usage, evidence_ref):
        self.settlements += 1
        assert evidence_ref == "deepseek:offline-provider-fixture"
        return self.reservation.model_copy(
            update={
                "state": "settled",
                "actual_amount": Decimal("0.0015"),
                "actual_units": usage,
                "is_new": False,
            }
        )

    def unknown(self, actor, identifier, reason):
        self.unknowns += 1


class OfflineProviderFixture:
    # Only protocol-shape tests use this flag. No HTTP client exists in this fake.
    fixture_transport = False

    def __init__(self, *, error=None):
        self.calls, self.error = [], error

    async def ocr(self, route, content, filename):
        self.calls.append((route, content, filename))
        if self.error:
            raise self.error
        return ProviderResponse(
            request_id="offline-provider-fixture",
            pages=(OCRPage(page_number=1, text="Alpha Beta", method="deepseek_ocr"),),
            usage={"pages": 1.0},
        )


async def observation_fixture(root):
    selected, png = OCR.select_png(root, "CASE-01", "f", "v")
    actor = ActorContext(group_id="g", member_id="m", role=Role.OWNER)
    ledger, provider = OfflineLedgerFixture(), OfflineProviderFixture()
    observation = await OCR.observe(selected, png, actor, ledger, provider)
    # This unit fixture explicitly simulates the DB readback shape; it is never
    # persisted under runtime/live or reported as real provider evidence.
    observation["budget_receipt"]["evidence_ref"] = observation["budget_evidence_ref"]
    observation["budget_receipt_read_from_database"] = True
    return observation, ledger, provider


def attach_fixture(evidence, observation):
    return {
        **deepcopy(evidence),
        "independent_ocr_observations": [observation],
        "independent_ocr_observation_sources": [
            {
                "path": "/offline-fixture-only.json",
                "sha256": "9" * 64,
                "payload_sha256": OCR.canonical_hash(observation),
            }
        ],
    }


def test_selection_decodes_actual_pixels_and_strips_untrusted_png_metadata(run):
    root, evidence = run
    selected, png = OCR.select_png(root, "CASE-01", "f", "v")
    assert b"expected text must never be sent" not in png
    assert selected["source"]["sha256"] == evidence["artifacts"][0]["sha256"]
    assert selected["input_sha256"] != selected["source"]["sha256"]
    assert selected["input_pixel_sha256"] == selected["source"]["pixel_sha256"]


@pytest.mark.parametrize("state", ["paused", "failed", "not_run", "running"])
def test_uncompleted_work_cannot_start_ocr(run, state):
    root, _ = run
    path = root / "progress/CASE-01.json"
    progress = json.loads(path.read_text())
    progress["execution_state"] = state
    write(path, progress)
    with pytest.raises(RuntimeError, match="completed"):
        OCR.select_png(root, "CASE-01", "f", "v")


@pytest.mark.parametrize("failure", ["validation", "scope", "pixels", "bytes", "symlink"])
def test_selection_rejects_unvalidated_or_changed_artifacts(run, failure):
    root, evidence = run
    artifact = evidence["artifacts"][0]
    if failure == "validation":
        artifact["validation"]["passed"] = False
    elif failure == "scope":
        artifact["work_id"] = "another"
    elif failure == "pixels":
        artifact["decoded"]["image"]["pixel_sha256"] = "0" * 64
    elif failure == "bytes":
        Path(artifact["content_path"]).write_bytes(b"changed")
    else:
        path = root / "app/artifacts/f/versions/v/content"
        target = path.with_name("copy")
        path.rename(target)
        path.symlink_to(target)
    write(root / "evidence/CASE-01.json", evidence)
    with pytest.raises(RuntimeError):
        OCR.select_png(root, "CASE-01", "f", "v")


async def test_one_provider_attempt_has_no_prompt_or_gold_and_uses_original_ledger_scope(run):
    root, _ = run
    observation, ledger, provider = await observation_fixture(root)
    assert observation["status"] == "completed"
    assert ledger.attempted == ledger.settlements == 1 and ledger.unknowns == 0
    assert len(provider.calls) == 1 and provider.calls[0][2] == "image.png"
    assert provider.calls[0][0].price_id == OCR.PRICE_ID
    assert hashlib.sha256(provider.calls[0][1]).hexdigest() == observation["input_sha256"]


async def test_existing_http_adapter_sends_only_clean_png_and_fixed_ocr_purpose(run):
    root, _ = run
    selected, png = OCR.select_png(root, "CASE-01", "f", "v")
    captured = []

    def reply(request):
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "id": "offline-provider-fixture",
                "page_count": 1,
                "pages": [{"index": 0, "text": "Alpha Beta", "finish_reason": "stop"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        provider = MediaHTTPProvider("deepseek", "synthetic-key", client=client)
        result = await OCR.observe(
            selected,
            png,
            ActorContext(group_id="g", member_id="m", role=Role.OWNER),
            OfflineLedgerFixture(),
            provider,
        )
    assert len(captured) == 1
    request = captured[0]
    assert request.url == "https://api.deepseek.com/v1/tools/ocr"
    assert png in request.content and b'filename="image.png"' in request.content
    assert b"text_recognition" in request.content
    assert b"expected text must never be sent" not in request.content
    assert b"untrusted-name" not in request.content and b"Alpha Beta" not in request.content
    assert result["transport"] == "offline_fixture"


async def test_provider_failure_keeps_unknown_reservation_and_never_retries(run):
    root, _ = run
    selected, png = OCR.select_png(root, "CASE-01", "f", "v")
    ledger = OfflineLedgerFixture()
    provider = OfflineProviderFixture(error=DomainError("provider_request_failed", "fixture"))
    actor = ActorContext(group_id="g", member_id="m", role=Role.OWNER)
    result = await OCR.observe(selected, png, actor, ledger, provider)
    assert result["status"] == "failed" and result["error_code"] == "provider_request_failed"
    assert ledger.attempted == ledger.unknowns == len(provider.calls) == 1
    assert ledger.settlements == 0 and "text" not in result


async def test_extra_machine_ocr_can_pass_but_original_local_failure_stays_visible(run):
    root, evidence = run
    assert (
        CHECKS.evaluate_case(CASE, evidence)["checks"]["image_ocr_required_text"]["status"]
        == "FAIL"
    )
    observation, _, _ = await observation_fixture(root)
    attached = attach_fixture(evidence, observation)
    result = CHECKS.evaluate_case(CASE, attached)
    assert result["status"] == "PASS"
    check = result["checks"]["image_ocr_required_text"]
    assert check["original_local_observation"]["status"] == "FAIL"
    assert evidence["artifacts"][0]["decoded"]["image"]["ocr_text"] == ""


@pytest.mark.parametrize(
    "failure",
    [
        "receipt_missing",
        "receipt_scope",
        "receipt_unsettled",
        "receipt_readback",
        "source_sha",
        "pixel_sha",
        "input_pixels",
        "scope",
        "version",
        "case",
        "human",
        "response_hash",
        "receipt_provider",
        "fixture_transport",
        "claimed_text",
    ],
)
async def test_unbound_or_unreceipted_ocr_cannot_pass(run, failure):
    root, evidence = run
    observation, _, _ = await observation_fixture(root)
    if failure == "receipt_missing":
        observation.pop("budget_receipt")
    elif failure == "receipt_scope":
        observation["budget_receipt"]["group_id"] = "other"
    elif failure == "receipt_unsettled":
        observation["budget_receipt"]["state"] = "unknown"
    elif failure == "receipt_readback":
        observation.pop("budget_receipt_read_from_database")
    elif failure == "source_sha":
        observation["source"]["sha256"] = "0" * 64
    elif failure == "pixel_sha":
        observation["source"]["pixel_sha256"] = "0" * 64
    elif failure == "input_pixels":
        observation["input_pixel_sha256"] = "0" * 64
    elif failure == "scope":
        observation["scope"]["work_id"] = "other"
    elif failure == "version":
        observation["source"]["version_id"] = "other"
    elif failure == "case":
        observation["case_id"] = "OTHER-01"
    elif failure == "human":
        observation["kind"] = "independent_assistant_review"
    elif failure == "response_hash":
        observation["provider_response"]["pages"][0]["text"] = "Alpha Beta altered"
    elif failure == "receipt_provider":
        observation["budget_receipt"]["evidence_ref"] = "deepseek:another-request"
    elif failure == "fixture_transport":
        observation["transport"] = "offline_fixture"
    else:
        observation["text"] = "Alpha Beta metadata claim only"
    result = CHECKS.evaluate_case(CASE, attach_fixture(evidence, observation))
    assert result["status"] == "FAIL"
    assert result["checks"]["_independent_ocr_observation"]["status"] == "FAIL"
    assert result["checks"]["image_ocr_required_text"]["status"] == "FAIL"


async def test_saved_observation_binding_rejects_later_mutation_and_never_overwrites(run):
    root, evidence = run
    original = (root / "evidence/CASE-01.json").read_bytes()
    observation, _, _ = await observation_fixture(root)
    path = root / "ocr_observations/CASE-01/v.json"
    OCR.save_new(path, observation)
    attached = OCR.attach_observations(root, "CASE-01", evidence)
    assert CHECKS.evaluate_case(CASE, attached)["status"] == "PASS"
    assert (root / "evidence/CASE-01.json").read_bytes() == original
    with pytest.raises(FileExistsError):
        OCR.save_new(path, observation)
    mutated = deepcopy(observation)
    mutated["source_evidence"]["sha256"] = "0" * 64
    write(path, mutated)
    with pytest.raises(RuntimeError, match="immutable"):
        OCR.attach_observations(root, "CASE-01", evidence)


async def test_default_cli_inspection_never_bootstraps_credentials(
    run, tmp_path, monkeypatch, capsys
):
    def forbidden():
        raise AssertionError("No credentials, providers, or budget mutation in dry inspection")

    monkeypatch.setattr(OCR, "ROOT", tmp_path)
    monkeypatch.setitem(sys.modules, "live_preflight", SimpleNamespace(setup=forbidden))
    await OCR.main(
        SimpleNamespace(run_id="run", case_id="CASE-01", file_id="f", version_id="v", execute=False)
    )
    assert json.loads(capsys.readouterr().out)["mode"] == "dry_inspection_no_credentials"


async def test_execute_bootstrap_unwraps_actual_secretstr_only_for_existing_http_adapter(
    run, tmp_path, monkeypatch, capsys
):
    root, _ = run
    actor = ActorContext(group_id="g", member_id="m", role=Role.OWNER)
    secret = SecretStr("synthetic-bootstrap-secret")
    captured = []

    class BootstrapLedger(OfflineLedgerFixture):
        def settle(self, *args):
            self.settled = super().settle(*args)
            return self.settled

        def report(self, actor):
            return {"group_id": actor.group_id, "offline_fixture": True}

    ledger = BootstrapLedger()

    @contextmanager
    def transaction(actor):
        row = {**ledger.settled.model_dump(), "evidence_ref": "deepseek:offline-provider-fixture"}
        yield SimpleNamespace(
            conn=SimpleNamespace(execute=lambda *args: SimpleNamespace(fetchone=lambda: row))
        )

    store = SimpleNamespace(transaction=transaction)

    def response(request):
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "id": "offline-provider-fixture",
                "page_count": 1,
                "pages": [{"index": 0, "text": "Alpha Beta", "finish_reason": "stop"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:

        def provider_factory(name, key):
            # Passing the actual SecretStr here reproduced the original failure
            # before this fix; use the real adapter constructor, not a provider fake.
            assert isinstance(key, str)
            return MediaHTTPProvider(name, key, client=client)

        monkeypatch.setattr(OCR, "ROOT", tmp_path)
        monkeypatch.setattr(OCR, "MediaHTTPProvider", provider_factory)
        monkeypatch.setitem(
            sys.modules,
            "live_preflight",
            SimpleNamespace(setup=lambda: (store, ledger, actor, secret)),
        )
        await OCR.main(
            SimpleNamespace(
                run_id="run", case_id="CASE-01", file_id="f", version_id="v", execute=True
            )
        )
    output = capsys.readouterr().out
    saved = (root / "ocr_observations/CASE-01/v.json").read_text()
    assert json.loads(output)["status"] == "completed"
    assert len(captured) == ledger.attempted == ledger.settlements == 1
    assert captured[0].headers["Authorization"] == "Bearer " + secret.get_secret_value()
    assert secret.get_secret_value() not in output + saved
    assert json.loads(saved)["budget_receipt_read_from_database"] is True
