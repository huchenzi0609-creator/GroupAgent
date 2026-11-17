"""Independent evidence-matching regressions for the Pot release gate."""

from __future__ import annotations

import importlib
import io
import json
import subprocess
import sys
from pathlib import Path

import pytest
from PIL import Image


@pytest.fixture
def oracles(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    return importlib.import_module("pot_blind_acceptance")


def native_text(message_id, text, *, state="accepted"):
    return {
        "receipt": {"platform_message_id": message_id, "state": state},
        "payload": {"kind": "text", "text": text},
    }


def test_scheduled_proof_joins_the_native_acceptance_instead_of_matching_text(oracles):
    facts = [
        {
            "occurrences": [
                {
                    "deliveries": [
                        {"state": "accepted", "platform_message_id": "actual-occurrence"},
                        {"state": "unknown", "platform_message_id": "unknown-occurrence"},
                    ]
                }
            ]
        }
    ]
    accepted = native_text("actual-occurrence", "Text differs from the acknowledgement")
    unrelated = native_text("another-message", "Literal reminder text", state="accepted")
    unknown = native_text("unknown-occurrence", "Another matching body", state="accepted")
    assert oracles.schedule_native_receipts(facts, [accepted, unrelated, unknown]) == [accepted]


def test_clock_compare_honors_requested_precision_without_prescribing_format(oracles):
    assert oracles.clock_display("现在4点6分8秒。", "04:06:08", seconds_required=True)
    assert oracles.clock_display("当前是04：06。", "04:06:08", seconds_required=False)
    assert not oracles.clock_display("当前是04:06。", "04:06:08", seconds_required=True)
    assert not oracles.clock_display("当前是04:06:01。", "04:06:08", seconds_required=False)


def test_honest_work_state_and_essential_information_are_not_boilerplate(oracles):
    assert oracles.useful_text({"receipts": [native_text("one", "目前没有我正在处理的工作。")]})
    assert not oracles.useful_text({"receipts": [native_text("two", "正在处理。")]})
    assert oracles.essential_artifact_input_reply(
        "把库存卡拍一下发来，我才能读出编号。", "库存卡|编号"
    )
    assert not oracles.essential_artifact_input_reply(
        "库存卡编号已经做成文档发给大家。", "库存卡|编号"
    )


def test_matched_work_fact_requires_exact_identity_and_version(oracles):
    expected = [{"id": "one", "goal_revision": 2}, {"id": "two", "goal_revision": 3}]
    assert oracles.work_fact_has([{"id": "two", "goal_revision": 3}], "two", expected)
    assert not oracles.work_fact_has([{"id": "one", "goal_revision": 2}], "two", expected)
    assert not oracles.work_fact_has([{"id": "two", "goal_revision": 2}], "two", expected)


def test_inspector_counts_actual_png_embeddings_instead_of_alt_text(oracles, tmp_path):
    from docx import Document
    from runtime_acceptance_checks import _INSPECT

    document = Document()
    document.add_paragraph("First image is intentionally embedded twice")
    for color in ("red", "red", "blue"):
        stream = io.BytesIO()
        Image.new("RGB", (12, 12), color).save(stream, format="PNG")
        stream.seek(0)
        document.add_picture(stream)
    path = tmp_path / "images.docx"
    document.save(path)
    decoded = json.loads(
        subprocess.check_output([sys.executable, "-c", _INSPECT, str(path), "docx"], text=True)
    )
    images = decoded["embedded_images"]
    assert len(images) == 3
    assert {row["content_type"] for row in images} == {"image/png"}
    assert len({row["sha256"] for row in images}) == 2


def test_chat_count_accepts_spacing_without_changing_value(oracles):
    witnessed = {"receipts": [native_text("one", "它有 12 条边。")]}
    wrong = {"receipts": [native_text("two", "它有 13 条边。")]}
    assert oracles.useful_text(witnessed, [r"12条边"], compact_whitespace=True)
    assert not oracles.useful_text(wrong, [r"12条边"], compact_whitespace=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2])
async def test_case_concurrency_keeps_the_report_and_evidence_separate(
    oracles, monkeypatch, tmp_path, concurrency
):
    import asyncio
    import hashlib
    from types import SimpleNamespace

    table = {
        identity: {"blind_level": "isolated_infrastructure", "blind_checks": ["witness"]}
        for identity in ("probe-first", "probe-second", "probe-third")
    }
    dataset = tmp_path / "sealed.json"
    dataset.write_text(
        json.dumps({"format_version": 1, "cases": {identity: {} for identity in table}})
    )
    dataset.chmod(0o600)
    lock = tmp_path / "lock.json"
    lock.write_text(
        json.dumps(
            {
                "dataset_sha256": hashlib.sha256(dataset.read_bytes()).hexdigest(),
                "case_ids": list(table),
            }
        )
    )
    dsn_file = tmp_path / "fixture.dsn"
    dsn_file.write_text("offline test fixture")
    monkeypatch.setattr(oracles, "coverage", lambda: table)
    monkeypatch.setattr(oracles, "BLIND_LOCK", lock)
    monkeypatch.setattr(oracles, "bindings", lambda: {"source": {"sha256": "fixture-ci"}})
    monkeypatch.setattr(oracles, "PostgresStore", lambda _: object())
    monkeypatch.setattr(oracles, "postgres_identity", lambda _: {"database": "pot_blind"})
    active: set[str] = set()
    peak = 0

    async def probe(args, identity, destination, spec, row):
        nonlocal peak
        active.add(identity)
        peak = max(peak, len(active))
        assert destination.name == identity
        await asyncio.sleep(0.01)
        row["checks"]["witness"] = {"status": "passed", "evidence": [{"probe": identity}]}
        active.remove(identity)

    monkeypatch.setattr(oracles, "evaluate_infrastructure", probe)
    args = SimpleNamespace(
        dataset=dataset,
        output=tmp_path / "report",
        dsn_file=dsn_file,
        worker_image="sha256:" + "a" * 64,
        timeout=1,
        cases=None,
        model="offline-test",
        vision_model="offline-test",
        asr_model="offline-test",
        concurrency=concurrency,
    )
    assert await oracles.run(args) == 0
    assert peak == concurrency
    report = json.loads((args.output / "report.json").read_text())
    assert report["environment"]["case_concurrency"] == concurrency
    assert [item["id"] for item in report["cases"]] == list(table)
    for result in report["cases"]:
        evidence = oracles.read_json(args.output / result["id"] / "evidence.json")
        assert evidence["checks"]["witness"]["evidence"] == [{"probe": result["id"]}]
