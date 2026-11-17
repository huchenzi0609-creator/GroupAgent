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
