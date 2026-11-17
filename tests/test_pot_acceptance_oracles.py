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
    assert oracles.essential_artifact_input_reply(
        "你把库存卡拍张照发上来，或者直接念一遍编号。", "库存卡|编号"
    )
    assert not oracles.essential_artifact_input_reply(
        "你还没有发上来库存卡照片，编号也没有提供。", "库存卡|编号"
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


@pytest.mark.asyncio
@pytest.mark.parametrize("same_source", [True, False])
async def test_admission_driver_observes_native_ack_at_its_actual_time(
    oracles, tmp_path, same_source
):
    import asyncio
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from groupagent.channels import AgentWechatConfig
    from groupagent.channels.local import LocalChannel

    group = "synthetic-admission-driver"
    channel = LocalChannel(
        tmp_path / "spool",
        AgentWechatConfig(
            base_url="http://127.0.0.1",
            account_id="synthetic-account",
            external_group_id="local-only@chatroom",
            internal_group_id=group,
            source_epoch="driver-test",
        ),
    )
    source_id = "current-source"
    queued_source = source_id if same_source else "other-source"
    queued = False
    accepted = asyncio.Event()
    receipt = None
    finished = None

    async def reference_foreground(*, batched):
        nonlocal queued, finished
        assert not batched
        queued = True
        if same_source:
            await asyncio.wait_for(accepted.wait(), 2)
        else:
            await asyncio.sleep(0.08)
        finished = datetime.now(UTC)

    async def local_send():
        nonlocal receipt, queued
        if queued:
            queued = False
            receipt = await channel.send_text(
                group, "Accepted scoped work", f"early_ack:{queued_source}:1"
            )
            accepted.set()

    scenario = oracles.Scenario.__new__(oracles.Scenario)
    scenario.app = SimpleNamespace(
        process_pending=reference_foreground, dispatcher=SimpleNamespace(tick=local_send)
    )
    scenario.pending_admission = lambda candidate: queued and candidate == queued_source
    await scenario.process_with_admission_receipts(source_id)

    assert receipt is not None and finished is not None
    assert receipt.state == "accepted"
    assert await channel.lookup_delivery(group, receipt.idempotency_key) == receipt
    assert (receipt.observed_at <= finished) is same_source
    assert len(list((channel.root / "outgoing").glob("*.json"))) == 1


def test_review_oracle_matches_the_actual_fetched_page(oracles):
    urls = oracles.fetch_source_urls(
        {
            "source": {
                "url": "https://example.org/public/verified/",
                "final_url": "https://example.org/public/verified/",
                "content": "Unfetched link https://example.org/public/unobserved",
            },
        }
    )
    assert oracles.page_url_key("https://example.org/public/verified/#content") in urls
    assert oracles.page_url_key("https://example.org/public/unobserved") not in urls
    assert oracles.page_url_key("https://example.org/public/verified/?version=2") not in urls


def test_inspector_extracts_live_relationship_of_link_inside_docx_table(oracles, tmp_path):
    from docx import Document
    from docx.opc.constants import RELATIONSHIP_TYPE
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from runtime_acceptance_checks import _INSPECT

    document = Document()
    paragraph = document.add_table(rows=1, cols=1).cell(0, 0).paragraphs[0]
    address = "https://example.org/verified?case=one"
    rel_id = paragraph.part.relate_to(address, RELATIONSHIP_TYPE.HYPERLINK, is_external=True)
    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("r:id"), rel_id)
    run = OxmlElement("w:r")
    text = OxmlElement("w:t")
    text.text = "Source label"
    run.append(text)
    hyperlink.append(run)
    paragraph._p.append(hyperlink)

    path = tmp_path / "table-link.docx"
    document.save(path)
    decoded = json.loads(
        subprocess.check_output([sys.executable, "-c", _INSPECT, str(path), "docx"], text=True)
    )
    assert decoded["links"] == [address]


@pytest.mark.asyncio
async def test_driver_follows_its_event_through_older_work_and_retry(oracles):
    from types import SimpleNamespace

    queue = ["older-event", "target-event", "target-event", "later-event"]
    seen = []
    retries = {
        "attempts": 1,
        "code": "turn_execution_pending",
        "next_attempt_at": None,
    }

    def status(current):
        assert current == "target-event"
        attempts = seen.count(current)
        if attempts == 2:
            return {"state": "decided"}
        return {"state": "pending", "retry": retries if attempts else None}

    async def one_tick(*, batched):
        assert not batched
        seen.append(queue.pop(0))

    driver = oracles.Scenario.__new__(oracles.Scenario)
    driver.app = SimpleNamespace(process_pending=one_tick)
    driver.args = SimpleNamespace(timeout=2)
    driver.foreground_status = status
    observed = await driver.process_event_until_observed("target-event", dispatch=False)
    assert seen == ["older-event", "target-event", "target-event"]
    assert observed == {"state": "decided", "observed_retries": [retries]}
    assert queue == ["later-event"]


@pytest.mark.asyncio
async def test_driver_reports_pending_timeout_with_retry_reason(oracles):
    from types import SimpleNamespace

    retry = {"attempts": 1, "code": "turn_execution_pending", "next_attempt_at": None}

    async def no_ready_work(*, batched):
        assert not batched
        return 0

    driver = oracles.Scenario.__new__(oracles.Scenario)
    driver.app = SimpleNamespace(process_pending=no_ready_work)
    driver.args = SimpleNamespace(timeout=0.02)
    driver.foreground_status = lambda _: {"state": "pending", "retry": retry}
    observed = await driver.process_event_until_observed("target-event", dispatch=False)
    assert observed == {"state": "observation_timeout", "retry": retry, "observed_retries": [retry]}


def test_unfinished_foreground_blocks_blind_pass_even_for_turn_without_latency_tag(oracles):
    case = {
        "level": "deepseek_local",
        "checks": {"witness": {"status": "passed", "evidence": [{"persisted": True}]}},
        "model_rounds": [{"status": "returned"}],
        "turns": [{"foreground": {"state": "observation_timeout"}, "latency_required": False}],
    }
    assert oracles.blind_status(case, {"witness"}) == "blocked"
    case["turns"] = [{"foreground": {"state": "decided"}, "latency_required": False}]
    assert oracles.blind_status(case, {"witness"}) == "passed"
