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
    worker_image = "sha256:" + "a" * 64
    docker_host = "unix:///synthetic-oracle-fixture"
    worker_proof = {"sha256": "b" * 64, "files": 3}

    def worker_source(image, host):
        assert (image, host) == (worker_image, docker_host)
        return worker_proof

    monkeypatch.setattr(oracles, "observe_worker_source", worker_source)
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
        worker_image=worker_image,
        docker_host=docker_host,
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
    assert report["environment"]["worker_source"] == worker_proof
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


@pytest.mark.parametrize("state", ["blocked", "pending", "observation_timeout"])
def test_unfinished_foreground_blocks_blind_pass_even_for_turn_without_latency_tag(oracles, state):
    case = {
        "level": "deepseek_local",
        "checks": {"witness": {"status": "passed", "evidence": [{"persisted": True}]}},
        "model_rounds": [{"status": "returned"}],
        "turns": [{"foreground": {"state": state}, "latency_required": False}],
    }
    assert oracles.blind_status(case, {"witness"}) == "blocked"
    case["turns"] = [{"foreground": {"state": "decided"}, "latency_required": False}]
    assert oracles.blind_status(case, {"witness"}) == "passed"


def test_native_quote_accepts_exact_preloaded_work_version_without_an_extra_read(oracles):
    target = "synthetic-claimed-work"
    source = "current-quote-event"
    exact = {"id": target, "goal_revision": 3}
    stale = {"id": target, "goal_revision": 2}

    def observation(*, event_id=source, kind="bot_reply", quote_id=target, facts=None):
        return {
            "source_event_id": event_id,
            "input_context": {
                "context": {
                    "trusted_routing": {"source": kind, "recipient": "agent"},
                    "quoted_work_id": quote_id,
                    "work_facts_overview": {"facts": facts or [exact]},
                }
            },
        }

    assert oracles.work_fact_has(
        oracles.quoted_work_context([observation()], source, target), target, [exact]
    )
    assert not oracles.work_fact_has(
        oracles.quoted_work_context([observation(facts=[stale])], source, target), target, [exact]
    )
    assert not oracles.quoted_work_context(
        [
            observation(event_id="earlier-event"),
            observation(kind="human_reply"),
            observation(quote_id="different-work"),
        ],
        source,
        target,
    )


def test_native_markdown_does_not_include_autolink_delimiters(oracles):
    text = (
        "Verified: <https://example.org/pages/one?query=guide>\n"
        "另见 [source](https://example.org/pages/topic_(details)).\n"
        "Plain source https://example.org/pages/plain。"
    )
    assert oracles.markdown_urls(text) == {
        "https://example.org/pages/one?query=guide",
        "https://example.org/pages/topic_(details)",
        "https://example.org/pages/plain",
    }


def test_queued_progress_proves_the_same_loaded_subject_and_phase(oracles):
    from types import SimpleNamespace

    work = SimpleNamespace(
        id="real-one",
        goal="核对 PONDA 门户并制作 PDF",
        model_dump=lambda **_: {"id": "real-one", "goal_revision": 3},
    )
    facts = [{"id": "real-one", "goal_revision": 3, "state": "queued", "goal": work.goal}]
    text = {"receipts": [native_text("reply", "还在排队，尚未开始核对 PONDA。")]}
    assert oracles.queued_progress_subjects(text, facts, work) == {"ponda"}
    generic = {"receipts": [native_text("reply", "这份 PDF 还在排队。")]}
    assert not oracles.queued_progress_subjects(generic, facts, work)
    assert not oracles.queued_progress_subjects(text, [{**facts[0], "goal_revision": 2}], work)
    assert not oracles.queued_progress_subjects(text, [{**facts[0], "state": "completed"}], work)


def test_restart_witness_reconciles_exact_intent_and_validates_distinct_pending_reply(oracles):
    spec = {
        "official_domains": ["example.org"],
        "official_source_patterns": [r"example\.org/manual"],
    }
    url = "https://example.org/manual"
    before = [native_text("wire-a", "已登记检查工作。")]
    verified = native_text("wire-b", "已打开核对了官方手册 https://example.org/manual。")
    after = [*before, verified]

    def part(intent, slot, native=None):
        return {
            "intent_id": intent,
            "idempotency_key": slot + ":source:1",
            "payload_hash": "hash-" + intent,
            "position": 0 if native else None,
            "platform_message_id": native,
            "part_state": "accepted" if native else None,
        }

    prior = [part("ack", "early_ack"), part("distinct", "reply")]
    accepted = [part("ack", "early_ack", "wire-a"), part("distinct", "reply", "wire-b")]
    assert oracles.restart_witness(before, after, prior, accepted, {url}, spec) == {
        "reconciled": True,
        "verified_separate_reply_ids": ["wire-b"],
    }
    generic = native_text("wire-b", "已登记核对 https://example.org/manual 的工作。")
    assert not oracles.restart_witness(before, [*before, generic], prior, accepted, {url}, spec)[
        "reconciled"
    ]
    assert not oracles.restart_witness(before, after, prior, accepted, set(), spec)["reconciled"]
    assert not oracles.restart_witness(before, [*after, verified], prior, accepted, {url}, spec)[
        "reconciled"
    ]
    recreated = [accepted[0], part("new-intent", "reply", "wire-b")]
    assert not oracles.restart_witness(before, after, prior, recreated, {url}, spec)["reconciled"]


@pytest.mark.parametrize(
    ("text", "actionable"),
    [
        ("你把领件码发我（文字或照片都行）。", True),
        ("发我领件码，我再核对。", True),
        ("麻烦把领件码发我一下。", True),
        ("请把已经填写的领件码发我。", True),
        ("记录里没有领件码。你可以把它发我。", True),
        ("你没发我领件码，我查不到。", False),
        ("你还没把领件码发我。", False),
        ("请不要把领件码发我。", False),
        ("我已经看过你上次发我的领件码。", False),
        ("你之前把领件码发我了。", False),
        ("发我领件码了吗？", False),
    ],
)
def test_colloquial_missing_input_requires_an_actual_request(oracles, text, actionable):
    assert oracles.essential_artifact_input_reply(text, "领件码") is actionable


def test_only_fresh_read_only_work_control_observation_counts_as_fact(oracles):
    from types import SimpleNamespace

    work = {"id": "running-work", "goal_revision": 4}
    current = {**work, "state": "running"}
    stale = {**work, "goal_revision": 3}
    observed = {
        "kind": "control_work",
        "status": "observed",
        "committed": False,
        "code": "work_control_state_observed",
        "facts": {"status": "matched", "facts": [current]},
    }
    request = {"event_id": "request"}

    def extract(observation):
        fixture = SimpleNamespace(
            model_calls=[
                {"source_event_id": "unrelated", "tool_observations": [observed]},
                {"source_event_id": "request", "tool_observations": [observation]},
            ]
        )
        return oracles.work_facts_observed(fixture, request)

    assert extract(observed) == [current]
    assert oracles.work_fact_has(extract(observed), work["id"], [work])
    assert not oracles.work_fact_has(
        extract({**observed, "facts": {"status": "matched", "facts": [stale]}}),
        work["id"],
        [work],
    )
    for changed in (
        {"committed": True},
        {"committed": None},
        {"replayed": True},
        {"work_effects": [{"id": work["id"]}]},
        {"kind": "update_work"},
        {"status": "rejected"},
        {"code": "version_conflict"},
        {"facts": {"status": "partial", "facts": [current]}},
        {"facts": {"status": "matched", "facts": [current, stale]}},
    ):
        assert extract({**observed, **changed}) == []
    assert extract({"kind": "read_work_facts", "status": "matched", "facts": [current]}) == [
        current
    ]


def test_short_condensation_reply_accepts_equivalent_cold_surface_wording(oracles):
    patterns = ["冷|降温", "水汽|水蒸气"]
    for phrase in ("冰凉", "低温", "温度较低"):
        reply = {"receipts": [native_text("reply", f"水蒸气遇到{phrase}的器壁，凝结成水滴。")]}
        assert oracles.short_condensation_reply(reply, patterns)
    no_vapor = {"receipts": [native_text("reply", "器壁温度较低。")]}
    no_cold_surface = {"receipts": [native_text("reply", "空气里有水蒸气。")]}
    assert not oracles.short_condensation_reply(no_vapor, patterns)
    assert not oracles.short_condensation_reply(no_cold_surface, patterns)


def test_group_summary_accepts_named_member_assignment_sections(oracles):
    patterns = ["盘点"]

    def reply(text):
        return {"receipts": [native_text("reply", text)]}

    for section in ("分工", "组员认领", "成员任务"):
        assert oracles.categorized_group_summary(
            reply(f"{section}：甲负责盘点。我的工作：无。提醒：无。"), patterns
        )
    assert not oracles.categorized_group_summary(reply("我负责盘点。分工：无。"), patterns)
    assert not oracles.categorized_group_summary(
        reply("组员任务：甲负责排版。我的工作：无。提醒：无。"), patterns
    )


def test_queued_progress_accepts_a_relevant_unproduced_output(oracles):
    from types import SimpleNamespace

    work = SimpleNamespace(
        id="report-work",
        goal="核验入口后制作核对报告",
        scope={"outputs": [{"format": "pdf"}]},
        model_dump=lambda **_: {"id": "report-work", "goal_revision": 3},
    )
    facts = [
        {
            "id": work.id,
            "goal_revision": 3,
            "state": "queued",
            "goal": work.goal,
            "outputs": {"candidate_count": 0, "validated_count": 0},
        }
    ]

    def turn(text):
        return {"receipts": [native_text("reply", text)]}

    honest = turn("还在排队等执行，没有生成或交付 PDF。")
    assert oracles.queued_progress_subjects(honest, facts, work) == {"PDF"}
    assert not oracles.queued_progress_subjects(turn("这份 PDF 还在排队。"), facts, work)
    assert not oracles.queued_progress_subjects(
        honest, [{**facts[0], "outputs": {"candidate_count": 1, "validated_count": 0}}], work
    )
    assert not oracles.queued_progress_subjects(honest, [{**facts[0], "goal_revision": 2}], work)
    assert not oracles.queued_progress_subjects(honest, [{**facts[0], "state": "completed"}], work)


def test_identity_limitation_accepts_exact_uncertainty_in_plain_language(oracles):
    assert oracles.identity_limitation_is_explicit("仅凭这张截图确定不了另一个人是谁。")
    assert oracles.identity_limitation_is_explicit("只有一张模糊照片，没法确认身份。")
    assert not oracles.identity_limitation_is_explicit("这就是另一个人，我能确定。")


def test_markdown_url_parens_do_not_change_actual_fetched_source(oracles):
    fetched = oracles.fetch_source_urls({"source": {"url": "https://example.org/project"}})
    citations = {
        oracles.page_url_key(url)
        for url in oracles.markdown_urls("官方项目：https://example.org/project（正文链接）")
    }
    assert citations & fetched == {"https://example.org/project"}
    assert not citations & oracles.fetch_source_urls(
        {"source": {"url": "https://example.org/other"}}
    )


def test_docx_source_links_bind_each_accepted_png_to_its_own_caption(oracles, tmp_path):
    import hashlib

    from docx import Document
    from runtime_acceptance_checks import _INSPECT

    pages = ["https://example.org/first", "https://example.org/second"]
    blobs = []
    for color in ("red", "blue"):
        stream = io.BytesIO()
        Image.new("RGB", (12, 12), color).save(stream, format="PNG")
        blobs.append(stream.getvalue())
    imports = [{"output": {"page_citation": f"{url}（已核验）"}} for url in pages]
    conversions = [
        {
            "output": {
                "format": "png",
                "sha256": hashlib.sha256(content).hexdigest(),
                "page_citation": f"{url}（已核验）",
            }
        }
        for url, content in zip(pages, blobs, strict=True)
    ]

    def inspect(captions, name):
        result = Document()
        for blob, caption in zip(blobs, captions, strict=True):
            result.add_picture(io.BytesIO(blob))
            result.add_paragraph(f"来源：{caption}（核对时间见说明）")
        path = tmp_path / name
        result.save(path)
        decoded = json.loads(
            subprocess.check_output([sys.executable, "-c", _INSPECT, str(path), "docx"], text=True)
        )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return {"decoded": decoded, "sha256": digest, "accepted_sha256": digest}

    correct = inspect(pages, "correct.docx")
    assert oracles.verified_docx_image_captions(correct, imports, conversions, 2)["verified"]
    assert not oracles.verified_docx_image_captions(
        {**correct, "accepted_sha256": None}, imports, conversions, 2
    )["verified"]
    swapped = inspect(pages[::-1], "swapped.docx")
    assert not oracles.verified_docx_image_captions(swapped, imports, conversions, 2)["verified"]


@pytest.mark.parametrize(
    ("text", "physically_explanatory"),
    [
        ("湿热空气遇冷，在冷碗外壁凝结成水珠。", True),
        ("潮湿的暖空气冷却并冷凝成小水滴。", True),
        ("湿润的空气接触冷玻璃后凝结为水滴。", True),
        ("干燥空气遇冷，在冷碗外壁凝结成水珠。", False),
        ("桌子旁边有湿热空气，碗壁很冷。", False),
        ("潮湿空气遇冷但没有凝结成水珠。", False),
        ("湿热空气遇冷后全都凝结成水珠。", False),
        ("整团潮湿空气冷却后变成水。", False),
    ],
)
def test_short_condensation_reply_checks_causal_moist_air_phrasing(
    oracles, text, physically_explanatory
):
    turn = {"receipts": [native_text("reply", text)]}
    assert (
        oracles.short_condensation_reply(turn, ["冷|降温", "水汽|水蒸气"]) is physically_explanatory
    )


def test_bare_url_stops_at_cjk_punctuation_but_explicit_destination_is_exact(oracles):
    link = "https://example.org/topic"
    encoded = "https://example.org/topic%EF%BC%8Cdetail?part=1,2"
    assert oracles.markdown_urls(f"入口 {link}，后面是链接说明。") == {link}
    assert oracles.markdown_urls(f"请见[源材料]({encoded})。") == {encoded}
    assert oracles.markdown_urls("取 https://example.org/index?part=1,2，供核对。") == {
        "https://example.org/index?part=1,2"
    }


def test_direct_read_substitutes_only_for_a_real_injected_search_failure(oracles):
    official = "https://example.org/project"
    failed_search = {
        "operation": "web_search",
        "fault_injected": True,
        "arguments": {"query": "project"},
    }
    found = {
        "operation": "web_fetch",
        "fault_injected": False,
        "arguments": {"url": official},
        "result": {"source": {"url": official}},
    }
    assert oracles.search_route_witness([failed_search, found], [], {official}) == {
        "successful_search": False,
        "verified_direct_read_fallback": True,
    }
    assert not any(oracles.search_route_witness([found], [], {official}).values())
    assert not any(
        oracles.search_route_witness(
            [failed_search, {**found, "result": None}], [], {official}
        ).values()
    )
    assert not any(
        oracles.search_route_witness(
            [failed_search, found], [], {"https://example.org/other"}
        ).values()
    )
    assert not any(
        oracles.search_route_witness(
            [failed_search, {**found, "fault_injected": True}], [], {official}
        ).values()
    )
    assert oracles.search_route_witness(
        [
            failed_search,
            {**failed_search, "fault_injected": False, "result": {"results": [{"url": official}]}},
        ],
        [],
        {official},
    )["successful_search"]


def test_exact_one_off_delivery_requires_native_identity_exact_body_and_no_memory(oracles):
    from types import SimpleNamespace

    body = "紫格七码"
    wire = native_text("exact-native-message", "【Pot】" + body)
    receipt = SimpleNamespace(state="accepted", platform_message_id="exact-native-message")
    assert oracles.exact_one_off_delivery(wire, receipt, body, [])
    for changed_text in ("记下了：" + body, "【Pot】" + body + "。", body, "【Pot】 " + body):
        assert not oracles.exact_one_off_delivery(
            native_text("exact-native-message", changed_text), receipt, body, []
        )
    assert not oracles.exact_one_off_delivery(wire, None, body, [])
    assert not oracles.exact_one_off_delivery(
        wire,
        SimpleNamespace(state="accepted", platform_message_id="another-native-message"),
        body,
        [],
    )
    assert not oracles.exact_one_off_delivery(
        wire, SimpleNamespace(state="unknown", platform_message_id="exact-native-message"), body, []
    )
    assert not oracles.exact_one_off_delivery(
        wire, receipt, body, [{"fact_id": "retained", "revision": 1, "state": "superseded"}]
    )
