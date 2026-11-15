"""Acceptance-checker semantics only; these fixtures are not live runtime evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "runtime_acceptance_checks", ROOT / "scripts/runtime_acceptance_checks.py"
)
assert SPEC and SPEC.loader
CHECKS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKS)


def evaluate(requirements, evidence):
    return CHECKS.evaluate_case({"id": "synthetic-checker-case", "checks": requirements}, evidence)


def artifact(format="md", text="", **decoded):
    digest = hashlib.sha256(b"explicit checker fixture, not actual artifact evidence").hexdigest()
    return {
        "file_id": "fixture-file",
        "version_id": "fixture-version",
        "group_id": "g",
        "work_id": "w",
        "format": format,
        "sha256": digest,
        "validation": {"passed": True, "sha256": digest},
        "provenance": {},
        "decoded": {"source_sha256": digest, "text": text, **decoded},
    }


def receipt(name, output, *, group="g"):
    return {
        "group_id": group,
        "work_id": "w",
        "receipt": {
            "call_id": "fixture-call",
            "success": True,
            "tool_name": name,
            "output": output,
        },
    }


def test_all_thirty_frozen_cases_are_not_run_when_evidence_is_missing():
    dataset = json.loads((ROOT / "tests/fixtures/runtime_acceptance_cases.json").read_text())
    assert len(dataset["cases"]) == 30
    for case in dataset["cases"]:
        result = CHECKS.evaluate_case(case, {})
        assert result["status"] == "NOT_RUN"
        assert set(case["checks"]) <= result["checks"].keys()
        assert not result["business_writes_applied_by_checker"]


def test_paused_edit_without_comparison_is_not_run_instead_of_raising_or_passing():
    requirements = {
        "new_version_required": True,
        "source_bytes_unchanged": True,
        "source_provenance_chain_required": True,
        "unchanged_source_units": [1, 2],
    }
    for comparison in [
        None,
        {},
        {"source_artifact": None, "new_artifact": None},
        {"source_artifact": {"decoded": None}, "new_artifact": None},
        {"source_artifact": "malformed", "new_artifact": {"decoded": None}},
    ]:
        result = evaluate(
            requirements,
            {
                "scope": {"group_id": "g", "work_id": "w"},
                "artifacts": [],
                "tool_receipts": [],
                "comparison": comparison,
            },
        )
        assert result["status"] == "NOT_RUN"
        assert all(result["checks"][key]["status"] == "NOT_RUN" for key in requirements)
        assert not result["business_writes_applied_by_checker"]


def test_correct_stdout_from_wrong_scope_cannot_pass():
    result = evaluate(
        {"successful_tools_all": ["python_compute"], "stdout_json_equals": {"answer": 5}},
        {
            "scope": {"group_id": "g", "work_id": "w"},
            "tool_receipts": [
                receipt(
                    "python_compute",
                    {"exit_code": 0, "stdout": '{"answer":5}'},
                    group="another-group",
                )
            ],
        },
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["_receipt_scope"]["status"] == "FAIL"


def test_model_claimed_output_cannot_substitute_for_stdout():
    result = evaluate(
        {"stdout_json_equals": {"answer": 5}},
        {
            "answer_text": '{"answer":5}',
            "scope": {"group_id": "g", "work_id": "w"},
            "tool_receipts": [],
        },
    )
    assert result["status"] == "NOT_RUN"


def test_markdown_structure_and_json_are_checked_from_artifact_text():
    content = '# Test\n## Prepare\nReady\n\n|Name|Count|\n|---|---|\n|Alpha|1|\n|Beta|2|\n\n```json\n{"status":"ok","count":3}\n```\n'
    evidence = {"scope": {"group_id": "g", "work_id": "w"}, "artifacts": [artifact(text=content)]}
    wanted = {
        "markdown_h2": ["Prepare"],
        "markdown_table_min_data_rows": 2,
        "json_codeblock_equals": {"status": "ok", "count": 3},
    }
    assert evaluate(wanted, evidence)["status"] == "PASS"
    wanted["json_codeblock_equals"]["count"] = 4
    assert evaluate(wanted, evidence)["status"] == "FAIL"


def test_mermaid_edges_are_read_as_labeled_nodes_not_identifier_substrings():
    evidence = {
        "scope": {"group_id": "g", "work_id": "w"},
        "artifacts": [
            artifact(
                text="```mermaid\ngraph TD\n A[Start] -->|checked| B[Review]\n B --> C[Done]\n```"
            )
        ],
    }
    assert (
        evaluate({"markdown_required_edges": [["Start", "Review"], ["Review", "Done"]]}, evidence)[
            "status"
        ]
        == "PASS"
    )
    evidence["artifacts"][0]["decoded"]["text"] = "NotStart --> Review"
    assert (
        evaluate({"markdown_required_edges": [["Start", "Review"]]}, evidence)["status"] == "FAIL"
    )


def test_plain_markdown_edges_are_not_renamed_by_parenthetical_position_prose():
    text = (
        "# Flow\n\nThree-node vertical flow: Start → Review → Done.\n\n"
        "## Edges\n\n1. Start -> Review\n2. Review -> Done\n\n"
        "## Diagram\n\n- Nodes: Start (top), Review (middle), Done (bottom).\n"
    )
    assert CHECKS._markdown_edges(text) == {("start", "review"), ("review", "done")}
    evidence = {"scope": {"group_id": "g", "work_id": "w"}, "artifacts": [artifact(text=text)]}
    required = {"markdown_required_edges": [["Start", "Review"], ["Review", "Done"]]}
    assert evaluate(required, evidence)["status"] == "PASS"
    for bad in (
        "Nodes: Start (top), Review (middle), Done (bottom).",
        "Start -> Review\nNodes: Start (top), Review (middle), Done (bottom).",
        "Review -> Start\nDone -> Review\nNodes: Start (top), Review (middle), Done (bottom).",
    ):
        evidence["artifacts"][0]["decoded"]["text"] = bad
        assert evaluate(required, evidence)["status"] == "FAIL"


def test_mermaid_aliases_are_scoped_to_each_graph_and_never_plain_prose():
    wanted = {"markdown_required_edges": [["Start", "Review"]]}
    evidence = {"scope": {"group_id": "g", "work_id": "w"}, "artifacts": [artifact()]}
    for valid in (
        "```mermaid\ngraph TD\n A[Start] --> B[Review]\n```\n\nStart (top), Review (middle).",
        "~~~mermaid\nflowchart TD\n A[Start] --> B[Review]\n~~~~",
        "graph TD\nA[Start] --> B[Review]",
        "```mermaid\ngraph TD\n A[Start] --> B[Review]\n %% B[Unrelated]\n```",
        "```mermaid\ngraph TD\n A[Start] --> B[Review]\n```\n\n```mermaid\ngraph TD\n A[Other] --> B[Else]\n```",
    ):
        evidence["artifacts"][0]["decoded"]["text"] = valid
        assert evaluate(wanted, evidence)["status"] == "PASS"
    for invalid in (
        "```mermaid\ngraph TD\n A[Start]\n B[Review]\n```\n\nA --> B",
        "```mermaid\ngraph TD\n A[Other] --> B[Else]\n```\n\nA[Start], B[Review]",
        "```text\nA[Start] --> B[Review]\n```",
        "```mermaid\ngraph TD\n A[Start] <-- B[Review]\n```",
    ):
        evidence["artifacts"][0]["decoded"]["text"] = invalid
        assert evaluate(wanted, evidence)["status"] == "FAIL"


def test_audio_decode_or_matching_words_without_bound_asr_evidence_is_not_speech_acceptance():
    clip = artifact(
        format="wav",
        audio={
            "decoded": True,
            "duration_seconds": 2,
            "non_silent": True,
            "asr_transcript": "synthetic test",
        },
    )
    evidence = {"scope": {"group_id": "g", "work_id": "w"}, "artifacts": [clip]}
    wanted = {
        "speech_content_check_required": True,
        "transcript_any_of_text_groups": [["synthetic test"]],
    }
    assert evaluate(wanted, evidence)["status"] == "NOT_RUN"
    clip["decoded"]["audio"].update(asr_receipt="fixture-only", asr_source_sha256=clip["sha256"])
    assert evaluate(wanted, evidence)["status"] == "PASS"
    clip["decoded"]["audio"]["asr_source_sha256"] = "0" * 64
    assert evaluate(wanted, evidence)["status"] == "FAIL"


def test_citations_need_successful_provenance_and_actual_access_date():
    url = "https://docs.python.org/3/library/json.html"
    wanted = {"citations_must_match_tool_provenance": True, "citation_access_time_required": True}
    evidence = {
        "scope": {"group_id": "g", "work_id": "w"},
        "answer_text": f"Source: {url} accessed 2030-06-08",
        "tool_receipts": [],
    }
    assert evaluate(wanted, evidence)["status"] == "FAIL"
    evidence["tool_receipts"] = [
        receipt("web_fetch", {"source": {"url": url, "retrieved_at": "2030-06-08T10:00:00+00:00"}})
    ]
    assert evaluate(wanted, evidence)["status"] == "PASS"
    evidence["answer_text"] = f"Source: {url}"
    assert evaluate(wanted, evidence)["status"] == "FAIL"


def test_schedule_due_uses_canonical_event_and_real_origin_schedule_id():
    scope = {"group_id": "g", "work_id": "w", "occurred_at": "2030-06-08T10:00:00+08:00"}
    evidence = {
        "scope": scope,
        "scheduling": {
            "rule": {"id": "rule", "timing": {"kind": "at", "at": "2030-06-08T10:10:00+08:00"}},
            "works": [{"scope": {"origin_schedule_id": "rule", "derived_depth": 1}}],
        },
    }
    checks = {
        "relative_due_seconds": 600,
        "due_tolerance_seconds": 60,
        "time_anchor": "current canonical event occurred_at",
        "new_works_from_referenced_rule": 1,
        "derived_depth": 1,
    }
    assert evaluate(checks, evidence)["status"] == "PASS"
    evidence["scheduling"]["rule"]["timing"]["at"] = "2030-06-08T11:10:00+08:00"
    assert evaluate(checks, evidence)["status"] == "FAIL"


def test_visual_or_semantic_review_cannot_pass_without_explicit_evidence():
    requirements = {"visual_review_required": ["three nodes"], "semantic_claims": {"correct": True}}
    assert evaluate(requirements, {})["status"] == "NEEDS_REVIEW"
    reviews = {
        key: {"passed": True, "evidence_ref": "fixture-review-reference"} for key in requirements
    }
    assert evaluate(requirements, {"reviews": reviews})["status"] == "PASS"


def test_completion_cannot_precede_accepted_delivery():
    evidence = {
        "delivery": {
            "work_completed_at": "2030-06-08T10:00:00Z",
            "parts": [{"state": "accepted", "accepted_at": "2030-06-08T10:01:00Z"}],
        }
    }
    assert evaluate({"work_completed_after_accepted_delivery": True}, evidence)["status"] == "FAIL"
    evidence["delivery"]["work_completed_at"] = "2030-06-08T10:02:00Z"
    assert evaluate({"work_completed_after_accepted_delivery": True}, evidence)["status"] == "PASS"


async def test_decoder_builds_valid_process_spec_and_cleans_declared_input(tmp_path):
    import asyncio

    from groupagent.domain import ActorContext, Role
    from groupagent.media import IsolatedMediaWorker
    from groupagent.sandbox import ProcessResult
    from groupagent.tools import ToolContext, ToolScope

    payload = b"# Explicit decoder protocol fixture"

    class SchemaOnlyFixtureSandbox:
        def __init__(self):
            self.calls = []

        async def run(self, spec, context):
            self.calls.append(spec)
            assert spec.max_output_bytes == 2_000_000
            assert spec.program == "python3" and spec.args[:2] == ("-I", "-c")
            assert (tmp_path / spec.input_paths[0]).read_bytes() == payload
            return ProcessResult(
                image="sha256:" + "0" * 64,
                exit_code=0,
                stdout=json.dumps(
                    {"source_sha256": hashlib.sha256(payload).hexdigest(), "text": payload.decode()}
                ),
                elapsed_seconds=0,
            )

    sandbox = SchemaOnlyFixtureSandbox()
    worker = IsolatedMediaWorker(sandbox)
    context = ToolContext(
        ToolScope(
            "g",
            "w",
            ActorContext(group_id="g", member_id="m", role=Role.OWNER),
            tmp_path,
            frozenset(),
            "fixture",
        ),
        "decode",
        "decode",
        asyncio.Event(),
    )
    result = await CHECKS.decode_artifact(worker, context, "md", payload)
    assert result["text"] == payload.decode() and len(sandbox.calls) == 1
    assert list(tmp_path.iterdir()) == []


def test_null_chart_arguments_use_actual_bound_native_chart_provenance():
    chart = artifact("png", image={"decoded": True, "width": 1200, "height": 720, "nonblank": True})
    chart["provenance"]["chart_spec"] = {
        "kind": "bar",
        "categories": ["Alpha", "Beta", "Gamma"],
        "series": [{"name": "Counts", "values": [12.0, 24.0, 36.0]}],
    }
    tool = receipt("chart_render", {"version_id": chart["version_id"], "sha256": chart["sha256"]})
    tool["arguments"] = None
    result = evaluate(
        {
            "chart_data_equals": {
                "kind": "bar",
                "categories": ["Alpha", "Beta", "Gamma"],
                "values": [12, 24, 36],
            }
        },
        {"scope": {"group_id": "g", "work_id": "w"}, "artifacts": [chart], "tool_receipts": [tool]},
    )
    assert result["status"] == "PASS"
    chart["provenance"]["chart_spec"]["series"][0]["values"][0] = 99
    assert (
        evaluate(
            {
                "chart_data_equals": {
                    "kind": "bar",
                    "categories": ["Alpha", "Beta", "Gamma"],
                    "values": [12, 24, 36],
                }
            },
            {
                "scope": {"group_id": "g", "work_id": "w"},
                "artifacts": [chart],
                "tool_receipts": [tool],
            },
        )["status"]
        == "FAIL"
    )


def test_null_or_partial_chart_data_never_passes_or_crashes():
    requirement = {"chart_data_equals": {"kind": "bar", "categories": ["A"], "values": [1]}}
    for spec in (
        None,
        {},
        {"kind": "bar", "categories": ["A"], "series": None},
        {"kind": "bar", "categories": ["A"], "series": [None]},
        {"kind": "bar", "categories": ["A"], "series": [{"values": [True]}]},
    ):
        chart = artifact("png", image={"decoded": True})
        chart["provenance"]["chart_spec"] = spec
        tool = receipt(
            "chart_render", {"version_id": chart["version_id"], "sha256": chart["sha256"]}
        )
        tool["arguments"] = None
        result = evaluate(
            requirement,
            {
                "scope": {"group_id": "g", "work_id": "w"},
                "artifacts": [chart],
                "tool_receipts": [tool],
            },
        )
        assert result["status"] in {"NOT_RUN", "NEEDS_REVIEW"}, spec


def test_every_frozen_check_handles_nullable_evidence_without_implicit_pass():
    dataset = json.loads((ROOT / "tests/fixtures/runtime_acceptance_cases.json").read_text())
    evidence = {
        "scope": None,
        "artifacts": [None],
        "tool_receipts": [None],
        "input_media": None,
        "delivery": None,
        "scheduling": None,
        "before_after": None,
        "comparison": None,
        "reviews": None,
    }
    for case in dataset["cases"]:
        result = CHECKS.evaluate_case(case, evidence)
        assert result["status"] in {"NOT_RUN", "NEEDS_REVIEW"}
        assert set(case["checks"]) <= set(result["checks"])


def test_participation_metrics_use_real_gate_and_separate_fixed_denominators():
    from groupagent.coordinator import Decision

    cases = []
    for identifier, text, kinds, directness in (
        ("yes", "请解释可靠性是什么意思", ["reply"], "direct"),
        ("no", "林知夏，这个表格由你整理", ["observe"], "other"),
        ("mixed", "也许可以再讨论一下", ["observe", "reply"], "proactive"),
    ):
        context = {
            "group_id": "g-" + identifier,
            "requester_member_id": "self",
            "role": "member",
            "members": [],
            "agent_aliases": ["机器人"],
            "now": "2026-11-14T09:00:00+08:00",
        }
        cases.append(
            {
                "id": identifier,
                "text": text,
                "role": "member",
                "context": context,
                "expected_kind": kinds,
                "event_kind": "text",
                "occurred_at": context["now"],
                "candidate_directness": directness,
            }
        )
    digest = hashlib.sha256(
        json.dumps(cases, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    data = {"cases": cases, "cases_sha256": digest}
    records = [
        {
            "id": case["id"],
            "labels_sha256": digest,
            "decision": Decision(
                kind="reply",
                response="合成答复",
                reason_code="fixture",
                directness=case["candidate_directness"],
                confidence=1,
            ).model_dump(mode="json"),
        }
        for case in cases
    ]
    result = CHECKS.evaluate_interaction_participation(
        data, records, expected_denominators=(1, 1, 1)
    )
    assert result["explicit_request_recall"]["numerator"] == 1
    assert result["should_not_intervene_rate"]["numerator"] == 0
    assert result["effective"]["no"]["kind"] == "observe"
    assert result["other_13"]["denominator"] == 1
    assert result["provider_calls"] == result["business_writes_applied"] == 0
    import pytest

    with pytest.raises(ValueError):
        CHECKS.evaluate_interaction_participation(data, records)
    with pytest.raises(ValueError):
        CHECKS.evaluate_interaction_participation(
            data, records[:-1], expected_denominators=(1, 1, 1)
        )


def test_observed_web_evidence_fullwidth_parenthesis_does_not_enter_cited_url():
    url = "https://docs.python.org/3/library/json.html"
    wanted = {
        "citation_hosts_all": ["docs.python.org"],
        "citation_path_suffix": "/library/json.html",
        "citations_must_match_tool_provenance": True,
        "citation_access_time_required": True,
    }
    evidence = {
        "scope": {"group_id": "g", "work_id": "w"},
        "answer_text": f"参考 {url}（页面标题：json — JSON encoder and decoder）。访问时间：2026-11-14 20:47。",
        "tool_receipts": [
            receipt(
                "web_fetch", {"source": {"url": url, "retrieved_at": "2026-11-14T20:47:00+08:00"}}
            )
        ],
    }
    assert evaluate(wanted, evidence)["status"] == "PASS"
    evidence["answer_text"] = evidence["answer_text"].replace("docs.python.org", "wrong.example")
    result = evaluate(wanted, evidence)
    assert result["checks"]["citations_must_match_tool_provenance"]["status"] == "FAIL"
    assert result["checks"]["citation_hosts_all"]["status"] == "FAIL"


def test_bare_unicode_paths_and_encoded_punctuation_remain_legitimate_urls():
    urls = (
        "https://example.org/资料/页面?主题=排序",
        "https://example.org/%EF%BC%88资料%EF%BC%89/说明",
    )
    for url in urls:
        evidence = {
            "scope": {"group_id": "g", "work_id": "w"},
            "answer_text": f"链接：“{url}”。",
            "tool_receipts": [receipt("web_fetch", {"source": {"url": url}})],
        }
        assert (
            evaluate({"citations_must_match_tool_provenance": True}, evidence)["status"] == "PASS"
        )
