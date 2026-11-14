"""Offline runner/checker fixtures only; never bootstrap keys, Docker or live calls."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from groupagent.channels.models import ChannelReceipt
from groupagent.conversation.intent import new_work_intent
from groupagent.domain import ActorContext, EventKind, GroupEvent, Role

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "fast_execution_test", ROOT / "scripts/live_fast_admission_execution.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def forbidden(*args, **kwargs):
        raise AssertionError(
            "Offline test must not bootstrap credentials, Docker or paid providers"
        )

    monkeypatch.setattr(module, "setup", forbidden)
    monkeypatch.setattr(module, "build_app", forbidden)
    return module


def arguments(**changes):
    return SimpleNamespace(
        run_id="new-independent-run",
        worker_image="sha256:" + "1" * 64,
        harness_image="sha256:" + "2" * 64,
        docker_host="unix:///synthetic.sock",
        timeout=30,
        **changes,
    )


def proof_fixture(case_id):
    return {
        "valid": True,
        "checks": {"one_work": True, "one_ack": True, "original_goal": True},
        "work_id": "work",
        "ack_id": "ack",
        "source": {"id": "event", "group_id": "group"},
    }


def evidence_fixture(case_id):
    # These are protocol-shape fixtures, never actual runtime evidence.
    title = "快速受理闭环" if case_id == "FAST-MD" else "快速受理分页验证"
    digest = hashlib.sha256(b"explicit offline output fixture").hexdigest()
    output = {
        "file_id": "file",
        "version_id": "version",
        "group_id": "group",
        "work_id": "work",
        "format": "md" if case_id == "FAST-MD" else "pptx",
        "sha256": digest,
        "validation": {"passed": True, "sha256": digest},
        "decoded": {"source_sha256": digest, "text": title + "\nJSON data", "unit_count": 3},
    }
    deliveries, local = [], []
    for identifier, payload in (
        ("ack", "text:Accepted synthetic request"),
        ("result", "text:391" if case_id == "FAST-COMPUTE" else "artifact:version"),
    ):
        key = "offline-fixture:" + identifier
        receipt = ChannelReceipt(
            account_id="synthetic-account",
            group_id="group",
            idempotency_key=key,
            wire_idempotency_key=key,
            state="accepted",
            platform_message_id="local-only:" + identifier,
            observed_at=datetime.now(UTC),
        ).model_dump(mode="json")
        deliveries.append(
            {
                "id": identifier,
                "group_id": "group",
                "state": "accepted",
                "work_id": None if identifier == "ack" else "work",
                "payload_ref": payload,
            }
        )
        local.append(
            {
                "intent_id": identifier,
                "part": {
                    "state": "accepted",
                    "idempotency_key": key,
                    "platform_message_id": receipt["platform_message_id"],
                },
                "receipt": receipt,
                "spool_record": {"transport": "local-only", "receipt": receipt},
                "delivered_bytes_sha256": digest if identifier == "result" else None,
            }
        )
    return {
        "scope": {"group_id": "group", "work_id": "work", "event_id": "event"},
        "work_state": "completed",
        "artifacts": [] if case_id == "FAST-COMPUTE" else [output],
        "harness": {
            "version": "0.2.2rc2",
            "group_id": "group",
            "work_id": "work",
            "session_checkpoint_sha256": "4" * 64,
            "session_checkpoint_encoded_bytes": 100,
        },
        "billing": [
            {
                "group_id": "group",
                "work_id": "work",
                "operation": "text",
                "state": "settled",
                "actual_units": {"input_tokens": "5", "output_tokens": "2"},
                "evidence_ref": "offline-fixture",
            }
        ],
        "tool_receipts": [
            {
                "group_id": "group",
                "work_id": "work",
                "receipt": {
                    "tool_name": "python_compute",
                    "success": True,
                    "output": {"exit_code": 0, "stdout": "391\n"},
                },
            }
        ],
        "deliveries": deliveries,
        "local_receipts": local,
    }


def test_only_three_verbatim_user_inputs_are_sent_and_checker_gold_is_absent(runner):
    manifest = runner.input_manifest()
    assert [item["text"] for item in manifest["cases"]] == [
        "请制作一份介绍JSON数据结构的Markdown，标题为“快速受理闭环”。",
        "请制作3页介绍JSON数据结构的PPT，标题为“快速受理分页验证”。",
        "请计算17*23。",
    ]
    assert all(new_work_intent(item["text"]) is not None for item in manifest["cases"])
    assert "391" not in json.dumps(manifest, ensure_ascii=False)
    assert manifest["separate_from_runtime_matrix_30"] is True


def test_harness_binding_hashes_native_state_without_copying_its_base64(runner):
    native = "synthetic-private-native-session-content"
    binding = runner.harness_binding(
        {"sessions": native, "version": "0.2.2rc2", "group_id": "g", "work_id": "w"}
    )
    assert native not in json.dumps(binding)
    assert binding["session_checkpoint_sha256"] == hashlib.sha256(native.encode()).hexdigest()
    assert binding["session_checkpoint_encoded_bytes"] == len(native.encode())


def test_actual_run_01_none_default_is_schema_equivalent_without_dropping_constraints(runner):
    # Exact shape observed in real run 01 before any execute_tick/provider call.
    actual = [{"format": "md", "exact_units": None, "expected_text": ["快速受理闭环"]}]
    decision = [{"format": "md", "expected_text": ["快速受理闭环"]}]
    assert runner.output_constraints_equal(actual, decision)
    ppt = [{"format": "pptx", "exact_units": 3, "expected_text": ["快速受理分页验证"]}]
    for changed in (
        [{"format": "pptx", "exact_units": None, "expected_text": ["快速受理分页验证"]}],
        [{"format": "pptx", "exact_units": 2, "expected_text": ["快速受理分页验证"]}],
        [{"format": "pdf", "exact_units": 3, "expected_text": ["快速受理分页验证"]}],
        [{"format": "pptx", "exact_units": 3, "expected_text": []}],
    ):
        assert not runner.output_constraints_equal(changed, ppt)


@pytest.mark.parametrize("case_id", ["FAST-MD", "FAST-PPTX", "FAST-COMPUTE"])
def test_bound_complete_shapes_are_accepted_as_unit_fixtures_only(runner, case_id):
    assert (
        runner.assess(case_id, proof_fixture(case_id), evidence_fixture(case_id))["status"]
        == "PASS"
    )


@pytest.mark.parametrize(
    "failure",
    [
        "admission_false",
        "admission_check",
        "admission_scope",
        "paused",
        "harness_absent",
        "billing_absent",
        "billing_wrong_work",
        "receipt_absent",
        "receipt_unknown",
        "receipt_wrong_scope",
        "receipt_no_identity",
        "ack_missing",
        "spool_mismatch",
    ],
)
def test_incomplete_or_wrong_scope_evidence_cannot_pass(runner, failure):
    admission, evidence = proof_fixture("FAST-MD"), evidence_fixture("FAST-MD")
    if failure == "admission_false":
        admission["valid"] = False
    elif failure == "admission_check":
        admission["checks"]["one_work"] = False
    elif failure == "admission_scope":
        admission["work_id"] = "other"
    elif failure == "paused":
        evidence["work_state"] = "paused"
    elif failure == "harness_absent":
        evidence["harness"] = {}
    elif failure == "billing_absent":
        evidence["billing"] = []
    elif failure == "billing_wrong_work":
        evidence["billing"][0]["work_id"] = "other"
    elif failure == "receipt_absent":
        evidence["local_receipts"] = []
    elif failure == "receipt_unknown":
        evidence["local_receipts"][0]["receipt"]["state"] = "needs_review"
    elif failure == "receipt_wrong_scope":
        evidence["local_receipts"][0]["receipt"]["group_id"] = "other"
    elif failure == "receipt_no_identity":
        evidence["local_receipts"][0]["receipt"]["platform_message_id"] = None
    elif failure == "ack_missing":
        admission["ack_id"] = "nonexistent"
    else:
        evidence["local_receipts"][0]["spool_record"] = {}
    assert runner.assess("FAST-MD", admission, evidence)["status"] == "FAIL"


@pytest.mark.parametrize(
    "failure", ["pages", "title", "metadata_only", "sha", "validation", "file_scope", "wire_bytes"]
)
def test_ppt_needs_actual_decoded_three_slides_title_and_matching_delivered_bytes(runner, failure):
    evidence = evidence_fixture("FAST-PPTX")
    artifact = evidence["artifacts"][0]
    if failure == "pages":
        artifact["decoded"]["unit_count"] = 2
    elif failure in {"title", "metadata_only"}:
        artifact["decoded"]["text"] = "JSON only"
        artifact["name"] = "快速受理分页验证.pptx"
    elif failure == "sha":
        artifact["decoded"]["source_sha256"] = "0" * 64
    elif failure == "validation":
        artifact["validation"]["passed"] = False
    elif failure == "file_scope":
        artifact["work_id"] = "other"
    else:
        evidence["local_receipts"][1]["delivered_bytes_sha256"] = "0" * 64
    assert runner.assess("FAST-PPTX", proof_fixture("FAST-PPTX"), evidence)["status"] == "FAIL"


@pytest.mark.parametrize(
    "failure", ["answer_only", "stdout_wrong", "tool_wrong_scope", "not_delivered"]
)
def test_computation_needs_real_scoped_stdout_and_delivered_result(runner, failure):
    evidence = evidence_fixture("FAST-COMPUTE")
    if failure == "answer_only":
        evidence["tool_receipts"] = []
    elif failure == "stdout_wrong":
        evidence["tool_receipts"][0]["receipt"]["output"]["stdout"] = "392"
    elif failure == "tool_wrong_scope":
        evidence["tool_receipts"][0]["work_id"] = "other"
    else:
        evidence["deliveries"][1]["payload_ref"] = "text:No result"
    assert (
        runner.assess("FAST-COMPUTE", proof_fixture("FAST-COMPUTE"), evidence)["status"] == "FAIL"
    )


async def test_dry_inspection_never_bootstraps_secrets_or_executes(runner, capsys):
    await runner.run(arguments(execute=False))
    assert json.loads(capsys.readouterr().out)["mode"] == "dry_inspection_no_credentials"


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "legal_none_default",
        "duplicate_work",
        "duplicate_ack",
        "source_fingerprint",
        "goal",
        "source",
    ],
)
def test_admission_requires_actual_unique_db_work_ack_and_unmodified_current_source(
    runner, failure
):
    actor = ActorContext(group_id="group", member_id="member", role=Role.OWNER)
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id="event",
        source_key="offline:source",
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text=runner.INPUTS[0 if failure == "legal_none_default" else 1]["text"],
        occurred_at=stamp,
        received_at=stamp,
    )
    outputs = list(new_work_intent(event.text).outputs)
    if failure == "legal_none_default":
        outputs = [{**item, "exact_units": None} for item in outputs]
    scope = {
        "source_fingerprints": {event.id: "fingerprint"},
        "allowed_group_ids": [actor.group_id],
        "delivery_target": "origin_group",
        "admission_path": "deterministic_current_request",
        "outputs": outputs,
    }
    works = [
        {
            "id": "work",
            "goal": event.text,
            "request_event_id": event.id,
            "requester_id": actor.member_id,
            "scope": scope,
            "required_checks": ["result", "artifact:" + outputs[0]["format"]],
            "state": "queued",
        }
    ]
    acks = [{"id": "ack"}]
    source = event
    if failure == "duplicate_work":
        works *= 2
    elif failure == "duplicate_ack":
        acks *= 2
    elif failure == "source_fingerprint":
        scope["source_fingerprints"][event.id] = "other"
    elif failure == "goal":
        works[0]["goal"] = "truncated"
    elif failure == "source":
        source = event.model_copy(update={"text": "changed"})

    class Cursor:
        def __init__(self, query):
            self.query = query

        def fetchall(self):
            return works if "ga_work_orders" in self.query else acks

    @contextmanager
    def transaction(actor):
        yield SimpleNamespace(
            get_event=lambda identifier: source,
            get_decision=lambda identifier: {
                "result": {
                    "work_id": "work",
                    "delivery_id": "ack",
                    "decision": {
                        "kind": "start_work",
                        "reason_code": "explicit_new_work_admission",
                    },
                }
            },
            event_fingerprint=lambda identifier: "fingerprint",
            conn=SimpleNamespace(execute=lambda q, args: Cursor(q)),
        )

    result = runner.admission_snapshot(SimpleNamespace(transaction=transaction), actor, event)
    assert result["valid"] is (failure in {None, "legal_none_default"})
    if failure is None:
        assert result["declared_outputs"] == [
            {"format": "pptx", "exact_units": 3, "expected_text": ["快速受理分页验证"]}
        ]


def test_new_run_refuses_to_overwrite_old_evidence(runner, tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "source_hash", lambda: "3" * 64)
    args = arguments(execute=True)
    config = runner.configuration(args)
    root, _ = runner.create_run(args, config)
    original = {p.name: p.read_bytes() for p in root.iterdir()}
    with pytest.raises(FileExistsError):
        runner.create_run(args, config)
    assert {p.name: p.read_bytes() for p in root.iterdir()} == original


async def test_bootstrap_failure_is_preserved_with_three_not_run_cases(
    runner, tmp_path, monkeypatch
):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "source_hash", lambda: "3" * 64)
    await runner.run(arguments(execute=True))  # Injected forbidden setup, never a real bootstrap.
    result = json.loads((tmp_path / "runtime/live/new-independent-run/results.json").read_text())
    assert result["status"] == "FAIL" and result["fatal_error"]["type"] == "AssertionError"
    assert len(result["cases"]) == result["denominator"] == 3
    assert all(row["status"] == "NOT_RUN" for row in result["cases"])
