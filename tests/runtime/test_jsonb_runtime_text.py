"""Real PostgreSQL persistence must preserve model/tool strings outside JSONB's Unicode range."""

import hashlib
from datetime import UTC, datetime

import pytest

from groupagent.execution import EngineCheckpoint, ExecutionRequest, ModelMessage, RunState
from groupagent.execution.models import (
    EngineEvent,
    FinishedToolPayload,
    TerminalPayload,
    UsageTotals,
)
from groupagent.runtime.checkpoint_codec import decode_event, encode_checkpoint, encode_event
from groupagent.runtime.text import visible_text
from groupagent.tools import EffectState, ToolReceipt
from groupagent.tools.models import ToolLedgerEntry

from .test_work_facts import new_work


@pytest.mark.parametrize(
    "text", ["正常中文\n🙂e\u0301\tline\\u0000", "x\x00y", "x\ud800y", "x\udfffy"]
)
def test_visible_text_is_idempotent_and_retains_valid_text(text):
    projected = visible_text(text)
    assert visible_text(projected) == projected
    assert "\x00" not in projected and not any(
        0xD800 <= ord(value) <= 0xDFFF for value in projected
    )
    if text == "正常中文\n🙂e\u0301\tline\\u0000":
        assert projected == text


@pytest.mark.parametrize(
    "character", ["\x00", "\ud800", "\udfff"], ids=["nul", "high-surrogate", "low-surrogate"]
)
@pytest.mark.postgres
def test_checkpoint_and_event_preserve_incompatible_text_and_safe_failed_header(
    runtime_env, character
):
    env = runtime_env
    work = new_work(env)
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(work.id, "synthetic-jsonb-writer")
    receipt = ToolReceipt(
        call_id="synthetic-text",
        tool_name="synthetic_read",
        idempotency_key=f"{work.id}:synthetic-text",
        success=False,
        effect_state=EffectState.NO_EFFECT,
        error_code="process_failed",
        message="Synthetic failed diagnostic " + character + " preserved",
        output={"nested": {"text": "Source " + character + " exact text"}},
        loaded_tools=frozenset({"zulu", "alpha", "bravo"}),
    )
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id=work.id,
            attempt_id=lease.attempt_id,
            actor=env.actor,
            objective=work.goal,
            workspace=env.root.resolve(),
            trace_id=work.request_event_id,
        ),
        revision=work.goal_revision,
        state=RunState.CANDIDATE,
        sequence=2,
        usage=UsageTotals(rounds=1, tokens=3, tool_calls=1),
        safe_to_resume=True,
        candidate_text="Candidate " + character + " exact text",
        messages=(ModelMessage(role="assistant", content="Model " + character + " exact text"),),
        receipts=(receipt,),
        ledger=(ToolLedgerEntry(fingerprint="a" * 64, receipt=receipt),),
    )
    encoded = encode_checkpoint(checkpoint)
    assert "checkpoint_codec" in encoded
    assert character not in encoded["receipts"][0]["message"]
    env.state.save_checkpoint(env.actor, lease, checkpoint)
    assert env.state.load_checkpoint(env.actor, work.id) == checkpoint
    event = EngineEvent(
        work_id=work.id,
        attempt_id=lease.attempt_id,
        trace_id=work.request_event_id,
        sequence=1,
        time=datetime.now(UTC),
        kind="tool_finished",
        payload=FinishedToolPayload(receipt=receipt),
    )
    env.state.append_event(env.actor, lease, event)
    env.state.append_event(env.actor, lease, event)
    terminal = EngineEvent(
        work_id=work.id,
        attempt_id=lease.attempt_id,
        trace_id=work.request_event_id,
        sequence=2,
        time=datetime.now(UTC),
        kind="run_finished",
        payload=TerminalPayload(
            state=RunState.CANDIDATE,
            revision=work.goal_revision,
            candidate_text=checkpoint.candidate_text,
            usage=checkpoint.usage,
        ),
    )
    env.state.append_event(env.actor, lease, terminal)
    with env.store.transaction(env.actor) as tx:
        rows = tx.conn.execute(
            "SELECT body FROM ga_runtime_events WHERE group_id=%s AND work_id=%s ORDER BY sequence",
            (tx.group_id, work.id),
        ).fetchall()
    assert decode_event(rows[0]["body"]) == event
    assert decode_event(rows[1]["body"]) == terminal
    assert rows[0]["body"]["payload"]["receipt"]["effect_state"] == "no_effect"
    assert rows[0]["body"]["payload"]["receipt"]["loaded_tools"] == ["alpha", "bravo", "zulu"]
    assert "output" not in rows[0]["body"]["payload"]["receipt"]
    assert "runtime_event_codec" in encode_event(event)


@pytest.mark.parametrize("character", ["\x00", "\ud800"], ids=["nul", "surrogate"])
@pytest.mark.postgres
async def test_real_toolkit_text_delivery_uses_visible_projection_and_preserves_candidate(
    runtime_env, character
):
    from groupagent.billing import BudgetLedger
    from groupagent.config import AppConfig
    from groupagent.execution import ScriptedFakeModel
    from groupagent.memory import MemoryService
    from groupagent.runtime.executor import WorkSupervisor
    from groupagent.runtime.toolkit import WorkToolkit

    env = runtime_env
    work = new_work(env, "Answer a synthetic text question")
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(work.id, "synthetic-visible-output")
        work = tx.get_work(work.id)
    config = AppConfig.model_validate(
        {
            "group": {
                "internal_id": env.actor.group_id,
                "external_group_id": "synthetic-visible",
                "owner_ids": [env.actor.member_id],
            },
            "runtime": {"worker_image": "sha256:" + "a" * 64, "semantic_validation": False},
        }
    )
    toolkit = WorkToolkit(
        config,
        env.store,
        env.artifacts,
        env.registry,
        MemoryService(env.store),
        ScriptedFakeModel([]),
        BudgetLedger(env.store),
        media_providers={},
        routes={},
    )
    raw = "保留有效文本\nsource " + character + " exact"
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id=work.id,
            attempt_id=lease.attempt_id,
            actor=env.actor,
            objective=work.goal,
            workspace=env.root.resolve(),
            trace_id=work.request_event_id,
        ),
        revision=work.goal_revision,
        state=RunState.CANDIDATE,
        sequence=1,
        usage=UsageTotals(),
        safe_to_resume=True,
        candidate_text=raw,
        messages=(),
    )
    candidate = await toolkit.validate(
        work, lease, env.actor, WorkSupervisor._snapshot(checkpoint), env.root.resolve()
    )
    assert candidate.deliveries[0].payload_ref == "text:" + visible_text(raw)
    assert (
        candidate.deliveries[0].payload_hash
        == hashlib.sha256(visible_text(raw).encode()).hexdigest()
    )
    assert checkpoint.candidate_text == raw
