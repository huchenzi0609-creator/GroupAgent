"""Foreground facts expose bounded evidence, while execution payloads remain private."""

import json
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import WorkState
from groupagent.runtime.recovery import RECONCILIATION_SCOPE_KEY, plan_reconciliation

from .test_work_facts import accept, delivering_bundle, fact, native_running, new_work

pytestmark = [pytest.mark.postgres, pytest.mark.work_awareness]


def test_fact_lists_real_validated_artifact_and_native_receipt_evidence(runtime_env):
    env = runtime_env
    work, lease, bundle = delivering_bundle(env)
    initial = fact(env, work)
    artifact = initial["artifacts"]["items"][0]
    assert initial["artifacts"]["complete"] and artifact["validated"]
    assert artifact["name"] == "result.md" and artifact["format"] == "md"
    assert len(artifact["sha256"]) == 64 and artifact["byte_length"] > 0
    assert initial["execution"]["attempt_id"] == lease.attempt_id
    assert initial["execution"]["lease_live"]
    assert initial["delivery"]["receipts_complete"]
    assert {value["intent_id"] for value in initial["delivery"]["receipts"]} == {
        value.id for value in bundle
    }
    assert all(value["platform_message_id"] is None for value in initial["delivery"]["receipts"])
    native_running(env, bundle[0])
    running = fact(env, work)
    assert not running["execution"]["reconciliation_required"]
    assert running["delivery"]["receipts"][0]["platform_job_id"] == "synthetic-native-job"
    assert running["delivery"]["receipts"][0]["state"] == "dispatching"
    assert running["delivery"]["receipts"][0]["ledger_state"] == "unknown"
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_delivery_parts SET state='unknown' WHERE group_id=%s AND intent_id=%s",
            (env.actor.group_id, bundle[0].id),
        )
    uncertain = fact(env, work)
    assert uncertain["execution"]["reconciliation_required"]
    accept(env, bundle[0])
    accepted = fact(env, work)
    assert accepted["delivery"]["receipts"][0]["state"] == "accepted"
    assert accepted["delivery"]["receipts"][0]["platform_message_id"] == "synthetic-message"
    assert accepted["delivery"]["receipts"][0]["accepted_at"]
    assert "DO_NOT_EXPOSE" not in json.dumps(accepted)


def test_technical_reconciliation_exposes_next_inspection_and_keeps_true_state(runtime_env):
    env = runtime_env
    work = new_work(env)
    with env.store.transaction(env.actor) as tx:
        current = tx.transition_work(
            work.id, WorkState.PAUSED, expected_revision=work.revision, reason="effect_unknown"
        )
        plan = plan_reconciliation(current, "effect_unknown", now=datetime.now(UTC))
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=scope||%s WHERE group_id=%s AND id=%s",
            (Jsonb({RECONCILIATION_SCOPE_KEY: plan}), env.actor.group_id, work.id),
        )
    observed = fact(env, work)
    assert observed["state"] == "paused" and observed["pause_reason"] == "effect_unknown"
    assert observed["execution"]["reconciliation_required"]
    assert observed["execution"]["next_wake_at"] == plan["next_check_at"]
    assert observed["presentation"]["background_processing"]
    assert observed["status_detail"]["code"] == "background_processing"


def test_requirement_projection_is_bounded_and_marks_lost_detail(runtime_env):
    env = runtime_env
    work = new_work(
        env,
        scope={
            "outputs": [
                {
                    "format": "pptx",
                    "exact_units": 3,
                    "expected_text": ["S" * 500 for _ in range(30)],
                }
            ]
        },
    )
    observed = fact(env, work)
    requirement = observed["requirements"]["required_outputs"][0]
    assert requirement["format"] == "pptx" and requirement["exact_units"] == 3
    assert len(requirement["expected_text"]) == 8
    assert all(len(value) == 200 for value in requirement["expected_text"])
    assert not requirement["expected_text_complete"]
