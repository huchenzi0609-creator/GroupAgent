"""A quiet decision is a checked result, not an empty or fabricated delivery."""

import pytest

from groupagent.domain import DomainError, WorkState

from .test_toolkit_contract import call, candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres


async def test_scheduled_look_can_finish_without_a_wechat_message(toolkit_setup):
    env = toolkit_setup.env
    run = await prepare(
        toolkit_setup,
        goal="检查展台的场地许可是否有变化，没变化就安静",
        scope={
            "origin_schedule_id": "synthetic-scheduled-review",
            "derived_depth": 1,
            "allow_silent_result": True,
        },
    )
    assert "finish_silently" in run.prepared.request.visible_tools
    receipt = await call(run, "finish_silently", {"reason": "场地许可状态保持不变"})
    assert receipt.success and receipt.output["disposition"] == "no_notification"
    result = await toolkit_setup.toolkit.validate(
        run.work, run.lease, env.actor, candidate(run, "No change", [receipt]), run.workspace
    )
    assert result.silent_evidence and not result.deliveries
    with env.store.transaction(env.actor) as tx:
        work = tx.transition_work(
            run.work.id,
            WorkState.VALIDATING,
            expected_revision=run.work.revision,
            lease=run.lease,
        )
        tx.record_validation(work.id, run.lease, result.checks["result"], check_key="result")
    with pytest.raises(DomainError, match="Required delivery"):
        with env.store.transaction(env.actor) as tx:
            temporary = tx.transition_work(
                work.id, WorkState.DELIVERING, expected_revision=work.revision, lease=run.lease
            )
            tx.transition_work(
                work.id,
                WorkState.COMPLETED,
                expected_revision=temporary.revision,
                lease=run.lease,
                reason="authorized_silence",
            )
    with env.store.transaction(env.actor) as tx:
        tx.record_validation(
            work.id,
            run.lease,
            result.silent_evidence,
            check_key="silent_result",
        )
        prepared = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=work.revision, lease=run.lease
        )
        ended = tx.transition_work(
            work.id,
            WorkState.COMPLETED,
            expected_revision=prepared.revision,
            lease=run.lease,
            reason="authorized_silence",
        )
        assert ended.state == WorkState.COMPLETED
        assert not tx.list_delivery()


async def test_due_work_without_silence_grant_cannot_escape_delivery(toolkit_setup):
    env = toolkit_setup.env
    run = await prepare(
        toolkit_setup,
        goal="Produce an authorized daily report.",
        scope={"origin_schedule_id": "synthetic-report", "derived_depth": 1},
    )
    assert "finish_silently" not in run.prepared.request.allowed_tools
    with pytest.raises(DomainError, match="neither an answer nor a file"):
        await toolkit_setup.toolkit.validate(
            run.work, run.lease, env.actor, candidate(run, ""), run.workspace
        )
