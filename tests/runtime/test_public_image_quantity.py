"""Real admission transactions preserve human quantities across plans and revisions."""

import pytest

from groupagent.coordinator import Decision, RequirementEvidence

from .conftest import source
from .test_app import app_for, event
from .test_early_ack import intake

pytestmark = [pytest.mark.postgres, pytest.mark.public_image_workflow]


def plan(**fields):
    return Decision(
        kind="start_work",
        reason_code="synthetic_image_plan",
        goal="查找海豹表情包，交付PNG图片。",
        outputs=[{"format": "png"}],
        work_capabilities=["public_images"],
        directness="direct",
        confidence=1,
        **fields,
    )


@pytest.mark.parametrize("proof", [False, True])
async def test_wakeup_only_inherits_an_explicit_referenced_human_quantity(runtime_env, proof):
    env = runtime_env
    original = source(env, "Pot，找两张海豹表情包发到群里。")
    wake = event(env, "Pot！")
    decision = plan(
        public_image_count=2 if proof else 5,
        public_image_count_evidence=RequirementEvidence(
            event_id=original.id, quote="找两张海豹表情包"
        )
        if proof
        else None,
    )
    app, models = app_for(env, {wake.id: decision})
    try:
        await intake(app, env, wake)
        result = await app.coordinator.process(wake)
        assert result.work_id
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
            raw = tx.get_decision(wake.id)["result"]["decision"]
        assert work.scope["public_image_count"] == (2 if proof else None)
        assert work.scope["planner_public_image_count"] == (2 if proof else 5)
        assert raw["public_image_count"] == (2 if proof else 5)
        assert bool(work.scope["public_image_count_evidence"]) is proof
        assert not models
    finally:
        await app.close()


async def test_explicit_image_count_does_not_depend_on_a_request_verb(runtime_env):
    env = runtime_env
    quote = "挑两张海豹表情包放在文件里"
    request = event(env, "Pot，" + quote + "。")
    decision = plan(
        public_image_count=2,
        public_image_count_evidence=RequirementEvidence(event_id=request.id, quote=quote),
    )
    app, _ = app_for(env, {request.id: decision})
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.work_id, result.decision
        with env.store.transaction(env.actor) as tx:
            actual = tx.get_work(result.work_id)
        assert actual.scope["public_image_count"] == 2
        assert actual.scope["public_image_count_evidence"]["event_id"] == request.id
    finally:
        await app.close()


async def test_unrelated_revision_preserves_grounded_two_and_correction_replaces_it(runtime_env):
    env = runtime_env
    original = event(env, "Pot，找两张海豹表情包。")
    decisions = {}
    app, models = app_for(env, decisions)
    try:
        await intake(app, env, original)
        admitted = await app.coordinator.process(original)
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(admitted.work_id)
        assert work.scope["public_image_count"] == 2
        for text, count, quote, expected in (
            ("Pot，格式改成PNG。", None, None, 2),
            ("Pot，不要五张，改成两张。", 2, "改成两张", 2),
            ("Pot，数量不限。", None, "数量不限", None),
        ):
            update = event(env, text, reply_to=original.id)
            decisions[update.id] = Decision(
                kind="update_work",
                reason_code="synthetic_quantity_revision",
                goal="查找海豹表情包并交付PNG图片。",
                target_work_id=work.id,
                expected_revision=work.revision,
                public_image_count=count,
                public_image_count_evidence=RequirementEvidence(event_id=update.id, quote=quote)
                if quote
                else None,
                directness="direct",
                confidence=1,
            )
            await intake(app, env, update)
            changed = await app.coordinator.process(update)
            assert changed.work_id == work.id, changed.decision
            with env.store.transaction(env.actor) as tx:
                work = tx.get_work(work.id)
            assert work.scope["public_image_count"] == expected
            assert work.goal_revision >= 2
        assert not models
    finally:
        await app.close()
