"""Synthetic regressions for public images, honest diagnostics and first feedback."""

import pytest

from groupagent.control import WorkSelector
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.coordinator import Decision
from groupagent.domain import DomainError, WorkState

from .foreground_model_fixture import ForegroundScriptedModel, candidate_reply, natural_reply
from .test_app import app_for, event
from .test_early_ack import intake, setup_chat

pytestmark = pytest.mark.postgres


def image_request_model(app, request, *, admission_ack=None):
    """Explicit semantic fixture using the actual graph's named primitive."""
    proposal = candidate_reply(
        Decision(
            kind="start_work",
            reason_code="synthetic_image_delegation",
            goal=request.text,
            directness="direct",
            confidence=1,
            work_capabilities=["public_images"],
            public_image_count=2,
            outputs=[{"format": "png"}],
        )
    )
    if admission_ack is not None:
        tool = proposal.tool_calls[0]
        tool = tool.model_copy(
            update={"arguments": {**tool.arguments, "admission_ack": admission_ack}}
        )
        proposal = proposal.model_copy(update={"tool_calls": (tool,)})
    model = ForegroundScriptedModel(
        [proposal, natural_reply("我会上网找现成图片，核对清晰度和来源后发到这里。")]
    )
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(model)
    return model


async def test_existing_meme_request_starts_retrieval_without_image_generation(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    request = event(env, "找两张“松鼠”的表情包给我，要清晰一点。")
    model = image_request_model(app, request)
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.work_id and len(model.requests) == 2
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
        assert work.scope["public_image_count"] == 2
        assert work.scope["research_required"] is True
        assert "web_image_import" in work.scope["required_tools"]
        assert "上网找现成图片" in result.decision.response
        assert not app.config.features.image_generation
    finally:
        await app.close()


async def test_pause_question_uses_actual_preparation_error_instead_of_cost_guess(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    source = event(env, "请分析刚发的图片配色。")
    try:
        await intake(app, env, source)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("分析图片配色", source.id)
            tx.transition_work(
                work.id,
                WorkState.PAUSED,
                expected_revision=work.revision,
                reason="ambiguous_attachment",
            )

        class StatusInterpreter:
            async def decide(self, event, context):
                return Decision(
                    kind="query_work",
                    reason_code="status_question",
                    work_selector=WorkSelector(
                        basis="query", states=(WorkState.PAUSED,), ownership="group"
                    ),
                    directness="direct",
                    confidence=1,
                )

        app.coordinator.provider_factory = lambda *_: StatusInterpreter()
        question = event(env, "为什么？是什么原因导致的暂停")
        await intake(app, env, question)
        result = await app.coordinator.process(question)
        assert result.decision.reason_code == "work_status_query"
        assert "附件版本" in result.decision.response
        assert "费用" not in result.decision.response and not model.requests
        assert result.action_result["states"] == ["paused"]
        assert result.action_result["work_ids"] == [work.id]
    finally:
        await app.close()


async def test_true_attachment_ambiguity_waits_for_input_without_finance_pause(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    request = event(env, "请分析图片。")
    try:
        await intake(app, env, request)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("分析图片", request.id)

        async def ambiguous(*args):
            raise DomainError("ambiguous_attachment", "Synthetic two equal-quality assets")

        app.supervisor.factory = ambiguous
        result = await app.supervisor.run_once()
        assert result.id == work.id and result.state == WorkState.WAITING_INPUT
        assert app.notices.tick() == 1
        with env.store.transaction(env.actor) as tx:
            texts = [d.payload_ref for d in tx.list_delivery()]
        assert texts and all("费用" not in text for text in texts)
        assert any("附件版本" in text for text in texts)
    finally:
        await app.close()


@pytest.mark.parametrize("admission_ack", [None, "immediate"])
async def test_background_preparation_waits_until_useful_admission_is_accepted(
    runtime_env, admission_ack
):
    env = runtime_env
    app, model, _ = setup_chat(env)
    app.supervisor.work_ready = app._foreground_ready
    request = event(env, "找两张“松鼠”的表情包给我。")
    model = image_request_model(app, request, admission_ack=admission_ack)
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.work_id and len(model.requests) == 2
        assert await app.supervisor.tick() == 0
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
            assert work.state == WorkState.QUEUED
            if admission_ack == "immediate":
                assert (
                    tx.get_decision(request.id)["work_admission"]["delivery_id"]
                    == result.delivery_id
                )
                assert work.goal in result.decision.response
        assert await app.dispatcher.tick() == 1
        assert await app.supervisor.tick() == 1
    finally:
        await app.close()


async def test_person_identity_question_returns_an_explicit_boundary_without_starting_work(
    runtime_env,
):
    env = runtime_env
    app, model, _ = setup_chat(env)
    request = event(env, "图片里这个男人是谁？")
    model.text = "我暂时无法识别人脸身份，但可以描述画面。"
    try:
        await intake(app, env, request)
        result = await app.coordinator.process(request)
        assert result.work_id is None and len(model.requests) == 1
        assert "描述画面" in result.decision.response
    finally:
        await app.close()
