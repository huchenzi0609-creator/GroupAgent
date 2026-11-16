"""Controller composition, not just helper deduplication; synthetic references."""

import pytest
from media.conftest import png_fixture

from groupagent.coordinator import Decision
from groupagent.domain import Role

from .test_app import app_for
from .test_input_selection import emit, image, sent_images

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("prefix", ["", "新建独立工作："])
async def test_controller_resolves_overselected_versions_and_bot_echoes_to_sent_pair(
    runtime_env, prefix
):
    env = runtime_env
    originals, echoes = await sent_images(env)
    unrelated = [
        image(env, emit(env), png_fixture("Synthetic unrelated " + str(i)))[0] for i in range(2)
    ]
    request = emit(env, text="Pot，" + prefix + "把已经发到群里的那两张松鼠表情包放进Word文档。")
    candidate = Decision(
        kind="start_work",
        reason_code="synthetic_overselected_inputs",
        goal="把已发送的两张图放Word",
        input_versions=[x.version_id for x in [*originals, *unrelated]],
        input_event_ids=[x.id for x in echoes],
        outputs=[{"format": "docx"}],
        directness="direct",
        confidence=1,
    )
    app, _ = app_for(env, {request.id: candidate})
    try:
        result = await app.coordinator.process(request)
        assert result.work_id
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
        assert set(work.input_versions) == {x.version_id for x in originals}
        assert work.scope["attachment_event_ids"] == []
        assert set(result.decision.input_versions) == set(work.input_versions)
        assert result.decision.input_event_ids == []
    finally:
        await app.close()


async def test_controller_rejects_private_alias_before_reducing_to_shared_sent_images(runtime_env):
    env = runtime_env
    originals, echoes = await sent_images(env)
    peer = env.actor.model_copy(
        update={"member_id": "synthetic-private-owner", "role": Role.MEMBER}
    )
    private, _ = image(
        env,
        emit(env, actor=peer),
        png_fixture("POSTED SYNTHETIC SQUIRREL 0"),
        actor=peer,
        provenance={**originals[0].provenance, "input_version_id": originals[0].version_id},
        dependencies=(originals[0].provenance["source_event_id"],),
    )
    request = emit(env, text="Pot，新建独立工作，把已经发到群里的那两张松鼠表情包放Word。")
    candidate = Decision(
        kind="start_work",
        reason_code="synthetic_private_alias_guess",
        goal=request.text,
        input_versions=[value.version_id for value in [*originals, private]],
        input_event_ids=[value.id for value in echoes],
        outputs=[{"format": "docx"}],
        directness="direct",
        confidence=1,
    )
    app, _ = app_for(env, {request.id: candidate})
    try:
        context = app.coordinator._context(env.actor, request)
        assert private.version_id not in {row["version_id"] for row in context["files"]}
        result = await app.coordinator.process(request)
        assert result.work_id is None and result.decision.reason_code == "permission_denied"
    finally:
        await app.close()
