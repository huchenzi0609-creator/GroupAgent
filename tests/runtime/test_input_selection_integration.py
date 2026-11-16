"""Controller composition, not just helper deduplication; synthetic references."""

import pytest
from media.conftest import png_fixture

from groupagent.coordinator import Decision

from .test_app import app_for
from .test_input_selection import emit, image, sent_images

pytestmark = pytest.mark.postgres


async def test_controller_resolves_overselected_versions_and_bot_echoes_to_sent_pair(runtime_env):
    env = runtime_env
    originals, echoes = await sent_images(env)
    unrelated = [
        image(env, emit(env), png_fixture("Synthetic unrelated " + str(i)))[0] for i in range(2)
    ]
    request = emit(env, text="Pot，把已经发到群里的那两张松鼠表情包放进Word文档。")
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
