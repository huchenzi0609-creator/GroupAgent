"""Controller composition, not just helper deduplication; synthetic references."""

import json

import pytest
from conversation.test_react_turn import ModelScript, call, finish, tool_receipt
from media.conftest import png_fixture

from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import EventKind, Role
from groupagent.media import MediaSource
from groupagent.memory import DeterministicFixtureSummarizer, MemberService, MemoryService
from groupagent.scheduling import Scheduler

from .test_app import app_for, event
from .test_input_selection import accepted_image_ids, emit, image, sent_images

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("scenario", ["photo", "compare", "refine"])
async def test_new_attachment_work_has_only_current_inputs_unless_author_expands(
    runtime_env, scenario
):
    env = runtime_env
    combine = scenario == "compare"
    refine = scenario == "refine"
    old = emit(env, kind=EventKind.IMAGE, text="Synthetic unrelated project roster")
    old_version, _ = image(
        env, old, png_fixture("UNRELATED ROSTER"), name="old-screenshot.png", shared=True
    )
    cache_request = emit(env, text="Synthetic cache processing trigger")
    caption = "Pot，请描述我新拍的工作台。"
    authority = "把这张照片和我之前发的项目名单截图一起对照"
    if combine:
        caption = f"Pot，{authority}，指出区别。"
    handle = env.channel.add_attachment(
        "synthetic-current-workbench",
        "workbench.png",
        png_fixture("CURRENT WORKBENCH"),
        kind="image",
    )
    current = event(
        env,
        caption,
        kind=EventKind.IMAGE,
        attachments=[handle.as_attachment()],
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.ingest_event(current, current.id, "1", 0).event
    cached, _ = image(
        env,
        cache_request,
        png_fixture("CURRENT WORKBENCH"),
        provenance=MediaSource(
            group_id=env.actor.group_id,
            work_id="assigned-in-fixture",
            source_event_id=current.id,
            source_message_id=handle.message_id,
            member_id=env.actor.member_id,
            attachment_id=handle.asset_id or handle.message_id,
            declared_kind="image",
            origin_kind="channel",
        ).model_dump(mode="json"),
        dependencies=(current.id,),
        shared=True,
    )
    mixed_goal = "Describe the new workbench and read the names in the previous project roster."
    criteria = ["Include a name from the previous roster."]

    def plan(request):
        context = json.loads(request.messages[1].content)["context"]
        catalog = context["reference_catalog"]
        assert set(catalog["attachment_event_ids"]) >= {old.id, current.id}
        assert set(catalog["file_versions"]) >= {cached.version_id, old_version.version_id}
        return call(
            "start_work",
            "fresh-attachment",
            goal=mixed_goal,
            acceptance_criteria=criteria,
            input_event_ids=[old.id, current.id],
            input_versions=[old_version.version_id, cached.version_id],
            admission_ack="immediate",
            finish_on_commit=not refine,
        )

    def review(request):
        assert json.loads(request.messages[1].content) == {"current_utterance": caption}
        assert len(request.messages) == 2
        assert [tool["function"]["name"] for tool in request.tools] == ["review_historical_inputs"]
        return call(
            "review_historical_inputs",
            "scope-evidence",
            authorization_excerpt=authority if combine else None,
        )

    def propose_refinement(request):
        (work,) = tool_receipt(request)["work_effects"]
        return call(
            "update_work",
            "refine-current-work",
            target_work_id=work["id"],
            expected_revision=work["revision"],
            goal=mixed_goal,
            acceptance_criteria=criteria,
            input_event_ids=[old.id],
            input_versions=[old_version.version_id],
            work_capabilities=["documents"],
            outputs=[{"format": "text"}],
        )

    def close(request):
        assert tool_receipt(request)["committed"] is True
        return finish(reports=[("refine-current-work", "committed")])

    model = ModelScript(plan, review, *([propose_refinement, close] if refine else []))
    app, _ = app_for(env, {})
    coordinator = ConversationCoordinator(
        app.coordinator.config,
        env.store,
        lambda actor, event: ModelDecisionProvider(model),
        memory=MemoryService(env.store, DeterministicFixtureSummarizer()),
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    try:
        result = await coordinator.process(current)
        assert result.work_id and result.delivery_id
        assert len(model.requests) == (4 if refine else 2)
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
        assert set(work.scope["attachment_event_ids"]) == (
            {old.id, current.id} if combine else {current.id}
        )
        assert set(work.input_versions) == (
            {old_version.version_id, cached.version_id} if combine else {cached.version_id}
        )
        assert work.goal == (mixed_goal if combine else caption)
        assert work.acceptance_criteria == (criteria if combine else [])
        assert work.goal_revision == (2 if refine else 1)
        if not combine:
            assert set(work.scope["source_event_ids"]) == {current.id, cache_request.id}
            assert work.scope["context_artifact_versions"] == [cached.version_id]
            assert work.scope["attachment_scope_restricted"] is True
        if refine:
            later = event(
                env,
                "Pot，请修订刚才的工作，把我回复的名单截图也加进去对照。",
                kind=EventKind.QUOTE,
                reply_to=old.id,
            )
            with env.store.transaction(env.actor) as tx:
                later = tx.ingest_event(later, later.id, "1", 0).event
            later_model = ModelScript(
                call(
                    "update_work",
                    "human-revision",
                    target_work_id=work.id,
                    expected_revision=work.revision,
                    goal=later.text,
                    input_event_ids=[old.id],
                    input_versions=[old_version.version_id],
                    finish_on_commit=True,
                )
            )
            coordinator.provider_factory = lambda actor, event: ModelDecisionProvider(later_model)
            revised = await coordinator.process(later)
            assert revised.work_id == work.id and len(later_model.requests) == 1
            with env.store.transaction(env.actor) as tx:
                updated = tx.get_work(work.id)
            assert updated.goal == later.text and updated.goal_revision == 3
            assert set(updated.scope["attachment_event_ids"]) == {current.id, old.id}
            assert updated.input_versions == [old_version.version_id]
    finally:
        await app.close()


async def test_native_receipt_only_quote_protects_its_exact_file_anchor(runtime_env):
    env = runtime_env
    source = emit(env, text="Synthetic work that made two distinct images")
    target, lease = image(env, source, png_fixture("QUOTED NATIVE IMAGE"), shared=True)
    other, _ = image(
        env,
        source,
        png_fixture("OTHER IMAGE"),
        lease=lease,
        name="unrelated.png",
        shared=True,
    )
    (native_id,) = await accepted_image_ids(env, [target], lease)
    current = event(
        env,
        "Pot，请描述我回复的这张图片。",
        kind=EventKind.QUOTE,
        reply_to=native_id,
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.ingest_event(current, current.id, "1", 0).event
        assert (
            tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND body->>'platform_message_id'=%s",
                (env.actor.group_id, native_id),
            ).fetchone()
            is None
        )

    def plan(request):
        available = json.loads(request.messages[1].content)["context"]["reference_catalog"][
            "file_versions"
        ]
        assert {target.version_id, other.version_id} <= set(available)
        return call(
            "start_work",
            "quoted-image",
            goal="Describe unrelated.png and infer the subject of the group discussion.",
            input_versions=[other.version_id],
            admission_ack="immediate",
            finish_on_commit=True,
        )

    model = ModelScript(
        plan, call("review_historical_inputs", "direct-only", authorization_excerpt=None)
    )
    app, _ = app_for(env, {})
    coordinator = ConversationCoordinator(
        app.coordinator.config,
        env.store,
        lambda actor, event: ModelDecisionProvider(model),
        memory=MemoryService(env.store, DeterministicFixtureSummarizer()),
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    try:
        result = await coordinator.process(current)
        assert result.work_id and result.delivery_id and len(model.requests) == 2
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(result.work_id)
        assert work.input_versions == [target.version_id]
        assert work.scope["attachment_event_ids"] == []
        assert work.scope["context_artifact_versions"] == [target.version_id]
        assert work.goal == current.text
    finally:
        await app.close()


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
