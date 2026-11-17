"""Real scoped PostgreSQL/artifact control flow with synthetic inline image codecs.

No network, paid provider or OS sandbox acceptance is claimed by these tests.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from groupagent.artifacts import ArtifactScope
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory import MemoryService
from groupagent.research.image_transforms import (
    PublicImageValidationError,
    convert_image,
    resolve_public_image,
)
from groupagent.tools import EffectState, ToolReceipt

from .test_toolkit_contract import (
    SyntheticFixtureWorker,
    call,
    candidate,
    prepare,
)
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = [pytest.mark.postgres, pytest.mark.public_image_workflow]
_DELIVERY_CASES = json.loads(
    (
        Path(__file__).resolve().parents[1] / "fixtures" / "public_image_workflows.v1.json"
    ).read_text()
)["delivery_cases"]


@pytest.fixture
async def conversion_setup(toolkit_setup, monkeypatch):
    original = SyntheticFixtureWorker.run

    async def run(self, context, operation, inputs, *, parameters=None, outputs=(), **kwargs):
        if operation == "image_convert":
            self.calls.append(operation)
            if self.sandbox.policy_guard:
                await self.sandbox.policy_guard(context)
            params = dict(parameters or {})
            params["output_format"] = params.pop("format")
            result, payload = convert_image(inputs[0], **params)
            return result, (payload,)
        return await original(
            self, context, operation, inputs, parameters=parameters, outputs=outputs, **kwargs
        )

    monkeypatch.setattr(SyntheticFixtureWorker, "run", run)
    return toolkit_setup


async def imported_jpegs(setup, *, count=2, request_text=None):
    import io

    from PIL import Image

    text = request_text or f"请交付{count}张已验证的公开来源图片，全部使用PNG格式。"
    run = await prepare(
        setup,
        goal=text,
        request_text=text,
        scope={"public_image_count": count, "outputs": [{"format": "png"}]},
    )
    _, media, context = setup.toolkit._services[run.lease.attempt_id]
    original_source = await setup.toolkit._source(run.work, setup.env.actor, "image")
    originals = []
    for index, color in enumerate(("navy", "red", "green", "gold", "purple")[:count]):
        output = io.BytesIO()
        Image.new("RGB", (64 + index, 48 + index), color).save(output, "JPEG")
        source = original_source.model_copy(
            update={
                "origin_kind": "research",
                "source_url": f"https://images.example.com/image-{index}.jpg",
            }
        )
        ref = await media.ingest_image(
            context,
            source,
            output.getvalue(),
            research_provenance={
                "requested_url": source.source_url,
                "page_url": "https://example.com/page",
                "page_final_url": "https://example.com/page",
                "page_sha256": "a" * 64,
                "page_retrieved_at": "2026-11-16T00:00:00+00:00",
                "retrieved_at": "2026-11-16T00:00:01+00:00",
                "discovery": "img_src",
                "content_type": "image/jpeg",
            },
        )
        version = setup.env.artifacts.get(media.scope(context), ref.file_id, ref.version_id)
        setup.env.registry.register(setup.env.actor, run.lease, version, purpose="candidate")
        originals.append(version)
    return run, originals


async def convert_and_select(run, versions):
    converted = []
    for version in versions:
        receipt = await call(
            run,
            "image_convert",
            {"file_id": version.file_id, "version_id": version.version_id, "format": "png"},
            key="convert:" + version.version_id,
        )
        assert receipt.success, receipt
        converted.append(receipt)
    selected = await call(
        run,
        "artifact_deliver",
        {"version_ids": [item.output["version_id"] for item in converted]},
        key="deliver-converted",
    )
    assert selected.success
    return (*converted, selected)


@pytest.mark.parametrize("scenario", _DELIVERY_CASES, ids=lambda case: case["id"])
async def test_toolkit_accepts_jpeg_to_trusted_png_results_and_real_delivery_drafts(
    conversion_setup,
    scenario,
):
    setup = conversion_setup
    count = scenario["count"]
    run, originals = await imported_jpegs(setup, count=count, request_text=scenario["request"])
    receipts = await convert_and_select(run, originals)
    unused_page = ToolReceipt(
        call_id="unused-candidate-page",
        tool_name="web_image_candidates",
        idempotency_key="unused-candidate-page",
        success=True,
        effect_state=EffectState.NO_EFFECT,
        output={
            "scope_fingerprint": "d" * 64,
            "requested_url": "https://unused.example.com/github-page",
            "final_url": "https://unused.example.com/github-page",
            "title": "Unused candidate page",
            "retrieved_at": "2026-11-16T00:00:00Z",
            "citation": "https://unused.example.com/github-page（页面访问时间 UTC：2026-11-16T00:00:00Z）",
            "html_sha256": "e" * 64,
            "html_bytes_received": 12,
            "candidates": [],
            "filtered_candidate_count": 0,
            "truncated": False,
        },
    )
    accepted = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "图片已准备好。", (*receipts, unused_page)),
        run.workspace,
    )
    drafts = [draft for draft in accepted.deliveries if draft.payload_ref.startswith("artifact:")]
    assert len(drafts) == count
    assert {draft.payload_ref for draft in drafts} == {
        "artifact:" + receipt.output["version_id"] for receipt in receipts[:-1]
    }
    scope = ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id)
    for draft in drafts:
        # Each result is independently decoded and bound to one original JPEG.
        version_id = draft.payload_ref.removeprefix("artifact:")
        version, payload = setup.env.registry.read(
            setup.env.actor, version_id, require_validated=True
        )
        assert payload.startswith(b"\x89PNG\r\n\x1a\n")
        assert version.format == "png" and draft.payload_hash == version.sha256
        assert resolve_public_image(setup.env.artifacts, scope, version).source_sha256 in {
            item.sha256 for item in originals
        }
    assert setup.http_calls == []
    assert accepted.deliveries[0].payload_ref == "text:图片已准备好。"
    assert "unused.example.com" not in accepted.deliveries[0].payload_ref
    review = setup.model.requests[-1]
    assert "public_image_source" in review.messages[1].content
    assert "2026-11-16T00:00:00Z" in review.messages[1].content
    assert all(
        receipt.output["page_citation"] and receipt.output["image_citation"]
        for receipt in receipts[:-1]
    )


@pytest.mark.parametrize("show_time", [False, True])
async def test_requested_source_page_survives_reviewed_reused_image_delivery(
    conversion_setup, show_time
):
    setup = conversion_setup
    request = "交付两张PNG图片，在群里给出原始来源页面链接。"
    if show_time:
        request += "保留页面实际访问时间。"
    run, originals = await imported_jpegs(setup, request_text=request)
    receipts = await convert_and_select(run, originals)
    page = "https://example.com/page"
    if show_time:
        page += "（页面访问时间 UTC：2026-11-16T00:00:00Z）"
    answer = "图片已准备好。\n" + page
    accepted = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, answer, receipts),
        run.workspace,
    )
    assert accepted.deliveries[0].payload_ref == "text:" + answer
    assert len(accepted.deliveries) == 3
    assert not setup.http_calls
    review = json.loads(setup.model.requests[-1].messages[1].content)
    assert review["original_request"]["text"] == request
    assert all(
        "page_retrieved_at" in artifact["public_image_source"] for artifact in review["artifacts"]
    )


async def test_toolkit_missing_format_diagnostic_reuses_valid_existing_imports(conversion_setup):
    setup = conversion_setup
    run, originals = await imported_jpegs(setup)
    selected = await call(
        run,
        "artifact_deliver",
        {"version_ids": [item.version_id for item in originals]},
        key="deliver-jpegs",
    )
    assert selected.success
    with pytest.raises(PublicImageValidationError) as failure:
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(selected,)),
            run.workspace,
        )
    assert failure.value.details["validated_distinct_count"] == 2
    assert failure.value.details["missing_count_by_format"] == {"png": 2}
    assert failure.value.details["duplicate_version_ids"] == []
    assert {item["version_id"] for item in failure.value.details["reusable_images"]} == {
        item.version_id for item in originals
    }
    receipts = await convert_and_select(run, originals)
    result = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
    )
    assert len(result.deliveries) == 3 and setup.http_calls == []


async def test_toolkit_two_encodings_of_one_source_cannot_satisfy_two_images(conversion_setup):
    setup = conversion_setup
    run, originals = await imported_jpegs(
        setup, count=1, request_text="请交付2张已验证的公开来源图片，全部使用PNG格式。"
    )
    # Keep the accepted task count at two while creating only one source.
    with setup.env.store.transaction(setup.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=jsonb_set(scope,'{public_image_count}','2') WHERE group_id=%s AND id=%s",
            (setup.env.actor.group_id, run.work.id),
        )
        work = tx.get_work(run.work.id)
    receipts = await convert_and_select(run, originals)
    converted_id = receipts[0].output["version_id"]
    selected = await call(
        run,
        "artifact_deliver",
        {"version_ids": [originals[0].version_id, converted_id]},
        key="deliver-two-encodings",
    )
    with pytest.raises(PublicImageValidationError) as failure:
        await setup.toolkit.validate(
            work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(*receipts, selected)),
            run.workspace,
        )
    assert failure.value.details["validated_distinct_count"] == 1
    assert len(failure.value.details["duplicate_version_ids"]) == 1


async def test_legacy_planning_five_does_not_block_four_valid_png_images(conversion_setup):
    setup = conversion_setup
    run, originals = await imported_jpegs(
        setup, count=4, request_text="请找海豹表情包，交付PNG图片。"
    )
    assert json.loads(run.prepared.request.objective)["requested_image_count"] is None
    with setup.env.store.transaction(setup.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=jsonb_set(scope,'{public_image_count}','5'),"
            "goal='查找五张海豹表情包，交付PNG图片。',acceptance_criteria='[\"交付五张PNG图片\"]'::jsonb WHERE group_id=%s AND id=%s",
            (setup.env.actor.group_id, run.work.id),
        )
        run.work = tx.get_work(run.work.id)
    run.prepared = await setup.toolkit.prepare(run.work, run.lease, setup.env.actor, run.workspace)
    objective = json.loads(run.prepared.request.objective)
    assert "五张" in objective["goal"]
    assert objective["image_quantity_requirement"]["public_image_count"] is None
    assert any(
        "host-verified image_quantity_requirement" in instruction
        for instruction in objective["instructions"]
    )
    receipts = await convert_and_select(run, originals)
    accepted = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
    )
    assert (
        len([draft for draft in accepted.deliveries if draft.payload_ref.startswith("artifact:")])
        == 4
    )
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert tx.get_work(run.work.id).scope["public_image_count"] == 5
    assert setup.http_calls == []
    review = setup.model.requests[-1]
    review_payload = json.loads(review.messages[1].content)
    assert "五张" in review_payload["goal"] and "五张" in review_payload["criteria"][0]
    assert review_payload["image_quantity_requirement"]["public_image_count"] is None
    assert "only the host-verified image_quantity_requirement" in review.messages[0].content


async def test_unspecified_quantity_still_rejects_empty_or_wrong_format_delivery(conversion_setup):
    setup = conversion_setup
    run, originals = await imported_jpegs(
        setup, count=1, request_text="请找海豹表情包，交付PNG图片。"
    )
    for selected_ids in ([], [originals[0].version_id]):
        if selected_ids:
            selected = await call(run, "artifact_deliver", {"version_ids": selected_ids})
            assert selected.success
        with pytest.raises(PublicImageValidationError):
            await setup.toolkit.validate(
                run.work, run.lease, setup.env.actor, candidate(run), run.workspace
            )
    assert setup.http_calls == []


@pytest.mark.parametrize("withdrawal", ["catalog", "forgotten", "changed_bytes", "departed"])
async def test_derived_output_cannot_outlive_parent_authority(conversion_setup, withdrawal):
    setup = conversion_setup
    run, originals = await imported_jpegs(setup)
    receipts = await convert_and_select(run, originals)
    version_id = receipts[0].output["version_id"]
    if withdrawal == "catalog":
        with setup.env.store.transaction(setup.env.actor) as tx:
            tx.conn.execute(
                "UPDATE ga_artifact_catalog SET available=false WHERE group_id=%s AND version_id=%s",
                (setup.env.actor.group_id, originals[0].version_id),
            )
    elif withdrawal == "forgotten":
        MemoryService(setup.env.store).forget(setup.env.actor, event_ids=[run.event.id])
    elif withdrawal == "changed_bytes":
        original = originals[0]
        path = (
            setup.env.root
            / "artifacts"
            / original.file_id
            / "versions"
            / original.version_id
            / "content"
        )
        path.write_bytes(b"changed after validation")
    else:
        with setup.env.store.transaction(setup.env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_members(group_id,member_id,membership_state) VALUES(%s,%s,'left') ON CONFLICT(group_id,member_id) DO UPDATE SET membership_state='left'",
                (setup.env.actor.group_id, setup.env.actor.member_id),
            )
    with pytest.raises(DomainError):
        setup.env.registry.read(setup.env.actor, version_id)
    with pytest.raises(DomainError):
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
        )


@pytest.mark.parametrize("withdrawal", ["forgotten", "reader_departed"])
async def test_shared_derivative_can_be_reused_without_granting_private_parent_access(
    conversion_setup, withdrawal
):
    from .test_artifact_provenance_inputs import admitted_work, importer

    setup = conversion_setup
    env = setup.env
    run, originals = await imported_jpegs(setup)
    receipts = await convert_and_select(run, originals)
    await setup.toolkit.validate(
        run.work, run.lease, env.actor, candidate(run, receipts=receipts), run.workspace
    )
    shared_id = receipts[0].output["version_id"]
    with env.store.transaction(env.actor) as tx:
        env.registry.mark_shared_in_tx(tx, shared_id)
    reader = ActorContext(
        group_id=env.actor.group_id, member_id="synthetic-other-member", role=Role.MEMBER
    )
    shared, payload = env.registry.read(reader, shared_id, require_validated=True)
    assert payload.startswith(b"\x89PNG\r\n\x1a\n")
    with pytest.raises(DomainError) as private:
        env.registry.read(reader, originals[0].version_id)
    assert private.value.code == "permission_denied"

    # Use the ordinary authorized artifact-input path, including its source
    # dependency snapshot, rather than granting the reader any raw parent scope.
    reader_env = SimpleNamespace(**{**vars(env), "actor": reader})
    work, lease, context = admitted_work(reader_env, [run.event], [shared])
    processor, _, documents, media = importer(reader_env)
    inputs = await processor.import_for_work(work, lease, context, documents, media)
    assert len(inputs) == 1 and inputs[0]["from_version"] == shared_id
    copied, copied_bytes = env.registry.read(reader, inputs[0]["version_id"])
    assert copied.sha256 == shared.sha256 and copied_bytes == payload
    lineage = resolve_public_image(env.artifacts, media.scope(context), copied)
    assert lineage.source_sha256 == originals[0].sha256
    with pytest.raises(DomainError) as still_private:
        env.registry.read(reader, originals[0].version_id)
    assert still_private.value.code == "permission_denied"

    if withdrawal == "forgotten":
        MemoryService(env.store).forget(env.actor, event_ids=[run.event.id])
    else:
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_members(group_id,member_id,membership_state) VALUES(%s,%s,'left') ON CONFLICT(group_id,member_id) DO UPDATE SET membership_state='left'",
                (reader.group_id, reader.member_id),
            )
    for version_id in (shared_id, copied.version_id):
        with pytest.raises(DomainError):
            env.registry.read(reader, version_id)
