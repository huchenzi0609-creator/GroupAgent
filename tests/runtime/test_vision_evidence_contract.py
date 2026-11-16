"""Real PG/storage bytes, inline image decoder and explicit fixture reviewers.

The tests verify evidence/validation control flow, not live vision reliability
or Docker isolation. All HTTP uses MockTransport and synthetic pricing.
"""

import io
import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from PIL import Image, PngImagePlugin

from groupagent.artifacts import ArtifactScope
from groupagent.billing import PriceSnapshot
from groupagent.domain import DomainError
from groupagent.execution import ModelReply, TokenUsage
from groupagent.media import MediaHTTPProvider, ProviderRoute

from .test_toolkit_contract import call, candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres


def png(color):
    image = Image.new("RGB", (48, 32), color)
    metadata = PngImagePlugin.PngInfo()
    metadata.add_text("synthetic", "Original metadata is not a provider view")
    output = io.BytesIO()
    image.save(output, "PNG", pnginfo=metadata, compress_level=0)
    return output.getvalue()


@pytest.fixture
async def vision_setup(toolkit_setup):
    setup = toolkit_setup
    setup.toolkit.ledger.register_price(
        setup.env.actor,
        PriceSnapshot(
            id="fixture-vision",
            provider="synthetic-only",
            operation="media.vision",
            currency="CNY",
            rates={"input_tokens": Decimal("0.000001"), "output_tokens": Decimal("0.000001")},
            source_url="fixture://not-a-commercial-price",
            captured_at=datetime.now(UTC),
            test_only=True,
        ),
    )
    setup.vision_http = []

    def respond(request):
        setup.vision_http.append(request)
        return httpx.Response(
            200,
            json={
                "id": "synthetic-vision",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": "Synthetic observation. " * 600},
                    }
                ],
                "usage": {"prompt_tokens": 400, "completion_tokens": 20},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        setup.toolkit.providers["deepseek"] = MediaHTTPProvider(
            "deepseek", "synthetic-never-used-live", client=http
        )
        setup.toolkit.routes["fixture-vision"] = ProviderRoute(
            route_id="fixture-vision",
            provider="deepseek",
            capability="vision",
            model="deepseek-flash",
            price_id="fixture-vision",
        )
        yield setup


async def registered_images(setup, *, changed_candidate=False):
    text = "使用程序生成一张PNG并核对当前图像内容。"
    run = await prepare(setup, goal=text, scope={"outputs": [{"format": "png"}]})
    _, media, context = setup.toolkit._services[run.lease.attempt_id]
    source = await setup.toolkit._source(run.work, setup.env.actor, "image")
    analyzed = await media.ingest_image(context, source, png("navy"))
    artifact_scope = ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id)
    version = setup.env.artifacts.get(artifact_scope, analyzed.file_id, analyzed.version_id)
    setup.env.registry.register(setup.env.actor, run.lease, version, purpose="candidate")
    observation = await call(
        run,
        "image_analyze",
        {
            "images": [{"file_id": analyzed.file_id, "version_id": analyzed.version_id}],
            "prompt": "Describe this synthetic image",
            "route_id": "fixture-vision",
        },
    )
    assert observation.success
    selected = analyzed
    if changed_candidate:
        selected = await media.ingest_image(context, source, png("orange"))
        version = setup.env.artifacts.get(artifact_scope, selected.file_id, selected.version_id)
        setup.env.registry.register(setup.env.actor, run.lease, version, purpose="candidate")
    delivered = await call(run, "artifact_deliver", {"version_ids": [selected.version_id]})
    assert delivered.success
    return run, analyzed, selected, observation, delivered


@pytest.mark.parametrize("changed_candidate", [False, True])
async def test_original_view_mapping_survives_long_text_and_checks_current_candidate(
    vision_setup, monkeypatch, changed_candidate
):
    setup = vision_setup
    run, analyzed, selected, observation, delivered = await registered_images(
        setup, changed_candidate=changed_candidate
    )
    binding = observation.output["image_bindings"][0]
    assert binding["source_sha256"] == analyzed.sha256
    assert binding["source_sha256"] != binding["normalized_view_sha256"]
    seen = []

    async def explicit_binding_critic(request):
        seen.append(request)
        payload = json.loads(request.messages[1].content)
        assert "image_bindings" not in payload["tool_evidence"][0]["output"]
        evidence = payload["vision_image_bindings"]
        assert len(evidence) == 1
        assert evidence[0]["source_sha256"] == analyzed.sha256
        assert evidence[0]["normalized_view_sha256"] == observation.output["image_sha256"][0]
        assert evidence[0]["source"]["source_event_id"] == run.event.id
        assert evidence[0]["file_id"] == analyzed.file_id
        assert evidence[0]["version_id"] == analyzed.version_id
        assert evidence[0]["matches_candidate"] is (not changed_candidate)
        artifact = payload["artifacts"][0]
        assert (artifact["file_id"], artifact["version_id"], artifact["source_sha256"]) == (
            selected.file_id,
            selected.version_id,
            selected.sha256,
        )
        assert (
            "static_views=false" in request.messages[0].content
            and "规范化" in request.messages[0].content
        )
        assert "SHA" in request.messages[0].content and "比较" in request.messages[0].content
        return ModelReply(
            text=json.dumps(
                {
                    "passed": evidence[0]["matches_candidate"],
                    "reason": "Exact candidate binding"
                    if not changed_candidate
                    else "Different image is not candidate evidence",
                }
            ),
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", explicit_binding_critic)
    args = (
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "图像已核验。", (observation, delivered)),
        run.workspace,
    )
    if changed_candidate:
        with pytest.raises(
            DomainError, match="Different image is not candidate evidence"
        ) as failed:
            await setup.toolkit.validate(*args)
        assert failed.value.code == "validation_failed"
    else:
        result = await setup.toolkit.validate(*args)
        assert result.checks["result"] and result.checks["artifact:png"]
        assert any(d.payload_ref == "artifact:" + selected.version_id for d in result.deliveries)
    assert len(seen) == 1 and len(setup.vision_http) == 1
    with setup.env.store.transaction(setup.env.actor) as tx:
        recorded = tx.get_work(run.work.id).scope["_runtime_semantic_reviews"][-1]
    assert recorded["accepted"] is (not changed_candidate)


@pytest.mark.parametrize(
    "corruption",
    ["source_sha256", "normalized_view_sha256", "source", "image_index", "file_version"],
)
async def test_inconsistent_binding_cannot_reach_critic(vision_setup, corruption):
    setup = vision_setup
    run, _, selected, observation, delivered = await registered_images(
        setup, changed_candidate=corruption == "file_version"
    )
    body = json.loads(json.dumps(observation.output))
    binding = body["image_bindings"][0]
    if corruption == "source":
        binding["source"]["source_message_id"] = "not-the-observed-source"
    elif corruption == "image_index":
        binding["image_index"] = 1
    elif corruption == "file_version":
        binding["file_id"], binding["version_id"] = selected.file_id, selected.version_id
    else:
        binding[corruption] = "0" * 64
    invalid = observation.model_copy(update={"output": body})
    with pytest.raises(DomainError, match="Image analysis binding") as failed:
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "Synthetic result", (invalid, delivered)),
            run.workspace,
        )
    assert failed.value.code == "validation_failed"
    assert not setup.model.requests and len(setup.vision_http) == 1


async def test_source_only_observation_does_not_impose_a_global_candidate_vision_gate(
    vision_setup, monkeypatch
):
    setup = vision_setup
    run, _, selected, observation, delivered = await registered_images(
        setup, changed_candidate=True
    )
    seen = []

    async def explicit_other_evidence_critic(request):
        payload = json.loads(request.messages[1].content)
        seen.append(payload)
        assert payload["vision_image_bindings"][0]["matches_candidate"] is False
        assert payload["artifacts"][0]["source_sha256"] == selected.sha256
        # A fixture reviewer independently checks the actual output bytes. The
        # source observation is never represented as an analysis of this PNG.
        raw = setup.env.artifacts.read(
            ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id),
            selected.file_id,
            selected.version_id,
        )
        with Image.open(io.BytesIO(raw)) as image:
            assert image.format == "PNG" and image.getpixel((10, 10)) == (255, 165, 0)
        return ModelReply(
            text=json.dumps(
                {"passed": True, "reason": "Independent actual candidate-byte fixture"}
            ),
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    monkeypatch.setattr(setup.model, "complete", explicit_other_evidence_critic)
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "已按输入材料生成结果图。", (observation, delivered)),
        run.workspace,
    )
    assert result.checks["result"] and len(seen) == 1 and len(setup.vision_http) == 1
