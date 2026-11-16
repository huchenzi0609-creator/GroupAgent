"""Ordered bounded views and failure accounting, using real pixels and offline HTTP."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
from dataclasses import replace

import httpx
import pytest
from PIL import Image

from groupagent.artifacts import ArtifactStore
from groupagent.domain import DomainError
from groupagent.media import MediaHTTPProvider, MediaService, register_media_tools
from groupagent.media.models import VisionResult
from groupagent.media.providers import vision_request_bytes
from groupagent.media.validation import inspect_image
from groupagent.tools import EffectState, ToolGateway
from groupagent.tools.models import ToolFailure

from .test_providers import route
from .test_service_contract import NoChargeFixtureBudget

pytestmark = pytest.mark.public_image_workflow


class SerialPixelWorker:
    """Actual bounded decoder; inline fixture does not establish OS isolation."""

    def __init__(self):
        self.calls = []
        self.active = 0
        self.peak_active = 0

    async def run(self, context, operation, inputs, *, parameters=None, outputs=()):
        assert operation == "image_validate" and len(inputs) == 1
        self.active += 1
        self.peak_active = max(self.active, self.peak_active)
        try:
            await asyncio.sleep(0)
            info, pixels = inspect_image(inputs[0], **(parameters or {}))
            self.calls.append((info.source_sha256, parameters))
            return info.model_dump(mode="json"), (pixels,)
        finally:
            self.active -= 1


class AttemptBudget(NoChargeFixtureBudget):
    def __init__(self):
        super().__init__()
        self.begins = []
        self.settlements = []
        self.unknowns = []

    def begin(self, actor, identifier):
        self.begins.append(identifier)
        return super().begin(actor, identifier)

    def settle(self, actor, identifier, units, evidence):
        self.settlements.append(identifier)
        return super().settle(actor, identifier, units, evidence)

    def unknown(self, actor, identifier, reason):
        self.unknowns.append(identifier)
        return super().unknown(actor, identifier, reason)


def pixels(index: int) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (20 + index, 16), (index * 31, 60, 120)).save(output, "PNG")
    return output.getvalue()


async def references(service, context, source, count=5):
    values = []
    for index in range(count):
        values.append(
            await service.ingest_image(
                context,
                source.model_copy(
                    update={
                        "source_event_id": f"event-{index}",
                        "source_message_id": f"message-{index}",
                        "attachment_id": f"attachment-{index}",
                        "sequence": index,
                    }
                ),
                pixels(index),
            )
        )
    return tuple(values)


def provider_view(request):
    body = json.loads(request.content)
    content = next(item for item in body["messages"] if item["role"] == "user")["content"]
    images = tuple(
        base64.b64decode(item["image_url"]["url"].split(",", 1)[1])
        for item in content
        if item["type"] == "image_url"
    )
    labels = tuple(item["text"] for item in content[1:] if item["type"] == "text")
    return body, content[0]["text"], images, labels


def response(identifier):
    return httpx.Response(
        200,
        json={
            "id": identifier,
            "choices": [{"finish_reason": "stop", "message": {"content": identifier + " facts"}}],
            "usage": {"prompt_tokens": 40, "completion_tokens": 12},
        },
    )


async def test_five_images_use_four_plus_one_with_global_bindings_and_serial_work(
    tmp_path, media_context, media_source
):
    worker, budget, requests = SerialPixelWorker(), AttemptBudget(), []

    def respond(request):
        requests.append((request, len(worker.calls)))
        return response(f"batch-{len(requests)}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                worker,
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
                max_images=4,
                max_image_bytes=8 * 1024**2,
                max_image_pixels=12_000_000,
            )
            inputs = await references(service, media_context, media_source)
            worker.calls.clear()
            result = await service.analyze_images(
                media_context, inputs, "Read each image", route("vision")
            )

    assert result.complete and result.analysis_scope == "bounded_views"
    assert result.requested_image_count == 5 and result.unprocessed_image_indices == ()
    assert tuple(batch.image_indices for batch in result.batches) == ((0, 1, 2, 3), (4,))
    assert tuple(batch.request_id for batch in result.batches) == ("batch-1", "batch-2")
    assert [count for _, count in requests] == [4, 5]
    assert worker.peak_active == 1
    assert all(
        limits == {"max_bytes": 8 * 1024**2, "max_pixels": 12_000_000} for _, limits in worker.calls
    )
    assert len(budget.begins) == len(budget.settlements) == len(result.budget_receipts) == 2
    assert not budget.unknowns and set(budget.states.values()) == {"settled"}
    sent = []
    for request, _ in requests:
        _, prompt, images, labels = provider_view(request)
        assert "Bounded view:" in prompt and "outside this view" in prompt
        sent.extend(images)
        assert len(images) == len(labels)
    assert len(sent) == 5
    for index, (reference, binding, image) in enumerate(
        zip(inputs, result.image_bindings, sent, strict=True)
    ):
        assert (binding.image_index, binding.file_id, binding.version_id, binding.source) == (
            index,
            reference.file_id,
            reference.version_id,
            reference.source,
        )
        assert binding.source_sha256 == reference.sha256
        assert binding.normalized_view_sha256 == hashlib.sha256(image).hexdigest()
    assert "joint visual comparison across views has not been performed" in result.text


@pytest.mark.parametrize("limit", ["image_count", "json_bytes"])
async def test_service_batches_before_reviewed_route_limits_and_billing(
    tmp_path, media_context, media_source, limit
):
    budget, captured = AttemptBudget(), []

    def respond(request):
        captured.append(request)
        return response(f"route-batch-{len(captured)}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            provider = MediaHTTPProvider("deepseek", "fixture-key", client=http)
            service = MediaService(
                store,
                SerialPixelWorker(),
                providers={"deepseek": provider},
                budget=budget,
                max_images=4,
            )
            inputs = await references(service, media_context, media_source)
            spec = service.registry.resolve(route("vision"))
            if limit == "image_count":
                spec = replace(spec, max_images=2)
            else:
                # Includes every bounded prompt/label plus one real image, but
                # cannot hold two. The real provider enforces the same spec.
                spec = replace(spec, max_request_bytes=1150)
            key = (spec.provider, spec.capability, spec.model)
            service.registry._specs[key] = spec
            provider.registry = service.registry
            result = await service.analyze_images(media_context, inputs, "Read", route("vision"))
    assert result.complete
    sizes = [len(provider_view(request)[2]) for request in captured]
    assert sizes == ([2, 2, 1] if limit == "image_count" else [1, 1, 1, 1, 1])
    assert all(
        len(json.dumps(json.loads(request.content)).encode()) <= spec.max_request_bytes
        for request in captured
    )
    assert len(budget.begins) == len(budget.settlements) == len(captured)


async def test_second_batch_failure_replays_partial_and_only_remaining_image_needs_new_attempt(
    tmp_path, media_context, media_source
):
    budget, captured = AttemptBudget(), []

    def respond(request):
        captured.append(request)
        if len(captured) == 2:
            raise httpx.ReadTimeout("Synthetic lost second response")
        return response(f"attempt-{len(captured)}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                SerialPixelWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
                max_images=4,
            )
            inputs = await references(service, media_context, media_source)
            gateway = ToolGateway()
            register_media_tools(gateway, service, {"vision": route("vision")})
            scope = replace(media_context.scope, allowed_tools=gateway.registered_names)
            gateway.bind_scope(scope)
            context = replace(media_context, scope=scope)
            arguments = {
                "images": [
                    {"file_id": item.file_id, "version_id": item.version_id} for item in inputs
                ],
                "prompt": "Read each image",
                "route_id": "vision",
            }
            receipt = await gateway.invoke("image_analyze", arguments, context)
            assert not receipt.success and receipt.effect_state == EffectState.NO_EFFECT
            assert receipt.control is None and receipt.error_code == "provider_outcome_unknown"
            partial = VisionResult.model_validate(receipt.output)
            assert not partial.complete and partial.unprocessed_image_indices == (4,)
            assert [batch.complete for batch in partial.batches] == [True, False]
            assert [batch.image_indices for batch in partial.batches] == [(0, 1, 2, 3), (4,)]
            assert partial.batches[1].error_code == "provider_outcome_unknown"
            assert len(partial.budget_receipts) == 2 and "attempt-1 facts" in partial.text
            assert "attempt-2" not in partial.text
            replay = await gateway.invoke(
                "image_analyze", arguments, replace(context, call_id="replay")
            )
            assert replay.replayed and replay.output == receipt.output and len(captured) == 2
            retry = await gateway.invoke(
                "image_analyze",
                {**arguments, "images": [arguments["images"][4]]},
                replace(context, call_id="retry-remainder", idempotency_key="retry-remainder"),
            )
            assert retry.success and retry.effect_state == EffectState.NO_EFFECT
            retried = VisionResult.model_validate(retry.output)
            assert retried.complete and retried.image_bindings[0].file_id == inputs[4].file_id
    assert [len(provider_view(request)[2]) for request in captured] == [4, 1, 1]
    assert len(budget.keys) == len(budget.begins) == 3
    assert budget.states[partial.budget_receipts[0]] == "settled"
    assert budget.states[partial.budget_receipts[1]] == "unknown"
    assert len(budget.settlements) == 2 and len(budget.unknowns) == 1


async def test_cancelled_second_batch_keeps_first_charge_and_never_repeats_it(
    tmp_path, media_context, media_source
):
    budget, captured, second_started = AttemptBudget(), [], asyncio.Event()

    async def respond(request):
        captured.append(request)
        if len(captured) == 2:
            second_started.set()
            await asyncio.Event().wait()
        return response("first-only")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                SerialPixelWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
                max_images=4,
            )
            inputs = await references(service, media_context, media_source)
            task = asyncio.create_task(
                service.analyze_images(media_context, inputs, "Read", route("vision"))
            )
            await asyncio.wait_for(second_started.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(DomainError) as failed:
                await service.analyze_images(media_context, inputs, "Read", route("vision"))
            assert failed.value.code == "provider_reconciliation_required"
    assert len(captured) == len(budget.keys) == len(budget.begins) == 2
    assert list(budget.states.values()) == ["settled", "unknown"]
    assert len(budget.settlements) == len(budget.unknowns) == 1


async def test_known_usage_is_settled_even_if_cancelled_while_provider_returns(
    tmp_path, media_context, media_source
):
    budget = AttemptBudget()

    def respond(request):
        media_context.cancelled.set()
        return response("known-charge")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                SerialPixelWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            inputs = await references(service, media_context, media_source, count=1)
            with pytest.raises(DomainError) as failed:
                await service.analyze_images(media_context, inputs, "Read", route("vision"))
            assert failed.value.code == "cancelled"
    assert len(budget.settlements) == 1 and not budget.unknowns
    assert set(budget.states.values()) == {"settled"}


async def test_later_decode_failure_preserves_completed_facts_without_an_extra_charge(
    tmp_path, media_context, media_source
):
    budget, captured = AttemptBudget(), []

    def respond(request):
        captured.append(request)
        return response("first-four")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            worker = SerialPixelWorker()
            service = MediaService(
                store,
                worker,
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
                max_images=4,
            )
            inputs = await references(service, media_context, media_source)
            # Existing artifacts cannot evade a stricter active pixel cap. The
            # first four fit; the fifth must fail before another reservation.
            service.max_image_pixels = 380
            with pytest.raises(ToolFailure) as failed:
                await service.analyze_images(media_context, inputs, "Read", route("vision"))
            partial = VisionResult.model_validate(failed.value.output)
            assert failed.value.code == "image_pixel_limit"
            assert not partial.complete and partial.unprocessed_image_indices == (4,)
            assert len(partial.image_bindings) == 4
            assert len(partial.batches) == 1 and partial.batches[0].complete
    assert len(captured) == len(budget.keys) == len(budget.settlements) == 1
    assert not budget.unknowns


async def test_withdrawn_sources_do_not_escape_in_partial_result(
    tmp_path, media_context, media_source
):
    budget, captured, withdrawn = AttemptBudget(), [], False

    async def guard(context, identifiers):
        if withdrawn:
            raise DomainError("source_unavailable", "Synthetic source withdrawal")

    def respond(request):
        nonlocal withdrawn
        captured.append(request)
        withdrawn = True
        return response("withdrawn-facts")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                SerialPixelWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
                source_guard=guard,
                max_images=4,
            )
            inputs = await references(service, media_context, media_source)
            with pytest.raises(DomainError) as failed:
                await service.analyze_images(media_context, inputs, "Read", route("vision"))
            assert failed.value.code == "source_unavailable"
            assert getattr(failed.value, "output", None) is None
    assert len(captured) == len(budget.settlements) == 1
    assert set(budget.states.values()) == {"settled"} and not budget.unknowns


@pytest.mark.parametrize(
    "name,capability,method,arguments",
    [
        ("image_analyze", "vision", "analyze_images", {"prompt": "Read"}),
        ("provider_ocr", "ocr", "ocr", {}),
        ("audio_transcribe", "asr", "transcribe", {}),
    ],
)
async def test_paid_inspection_metadata_never_claims_external_business_writes(
    tmp_path, media_context, media_source, monkeypatch, name, capability, method, arguments
):
    async def unknown_billing(*args, **kwargs):
        raise DomainError(
            "provider_outcome_unknown", "Synthetic uncertain billing", effect_state="unknown"
        )

    with ArtifactStore(tmp_path / "store") as store:
        service = MediaService(store, SerialPixelWorker())
        reference = (await references(service, media_context, media_source, count=1))[0]
        monkeypatch.setattr(service, method, unknown_billing)
        gateway = ToolGateway()
        register_media_tools(gateway, service, {"inspection": route(capability)})
        scope = replace(media_context.scope, allowed_tools=gateway.registered_names)
        gateway.bind_scope(scope)
        assert gateway.is_read_only(name) and not gateway.is_concurrency_safe(name)
        receipt = await gateway.invoke(
            name,
            {
                **arguments,
                "route_id": "inspection",
                **(
                    {"images": [{"file_id": reference.file_id}]}
                    if name == "image_analyze"
                    else {"file_id": reference.file_id}
                ),
            },
            replace(media_context, scope=scope),
        )
        assert not receipt.success and receipt.effect_state == EffectState.NO_EFFECT
        assert receipt.control is None


def test_request_size_estimate_counts_unicode_and_base64_without_view_copies():
    from groupagent.media.providers import _vision_body

    images, labels, prompt = (b"a", b"bb", b"ccc", b"dddd"), ("一", "two", "三", "four"), "读图"
    urls = tuple("data:image/png;base64," + base64.b64encode(image).decode() for image in images)
    expected = len(json.dumps(_vision_body(route("vision"), prompt, urls, labels)).encode())
    assert (
        vision_request_bytes(route("vision"), prompt, tuple(map(len, images)), labels) == expected
    )
