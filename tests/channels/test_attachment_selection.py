"""Synthetic SDK media contracts through the real adapter and input downloader."""

from __future__ import annotations

import hashlib

import httpx
import pytest

from groupagent.channels.models import AttachmentHandle
from groupagent.domain import DomainError
from groupagent.runtime.inputs import InputProcessor

from .conftest import asset, message


def processor(tmp_path, adapter):
    return InputProcessor(
        None, None, None, {}, {}, None, adapter, tmp_path / "inputs", lambda _: None
    )


def image_copies(daemon, *, duplicate_standard=False, pending_original=False):
    """Distinct checked standard and preview copies of one image message."""
    payloads = {
        "asset-standard": b"synthetic-standard-image",
        "asset-thumbnail": b"synthetic-thumbnail-image",
    }
    daemon.messages["local-1"] = message(kind=3, body=None)
    daemon.assets["local-1"] = [
        asset(
            id=identifier,
            variant=identifier.removeprefix("asset-"),
            actualVariant=identifier.removeprefix("asset-"),
            content=content,
        )
        for identifier, content in payloads.items()
    ]
    if pending_original:
        daemon.assets["local-1"].append(
            asset(
                id="asset-original",
                variant="original",
                actualVariant=None,
                state="pending",
                contentType=None,
                byteLength=None,
                sha256=None,
                advertisedByteLength=1024,
            )
        )
    if duplicate_standard:
        daemon.assets["local-1"].append(
            asset(id="asset-other-standard", variant="standard", content=b"a different image")
        )

    def content_response(request):
        if request.url.path.endswith("/content"):
            identifier = request.url.path.rsplit("/", 2)[1]
            assert identifier in payloads
            return httpx.Response(200, content=payloads[identifier])
        return None

    daemon.failure = content_response
    return payloads


@pytest.mark.parametrize("reverse,pending_original", [(False, False), (True, False), (False, True)])
async def test_selected_single_image_resolves_best_cached_copy_before_media_processing(
    tmp_path, adapter, daemon, reverse, pending_original
):
    payloads = image_copies(daemon, pending_original=pending_original)
    if reverse:
        daemon.assets["local-1"].reverse()
    event = await adapter.read_message("local-1")
    intake = event.model_dump_json()
    value = processor(tmp_path, adapter)
    reference = value._handles(event)[0]
    assert reference.asset_id is None and reference.variant == "best"

    result = await value._download(event, reference)

    assert result.handle.asset_id == "asset-standard"
    assert result.handle.variant == "standard"
    assert result.content == payloads["asset-standard"]
    assert result.sha256 == hashlib.sha256(result.content).hexdigest()
    assert await adapter.fetch_attachment(reference) == result
    assert event.model_dump_json() == intake
    paths = [
        request.url.path for request in daemon.requests if request.url.path.endswith("/content")
    ]
    assert len(paths) == 2 and all(path.endswith("/media/asset-standard/content") for path in paths)
    assert not any(request.method == "POST" for request in daemon.requests)


@pytest.mark.parametrize("via_inputs", [False, True])
async def test_multiple_best_rank_assets_remain_ambiguous(tmp_path, adapter, daemon, via_inputs):
    image_copies(daemon, duplicate_standard=True)
    event = await adapter.read_message("local-1")
    reference = AttachmentHandle.model_validate(event.attachments[0]["handle"])

    with pytest.raises(DomainError) as error:
        if via_inputs:
            await processor(tmp_path, adapter)._download(event, reference)
        else:
            await adapter.fetch_attachment(reference)

    assert error.value.code == "ambiguous_attachment"
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)
    assert not any(request.method == "POST" for request in daemon.requests)


@pytest.mark.parametrize("via_inputs", [False, True])
async def test_concrete_variant_never_substitutes_a_higher_rank_copy(
    tmp_path, adapter, daemon, via_inputs
):
    payloads = image_copies(daemon)
    event = await adapter.read_message("local-1")
    reference = AttachmentHandle.model_validate(event.attachments[0]["handle"]).model_copy(
        update={"variant": "thumbnail"}
    )
    result = (
        await processor(tmp_path, adapter)._download(event, reference)
        if via_inputs
        else await adapter.fetch_attachment(reference)
    )
    assert result.handle.asset_id == "asset-thumbnail"
    assert result.handle.variant == "thumbnail"
    assert result.content == payloads["asset-thumbnail"]


async def test_input_resolution_preserves_an_issued_hash(tmp_path, adapter, daemon):
    image_copies(daemon)
    event = await adapter.read_message("local-1")
    reference = next(
        value
        for value in await adapter.attachment_handles("local-1")
        if value.variant == "standard"
    )
    next(value for value in daemon.assets["local-1"] if value["variant"] == "standard")[
        "sha256"
    ] = "a" * 64

    with pytest.raises(DomainError) as error:
        await processor(tmp_path, adapter)._download(event, reference)

    assert error.value.code == "attachment_version_conflict"
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)


async def test_asset_id_cannot_be_rebound_to_another_variant(adapter, daemon):
    image_copies(daemon)
    reference = next(
        value
        for value in await adapter.attachment_handles("local-1")
        if value.variant == "standard"
    )
    next(value for value in daemon.assets["local-1"] if value["variant"] == "standard")[
        "variant"
    ] = "thumbnail"

    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(reference)

    assert error.value.code == "scope_denied"
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)
