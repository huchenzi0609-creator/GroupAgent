"""Input caps are enforced before download/import using synthetic byte fixtures."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from groupagent.channels.models import AttachmentContent, AttachmentHandle
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.runtime.inputs import InputProcessor


def processor(tmp_path, channel=None, **limits):
    return InputProcessor(
        None,
        None,
        None,
        {},
        {},
        None,
        channel or SimpleNamespace(),
        tmp_path / "inputs",
        lambda _: None,
        **limits,
    )


def handle(
    *,
    kind="image",
    name="image.png",
    size=None,
    advertised=None,
    availability="available",
    asset="asset",
):
    return AttachmentHandle(
        account_id="synthetic",
        group_id="group",
        external_group_id="group@chatroom",
        source_epoch="test",
        message_id="message",
        asset_id=asset,
        kind=kind,
        variant="original",
        availability=availability,
        filename=name,
        size_bytes=size,
        advertised_size_bytes=advertised,
    )


def event(*handles):
    stamp = datetime.now(UTC)
    return GroupEvent(
        id="event",
        source_key="source",
        group_id="group",
        actor_id="member",
        kind=EventKind.TEXT,
        occurred_at=stamp,
        received_at=stamp,
        attachments=[value.as_attachment() for value in handles],
    )


class DownloadProbe:
    def __init__(self, current, content=b"12345"):
        self.current, self.content = current, content
        self.metadata_calls = self.fetch_calls = self.download_calls = 0

    async def attachment_handles(self, message):
        self.metadata_calls += 1
        return (self.current,)

    async def fetch_attachment(self, value):
        self.fetch_calls += 1
        return AttachmentContent(
            handle=value, content=self.content, sha256=hashlib.sha256(self.content).hexdigest()
        )

    async def request_attachment_download(self, value):
        self.download_calls += 1


def test_constructor_defaults_remain_compatible(tmp_path):
    value = processor(tmp_path)
    assert value.max_input_files == 20 and value.max_image_bytes == 15 * 1024**2
    assert value.max_document_bytes == 30 * 1024**2 and value.max_image_pixels == 40_000_000


@pytest.mark.parametrize(
    "options",
    [
        {"max_input_files": 0},
        {"max_input_files": 21},
        {"max_image_bytes": True},
        {"max_image_bytes": 15 * 1024**2 + 1},
        {"max_document_bytes": 0},
        {"max_image_pixels": 40_000_001},
    ],
)
def test_invalid_input_cap_configuration_is_rejected(tmp_path, options):
    with pytest.raises(ValueError):
        processor(tmp_path, **options)


async def test_declared_oversize_is_rejected_before_any_download_lookup(tmp_path):
    reference = handle(size=5)
    channel = DownloadProbe(reference)
    value = processor(tmp_path, channel, max_image_bytes=4)
    with pytest.raises(DomainError) as error:
        await value._download(event(reference), reference)
    assert error.value.code == "input_limit"
    assert channel.metadata_calls == channel.fetch_calls == 0


@pytest.mark.parametrize("pending", [False, True])
async def test_fresh_metadata_is_checked_before_fetch_or_daemon_download(tmp_path, pending):
    reference = handle()
    fresh = handle(
        size=None if pending else 5,
        advertised=5 if pending else None,
        availability="pending" if pending else "available",
    )
    channel = DownloadProbe(fresh)
    value = processor(tmp_path, channel, max_image_bytes=4)
    with pytest.raises(DomainError) as error:
        await value._download(event(reference), reference)
    assert error.value.code == "input_limit" and channel.metadata_calls == 1
    assert channel.fetch_calls == channel.download_calls == 0


async def test_actual_returned_bytes_are_bounded_even_without_declared_size(tmp_path):
    reference = handle(kind="file", name="photo.PNG")
    channel = DownloadProbe(reference)
    value = processor(tmp_path, channel, max_image_bytes=4, max_document_bytes=8)
    with pytest.raises(DomainError) as error:
        await value._download(event(reference), reference)
    assert error.value.code == "input_limit" and channel.fetch_calls == 1


async def test_document_import_is_rejected_before_calling_a_decoder(tmp_path):
    value = processor(tmp_path, max_document_bytes=4)
    with pytest.raises(DomainError) as error:
        await value._import_bytes(
            None, None, None, SimpleNamespace(declared_kind="file"), "report.pdf", b"12345"
        )
    assert error.value.code == "input_limit"


def test_configured_file_count_applies_to_each_selected_event(tmp_path):
    first, second = handle(asset="first"), handle(asset="second")
    value = processor(tmp_path, max_input_files=1)
    assert value._handles(event(first)) == (first,)
    with pytest.raises(DomainError) as error:
        value._handles(event(first, second))
    assert error.value.code == "invalid_attachment"
