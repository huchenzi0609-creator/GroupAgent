"""Configured caps remain effective for cached input and real preprocessing composition."""

from __future__ import annotations

import pytest

from groupagent.artifacts.isolated import IsolatedDocumentService
from groupagent.domain import DomainError
from groupagent.media import MediaService

from .test_inputs import attached, fixture_setup, scoped_work

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("kind", ["image", "file"])
async def test_lowered_per_file_caps_recheck_cached_inputs_before_worker(runtime_env, kind):
    env = runtime_env
    setup = fixture_setup(env)
    selected = attached(env, kind=kind)
    work, lease, context = scoped_work(env, [selected])
    documents = IsolatedDocumentService(env.artifacts, setup.worker)
    media = MediaService(env.artifacts, setup.worker)
    try:
        imported = await setup.processor.import_for_work(work, lease, context, documents, media)
        _, content = env.registry.read(env.actor, imported[0]["version_id"])
        field = "max_image_bytes" if kind == "image" else "max_document_bytes"
        setattr(setup.processor, field, len(content) - 1)
        setup.worker.calls.clear()
        with pytest.raises(DomainError) as error:
            await setup.processor.import_for_work(work, lease, context, documents, media)
        assert error.value.code == "input_limit" and not setup.worker.calls and not setup.calls
    finally:
        await setup.http.aclose()


async def test_lowered_file_count_rejects_existing_multi_input_selection(runtime_env):
    env = runtime_env
    setup = fixture_setup(env)
    selected = [attached(env, kind="file") for _ in range(2)]
    work, lease, context = scoped_work(env, selected)
    documents = IsolatedDocumentService(env.artifacts, setup.worker)
    media = MediaService(env.artifacts, setup.worker)
    try:
        assert (
            len(await setup.processor.import_for_work(work, lease, context, documents, media)) == 2
        )
        setup.processor.max_input_files = 1
        setup.worker.calls.clear()
        with pytest.raises(DomainError) as error:
            await setup.processor.import_for_work(work, lease, context, documents, media)
        assert error.value.code == "invalid_input" and not setup.worker.calls
    finally:
        await setup.http.aclose()


async def test_audio_preprocessing_passes_image_caps_to_its_media_service(runtime_env, monkeypatch):
    env = runtime_env
    setup = fixture_setup(env)
    setup.processor.max_image_bytes = 1024
    setup.processor.max_image_pixels = 100
    setup.processor.max_images = 2
    supplied = []

    def service(*args, **kwargs):
        supplied.append(kwargs)
        return MediaService(*args, **kwargs)

    monkeypatch.setattr("groupagent.runtime.inputs.MediaService", service)
    try:
        event = attached(env)
        result = await setup.processor.preprocess_audio(event)
        assert result.reply_to == event.id
        assert supplied[0]["max_image_bytes"] == 1024
        assert supplied[0]["max_image_pixels"] == 100 and supplied[0]["max_images"] == 2
    finally:
        await setup.http.aclose()
