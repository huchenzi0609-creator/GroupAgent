"""Budgeted media services composed from scoped storage and an isolated worker."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

from groupagent.artifacts import ArtifactScope, ArtifactStore, ValidationRecord
from groupagent.domain import DomainError
from groupagent.tools import ToolContext

from .isolation import IsolatedMediaWorker
from .models import (
    AudioInfo,
    Capability,
    CriticalUncertainty,
    ImageInfo,
    MediaRef,
    MediaSource,
    OCRPage,
    OCRResult,
    ProviderResponse,
    ProviderRoute,
    Transcript,
    VisionImageBinding,
    VisionResult,
)
from .providers import CapabilityRegistry, MediaHTTPProvider

SourceGuard = Callable[[ToolContext, tuple[str, ...]], Awaitable[None]]


class MediaService:
    def __init__(
        self,
        store: ArtifactStore,
        worker: IsolatedMediaWorker,
        *,
        providers: dict[str, MediaHTTPProvider] | None = None,
        budget: Any | None = None,
        source_guard: SourceGuard | None = None,
        max_image_bytes: int = 15 * 1024 * 1024,
        max_image_pixels: int = 40_000_000,
        max_images: int = 10,
    ):
        if (
            type(max_image_bytes) is not int
            or not 0 < max_image_bytes <= 15 * 1024 * 1024
            or type(max_image_pixels) is not int
            or not 0 < max_image_pixels <= 40_000_000
            or type(max_images) is not int
            or not 0 < max_images <= 10
        ):
            raise ValueError("Media image limits must be within 15 MiB, 40 MP and ten images")
        self.store, self.worker = store, worker
        self.providers, self.budget, self.source_guard = providers or {}, budget, source_guard
        self.max_image_bytes, self.max_image_pixels, self.max_images = (
            max_image_bytes,
            max_image_pixels,
            max_images,
        )
        self.registry = CapabilityRegistry()

    def _image_parameters(self) -> dict[str, int]:
        return {"max_bytes": self.max_image_bytes, "max_pixels": self.max_image_pixels}

    def _image_bytes(self, content: bytes) -> None:
        if not content or len(content) > self.max_image_bytes:
            raise DomainError("image_size_limit", "Image exceeds its configured byte limit")

    @staticmethod
    def scope(context: ToolContext) -> ArtifactScope:
        if context.group_id != context.actor.group_id or context.cancelled.is_set():
            raise DomainError("cancelled", "Media context is cancelled or invalid")
        return ArtifactScope(context.group_id, context.work_id, context.actor.member_id)

    async def _check(self, context: ToolContext, sources: tuple[MediaSource, ...]) -> ArtifactScope:
        scope = self.scope(context)
        for source in sources:
            if source.group_id != context.group_id or source.work_id != context.work_id:
                raise DomainError("scope_violation", "Media belongs to another group or work")
            if source.declared_kind == "unsupported_video":
                raise DomainError(
                    "unsupported_video", "Video never starts a media worker or provider request"
                )
            if not source.available:
                raise DomainError(
                    "media_unavailable", "The source attachment is unavailable, expired or recalled"
                )
        if self.source_guard:
            await self.source_guard(context, tuple(source.source_event_id for source in sources))
        return scope

    def load_ref(
        self, context: ToolContext, file_id: str, version_id: str | None = None
    ) -> MediaRef:
        version = self.store.get(self.scope(context), file_id, version_id)
        source_fields = {
            key: version.provenance[key]
            for key in MediaSource.model_fields
            if key in version.provenance
        }
        try:
            source = MediaSource.model_validate(source_fields)
        except ValueError:
            raise DomainError(
                "media_provenance_missing", "Artifact has no verified media source association"
            ) from None
        metadata = version.provenance.get("media_metadata", {})
        return MediaRef(
            file_id=file_id,
            version_id=version.version_id,
            sha256=version.sha256,
            format=version.format,
            source=source,
            width=metadata.get("width"),
            height=metadata.get("height"),
            duration_seconds=metadata.get("duration_seconds"),
            static_first_frame=metadata.get("static_first_frame", False),
            budget_receipts=tuple(metadata.get("budget_receipts", ())),
        )

    async def _save(
        self,
        context: ToolContext,
        source: MediaSource,
        name: str,
        content: bytes,
        metadata: dict[str, Any],
        checks: tuple[str, ...],
        *,
        limitations: tuple[str, ...] = (),
        research_provenance: dict[str, Any] | None = None,
    ) -> MediaRef:
        scope = await self._check(context, (source,))
        version = self.store.create(
            scope,
            name,
            content,
            provenance={
                **source.model_dump(mode="json"),
                "media_metadata": metadata,
                **(
                    {"public_image": research_provenance} if research_provenance is not None else {}
                ),
            },
        )
        self.store.record_validation(
            scope,
            version.file_id,
            version.version_id,
            ValidationRecord(
                file_id=version.file_id,
                version_id=version.version_id,
                sha256=version.sha256,
                format=version.format,
                structural_passed=True,
                content_passed=True,
                media_decoded=True,
                checks=checks,
                limitations=limitations,
            ),
        )
        return self.load_ref(context, version.file_id, version.version_id)

    async def ingest_image(
        self,
        context: ToolContext,
        source: MediaSource,
        content: bytes,
        *,
        research_provenance: dict[str, Any] | None = None,
    ) -> MediaRef:
        await self._check(context, (source,))
        if research_provenance is not None and source.origin_kind != "research":
            raise DomainError(
                "media_provenance_invalid", "Public-image provenance requires a research source"
            )
        if source.declared_kind not in {"image", "file"}:
            raise DomainError("unsupported_image", "Source is not an image attachment")
        self._image_bytes(content)
        result, _ = await self.worker.run(
            context,
            "image_validate",
            (content,),
            parameters=self._image_parameters(),
            outputs=("view.png",),
        )
        info = ImageInfo.model_validate(result)
        if info.source_sha256 != hashlib.sha256(content).hexdigest():
            raise DomainError("worker_integrity_failed", "Decoded source hash differs")
        if research_provenance is not None:
            if research_provenance.get("content_type") != info.mime_type:
                raise DomainError(
                    "public_image_content_type", "Public image MIME differs from its decoded format"
                )
            # Host-built metadata stays namespaced and cannot replace source,
            # scope, decoded dimensions, or the actual stored byte hash.
            research_provenance = {
                key: research_provenance[key]
                for key in (
                    "requested_url",
                    "page_url",
                    "page_final_url",
                    "page_sha256",
                    "page_retrieved_at",
                    "retrieved_at",
                    "discovery",
                )
                if key in research_provenance
            } | {
                "final_url": source.source_url,
                "content_type": info.mime_type,
                "sha256": info.source_sha256,
                "width": info.width,
                "height": info.height,
                "static_first_frame": info.static_first_frame,
            }
        return await self._save(
            context,
            source,
            "image-" + info.source_sha256[:16] + "." + info.format,
            content,
            info.model_dump(mode="json"),
            ("sha256", "full_image_decode", "pixel_limit", "source_association"),
            limitations=("GIF analysis is a static first-frame view",)
            if info.static_first_frame
            else (),
            research_provenance=research_provenance,
        )

    async def ingest_audio(
        self, context: ToolContext, source: MediaSource, content: bytes
    ) -> MediaRef:
        await self._check(context, (source,))
        if source.declared_kind not in {"audio", "file"}:
            raise DomainError("unsupported_audio", "Source is not an audio attachment")
        if len(content) > 25 * 1024 * 1024:
            raise DomainError("audio_size_limit", "Audio exceeds 25 MiB")
        result, _ = await self.worker.run(
            context,
            "audio_validate",
            (content,),
            outputs=("normalized.wav", *(f"chunk-{i}.wav" for i in range(6))),
        )
        info = AudioInfo.model_validate(
            {key: result[key] for key in AudioInfo.model_fields if key in result}
        )
        if info.sha256 != hashlib.sha256(content).hexdigest():
            raise DomainError("worker_integrity_failed", "Decoded source hash differs")
        return await self._save(
            context,
            source,
            "audio-" + info.sha256[:16] + "." + info.format,
            content,
            info.model_dump(mode="json"),
            (
                "sha256",
                "audio_only_stream",
                "full_audio_decode",
                "duration_limit",
                "source_association",
            ),
        )

    async def _content(self, context: ToolContext, reference: MediaRef) -> bytes:
        scope = await self._check(context, (reference.source,))
        actual = self.load_ref(context, reference.file_id, reference.version_id)
        if actual != reference:
            raise DomainError(
                "media_reference_mismatch",
                "Media reference differs from its immutable source metadata",
            )
        return self.store.read(scope, reference.file_id, reference.version_id)

    async def ocr(
        self,
        context: ToolContext,
        reference: MediaRef,
        *,
        languages: str = "eng+chi_sim",
        route: ProviderRoute | None = None,
        force_ocr: bool = False,
    ) -> OCRResult:
        content = await self._content(context, reference)
        operation = "ocr_pdf" if reference.format == "pdf" else "ocr_image"
        if reference.format not in {"pdf", "png", "jpeg", "jpg", "webp", "gif"}:
            raise DomainError("unsupported_ocr_format", "OCR requires a PDF or supported image")
        if reference.format != "pdf":
            self._image_bytes(content)
        if route is not None:
            if route.capability != Capability.OCR:
                raise DomainError(
                    "provider_route_mismatch", "Remote OCR needs its independent OCR route"
                )
            cloud_content = content
            if reference.format == "pdf":
                inspection, _ = await self.worker.run(
                    context, "document_inspect", (content,), parameters={"format": "pdf"}
                )
                page_count = inspection["unit_count"]
                filename = "document.pdf"
            else:
                _, views = await self.worker.run(
                    context,
                    "image_validate",
                    (content,),
                    parameters=self._image_parameters(),
                    outputs=("view.png",),
                )
                cloud_content, page_count, filename = views[0], 1, "image.png"
            response = await self._paid(
                context,
                route,
                "ocr",
                {"pages": Decimal(page_count)},
                lambda provider: provider.ocr(route, cloud_content, filename),
            )
            await self._check(context, (reference.source,))
            return OCRResult(
                pages=response.pages,
                source_sha256=reference.sha256,
                engine="deepseek_ocr",
                source=reference.source,
                input_sha256=hashlib.sha256(cloud_content).hexdigest(),
                static_first_frame=reference.static_first_frame,
                budget_receipts=(response.budget_reservation_id,)
                if response.budget_reservation_id
                else (),
            )
        result, _ = await self.worker.run(
            context,
            operation,
            (content,),
            parameters={
                "languages": languages,
                "force_ocr": force_ocr,
                **(self._image_parameters() if operation == "ocr_image" else {}),
            },
        )
        await self._check(context, (reference.source,))
        return OCRResult(
            pages=tuple(OCRPage.model_validate(page) for page in result["pages"]),
            source_sha256=reference.sha256,
            engine="tesseract" if operation == "ocr_image" else "pypdf+tesseract",
            source=reference.source,
            static_first_frame=reference.static_first_frame,
        )

    async def _paid(
        self,
        context: ToolContext,
        route: ProviderRoute,
        suffix: str,
        estimates: dict[str, Decimal],
        call: Callable[[MediaHTTPProvider], Awaitable[ProviderResponse]],
    ) -> ProviderResponse:
        self.registry.resolve(route)
        provider = self.providers.get(route.provider)
        if provider is None or self.budget is None:
            raise DomainError(
                "provider_or_budget_unconfigured",
                "Media route, key and explicit pricing budget are required",
            )
        if getattr(self.budget, "fixture_only", False) and not provider.fixture_transport:
            raise DomainError(
                "fixture_budget_forbidden",
                "A fixture budget cannot authorize a real provider transport",
            )
        self.scope(context)
        reservation = await asyncio.to_thread(
            self.budget.reserve,
            context.actor,
            context.idempotency_key + ":" + suffix,
            "media." + route.capability.value,
            route.price_id,
            estimates,
            work_id=context.work_id,
        )
        if not reservation.is_new or not await asyncio.to_thread(
            self.budget.begin, context.actor, reservation.id
        ):
            raise DomainError(
                "provider_reconciliation_required",
                "This paid call already has a durable reservation",
            )
        try:
            if context.cancelled.is_set():
                raise DomainError("cancelled", "Media work was cancelled")
            response = await call(provider)
            if context.cancelled.is_set():
                raise DomainError("cancelled", "Media work was cancelled")
            usage = {key: Decimal(str(value)) for key, value in response.usage.items()}
            if not usage or any(not value.is_finite() or value < 0 for value in usage.values()):
                raise DomainError(
                    "provider_usage_missing", "Provider usage cannot be safely settled"
                )
            settled = await asyncio.to_thread(
                self.budget.settle,
                context.actor,
                reservation.id,
                usage,
                route.provider + ":" + response.request_id,
            )
            if settled.overrun:
                raise DomainError(
                    "budget_overrun", "Actual provider usage exceeded its reservation"
                )
            return response.model_copy(update={"budget_reservation_id": reservation.id})
        except BaseException as error:
            await asyncio.to_thread(
                self.budget.unknown, context.actor, reservation.id, "media_call_incomplete"
            )
            if isinstance(error, DomainError) and error.resource_ref is None:
                error.resource_ref = reservation.id
            raise

    async def analyze_images(
        self,
        context: ToolContext,
        references: tuple[MediaRef, ...],
        prompt: str,
        route: ProviderRoute,
    ) -> VisionResult:
        if (
            route.capability != Capability.VISION
            or not 1 <= len(references) <= self.max_images
            or not prompt.strip()
            or len(prompt) > 8000
        ):
            raise DomainError(
                "invalid_media_request", "Vision prompt or image count exceeds its configured limit"
            )
        views, labels, hashes, sources, static, bindings = [], [], [], [], [], []
        for index, reference in enumerate(references):
            content = await self._content(context, reference)
            self._image_bytes(content)
            source_sha256 = hashlib.sha256(content).hexdigest()
            if source_sha256 != reference.sha256:
                raise DomainError(
                    "media_reference_mismatch", "Image bytes differ from their reference"
                )
            result, outputs = await self.worker.run(
                context,
                "image_validate",
                (content,),
                parameters=self._image_parameters(),
                outputs=("view.png",),
            )
            info = ImageInfo.model_validate(result)
            if len(outputs) != 1:
                raise DomainError(
                    "worker_integrity_failed", "Image validation returned no unique view"
                )
            view_sha256 = hashlib.sha256(outputs[0]).hexdigest()
            if (
                info.source_sha256 != source_sha256
                or info.byte_length != len(content)
                or info.preview_sha256 != view_sha256
            ):
                raise DomainError(
                    "worker_integrity_failed", "Image validation hashes differ from actual bytes"
                )
            views.append(outputs[0])
            hashes.append(view_sha256)
            sources.append(reference.source)
            static.append(info.static_first_frame)
            bindings.append(
                VisionImageBinding(
                    image_index=index,
                    file_id=reference.file_id,
                    version_id=reference.version_id,
                    source=reference.source,
                    source_sha256=source_sha256,
                    normalized_view_sha256=view_sha256,
                    normalized_pixel_sha256=info.pixel_sha256,
                    width=info.width,
                    height=info.height,
                    static_first_frame=info.static_first_frame,
                    alpha_composited_on_white=info.alpha_composited_on_white,
                )
            )
            labels.append(
                f"Image {index + 1}; source_message={reference.source.source_message_id}; author={reference.source.member_id}; "
                f"quoted_message={reference.source.quote_message_id}; view={'static GIF first frame' if info.static_first_frame else 'validated pixels'}"
            )
        response = await self._paid(
            context,
            route,
            "vision",
            {
                "input_tokens": Decimal(len(prompt) * 2 + 4096 * len(views)),
                "output_tokens": Decimal(2048),
            },
            lambda provider: provider.vision(route, prompt, tuple(views), tuple(labels)),
        )
        await self._check(context, tuple(sources))
        return VisionResult(
            text=response.text,
            sources=tuple(sources),
            image_sha256=tuple(hashes),
            static_views=tuple(static),
            provider=route.provider,
            model=route.model,
            request_id=response.request_id,
            image_bindings=tuple(bindings),
            budget_receipts=(response.budget_reservation_id,)
            if response.budget_reservation_id
            else (),
        )

    async def transcribe(
        self,
        context: ToolContext,
        reference: MediaRef,
        route: ProviderRoute,
        *,
        critical_terms: tuple[str, ...] = (),
    ) -> Transcript:
        if route.capability != Capability.ASR:
            raise DomainError(
                "provider_route_mismatch", "ASR needs an independently configured route"
            )
        content = await self._content(context, reference)
        result, outputs = await self.worker.run(
            context,
            "audio_validate",
            (content,),
            outputs=("normalized.wav", *(f"chunk-{i}.wav" for i in range(6))),
        )
        info = AudioInfo.model_validate(
            {key: result[key] for key in AudioInfo.model_fields if key in result}
        )
        texts, ids, confidences, budget_receipts = [], [], [], []
        remaining = info.duration_seconds
        for index, output_index in enumerate(result["chunk_indices"]):
            duration = min(110, remaining)
            chunk = outputs[output_index]

            async def transcribe_chunk(
                provider: MediaHTTPProvider, data: bytes = chunk, seconds: float = duration
            ) -> ProviderResponse:
                return await provider.transcribe(route, data, seconds)

            response = await self._paid(
                context,
                route,
                f"asr-{index}",
                {"audio_seconds": Decimal(math.ceil(duration))},
                transcribe_chunk,
            )
            texts.append(response.text)
            ids.append(response.request_id)
            if response.budget_reservation_id:
                budget_receipts.append(response.budget_reservation_id)
            confidences.append(response.confidence)
            remaining -= duration
        await self._check(context, (reference.source,))
        text = "\n".join(texts)
        if not text.strip():
            raise DomainError(
                "transcription_empty",
                "No recognizable speech was returned; ask for a clearer source",
            )
        confidence = (
            min(value for value in confidences if value is not None)
            if confidences and all(value is not None for value in confidences)
            else None
        )
        uncertainties = critical_uncertainties(text, confidence, critical_terms)
        return Transcript(
            text=text,
            source=reference.source,
            duration_seconds=info.duration_seconds,
            confidence=confidence,
            uncertainty=uncertainties,
            needs_clarification=bool(uncertainties),
            provider=route.provider,
            model=route.model,
            request_ids=tuple(ids),
            budget_receipts=tuple(budget_receipts),
        )

    async def synthesize(
        self,
        context: ToolContext,
        text: str,
        route: ProviderRoute,
        source: MediaSource,
        *,
        explicitly_requested: bool = False,
        voice: str = "stella",
    ) -> MediaRef:
        await self._check(context, (source,))
        if explicitly_requested is not True:
            raise DomainError(
                "voice_not_requested",
                "Ordinary answers stay text unless speech is explicitly requested",
            )
        if route.capability != Capability.TTS:
            raise DomainError("provider_route_mismatch", "Speech synthesis needs its own TTS route")
        if not text.strip() or len(text) > 400 or voice not in {"stella", "tide"}:
            raise DomainError(
                "invalid_media_request", "Short speech requires bounded text and a preset voice"
            )
        response = await self._paid(
            context,
            route,
            "tts",
            {"characters": Decimal(len(text) * 2)},
            lambda provider: provider.synthesize(route, text, voice=voice),
        )
        result, outputs = await self.worker.run(
            context, "tts_wrap", (response.content,), outputs=("speech.wav",)
        )
        info = AudioInfo.model_validate(result)
        return await self._save(
            context,
            source,
            "speech-" + info.sha256[:16] + ".wav",
            outputs[0],
            {
                **info.model_dump(mode="json"),
                "budget_receipts": [response.budget_reservation_id],
                "provider_request_id": response.request_id,
            },
            (
                "complete_http_response",
                "pcm_sample_alignment",
                "full_wav_decode",
                "short_voice_limit",
            ),
            limitations=(
                "Preset synthetic voice, not a cloned speaker",
                "Structural audio checks do not certify every spoken word",
            ),
        )

    async def generate_image(
        self, context: ToolContext, prompt: str, route: ProviderRoute, source: MediaSource
    ) -> MediaRef:
        await self._check(context, (source,))
        if route.capability != Capability.IMAGE_GENERATION:
            raise DomainError(
                "provider_route_mismatch", "Image generation is separate from vision analysis"
            )
        if not prompt.strip() or len(prompt) > 8000:
            raise DomainError("invalid_media_request", "A bounded image prompt is required")
        response = await self._paid(
            context,
            route,
            "image-generation",
            {"images": Decimal(1)},
            lambda provider: provider.generate_image(route, prompt),
        )
        self._image_bytes(response.content)
        result, _ = await self.worker.run(
            context,
            "image_validate",
            (response.content,),
            parameters=self._image_parameters(),
            outputs=("view.png",),
        )
        info = ImageInfo.model_validate(result)
        if info.format != "png" or (info.width, info.height) != (1024, 1024):
            raise DomainError(
                "invalid_provider_image",
                "Generated image differs from the requested format/dimensions",
            )
        return await self._save(
            context,
            source,
            "generated-" + info.source_sha256[:16] + ".png",
            response.content,
            {
                **info.model_dump(mode="json"),
                "budget_receipts": [response.budget_reservation_id],
                "provider_request_id": response.request_id,
            },
            ("provider_image_output", "full_image_decode", "expected_dimensions", "sha256"),
            limitations=(
                "Image meaning and visual quality require representative semantic acceptance",
            ),
        )


def critical_uncertainties(
    text: str, confidence: float | None, terms: tuple[str, ...]
) -> tuple[CriticalUncertainty, ...]:
    if confidence is not None and confidence >= 0.85:
        return ()
    reason = "confidence_unavailable" if confidence is None else "low_confidence"
    fields = []
    for term in terms:
        if term and term in text:
            fields.append(("name", term))
    fields.extend(
        ("number", match.group())
        for match in re.finditer(
            r"\d+(?:\.\d+)?|[一二三四五六七八九十百千万]+(?:元|人|次|个|天|份|页|节|章|小时|分钟|秒|周|月)",
            text,
        )
    )
    fields.extend(
        ("date_or_time", match.group())
        for match in re.finditer(
            r"(?:今天|明天|明晚|后天|今晚|下周[一二三四五六日天]?|周[一二三四五六日天]|\d{1,2}[月日号点时]|[一二三四五六七八九十]{1,3}点)",
            text,
        )
    )
    return tuple(
        CriticalUncertainty.model_validate({"field": field, "text": value, "reason": reason})
        for field, value in dict.fromkeys(fields)
    )
