"""Media tools use stored scoped references, never host paths, credentials or roles."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import Field

from groupagent.domain import DomainError
from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure
from groupagent.vision_policy import FACE_ID_RESPONSE, requests_face_identification

from .models import (
    Capability,
    MediaRef,
    MediaSource,
    OCRResult,
    ProviderRoute,
    Transcript,
    VisionResult,
)
from .service import MediaService


class FileReference(ToolData):
    file_id: str = Field(min_length=1)
    version_id: str | None = None


class VisionInput(ToolData):
    images: tuple[FileReference, ...] = Field(min_length=1, max_length=10)
    prompt: str = Field(min_length=1, max_length=8000)
    route_id: str


class OCRInput(FileReference):
    languages: str = "eng+chi_sim"
    include_word_boxes: bool = False
    force_ocr: bool = False


class CloudOCRInput(OCRInput):
    route_id: str


class ASRInput(FileReference):
    route_id: str


class SpeechInput(ToolData):
    text: str = Field(min_length=1, max_length=400)
    route_id: str
    voice: str = "stella"


class ImageGenerationInput(ToolData):
    prompt: str = Field(min_length=1, max_length=8000)
    route_id: str


OutputSource = Callable[[ToolContext, str], Awaitable[MediaSource]]
SpeechPermission = Callable[[ToolContext], Awaitable[bool]]
CriticalTerms = Callable[[ToolContext], Awaitable[tuple[str, ...]]]


def register_media_tools(
    gateway: ToolGateway,
    service: MediaService,
    routes: dict[str, ProviderRoute],
    *,
    output_source: OutputSource | None = None,
    speech_permission: SpeechPermission | None = None,
    critical_terms: CriticalTerms | None = None,
) -> None:
    for route in routes.values():
        service.registry.resolve(route)

    def configured(identifier: str, capability: Capability) -> ProviderRoute:
        route = routes.get(identifier)
        if route is None or route.capability != capability:
            raise ToolFailure(
                "provider_route_unconfigured",
                "Field route_id must select a configured media_routes entry with the required capability. No worker, provider or budget operation was started.",
                no_effect=True,
            )
        return route

    async def vision(arguments: VisionInput, context: ToolContext) -> VisionResult:
        if requests_face_identification(arguments.prompt, image_context=True):
            raise ToolFailure("face_identification_unavailable", FACE_ID_RESPONSE, no_effect=True)
        route = configured(arguments.route_id, Capability.VISION)
        references = tuple(
            service.load_ref(context, item.file_id, item.version_id) for item in arguments.images
        )
        return await service.analyze_images(context, references, arguments.prompt, route)

    async def ocr(arguments: OCRInput, context: ToolContext) -> OCRResult:
        reference = service.load_ref(context, arguments.file_id, arguments.version_id)
        result = await service.ocr(
            context,
            reference,
            languages=arguments.languages,
            force_ocr=arguments.force_ocr,
        )
        if not arguments.include_word_boxes:
            result = result.model_copy(
                update={
                    "pages": tuple(page.model_copy(update={"words": ()}) for page in result.pages)
                }
            )
        return result

    async def cloud_ocr(arguments: CloudOCRInput, context: ToolContext) -> OCRResult:
        route = configured(arguments.route_id, Capability.OCR)
        return await service.ocr(
            context,
            service.load_ref(context, arguments.file_id, arguments.version_id),
            route=route,
        )

    async def asr(arguments: ASRInput, context: ToolContext) -> Transcript:
        route = configured(arguments.route_id, Capability.ASR)
        terms = await critical_terms(context) if critical_terms else ()
        return await service.transcribe(
            context,
            service.load_ref(context, arguments.file_id, arguments.version_id),
            route,
            critical_terms=terms,
        )

    async def tts(arguments: SpeechInput, context: ToolContext) -> MediaRef:
        route = configured(arguments.route_id, Capability.TTS)
        try:
            if (
                output_source is None
                or speech_permission is None
                or not await speech_permission(context)
            ):
                raise ToolFailure(
                    "voice_not_requested",
                    "Trusted request evidence has not authorized speech output",
                    no_effect=True,
                )
            source = await output_source(context, "audio")
        except DomainError as error:
            # Permission/provenance checks precede synthesize and cannot have
            # called a provider, reserved its charge or launched a media worker.
            raise ToolFailure(error.code, error.message, no_effect=True) from error
        return await service.synthesize(
            context,
            arguments.text,
            route,
            source,
            explicitly_requested=True,
            voice=arguments.voice,
        )

    async def image_generate(arguments: ImageGenerationInput, context: ToolContext) -> MediaRef:
        route = configured(arguments.route_id, Capability.IMAGE_GENERATION)
        if output_source is None:
            raise DomainError(
                "media_provenance_missing", "Generated output needs trusted request provenance"
            )
        source = await output_source(context, "image")
        return await service.generate_image(
            context,
            arguments.prompt,
            route,
            source,
        )

    definitions = (
        (
            "image_analyze",
            "Analyze actual validated pixels in the given ordered scoped images. GIF is explicitly a static first-frame view.",
            VisionInput,
            VisionResult,
            vision,
            "read_only",
        ),
        (
            "media_ocr",
            "Read image/PDF text with page citations using isolated local OCR. Word boxes include PDF points when applicable.",
            OCRInput,
            OCRResult,
            ocr,
            "read_only",
        ),
        (
            "provider_ocr",
            "Use separately configured and budgeted cloud OCR, with per-page completion checks.",
            CloudOCRInput,
            OCRResult,
            cloud_ocr,
            "read_only",
        ),
        (
            "audio_transcribe",
            "Transcribe scoped audio using an independent ASR route. Critical fields with unavailable or low confidence require clarification before dependent writes.",
            ASRInput,
            Transcript,
            asr,
            "read_only",
        ),
        (
            "speech_synthesize",
            "Produce a short validated synthetic voice artifact only when trusted request evidence authorizes voice output.",
            SpeechInput,
            MediaRef,
            tts,
            "local_draft",
        ),
        (
            "image_generate",
            "Generate an image through a separately configured image-generation provider; vision analysis is not an image generator.",
            ImageGenerationInput,
            MediaRef,
            image_generate,
            "local_draft",
        ),
    )
    for name, description, input_model, output_model, handler, risk in definitions:
        if name == "image_generate" and not any(
            route.capability == Capability.IMAGE_GENERATION for route in routes.values()
        ):
            continue
        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=input_model,
                output_model=output_model,
                handler=handler,
                risk_class=risk,
                resource_scopes=("group", "work", "artifacts"),
                timeout_seconds=180,
                max_input_bytes=64000,
                max_output_bytes=512000,
                max_artifact_bytes=25 * 1024 * 1024,
                cost_meter="reserved_media_provider_usage",
                idempotency_mode=IdempotencyMode.READ_ONLY
                if name == "media_ocr"
                else IdempotencyMode.RECONCILE_BEFORE_RETRY,
            )
        )
