"""Trusted image format conversion and immutable public-source lineage.

Only the bounded worker performs decoding/encoding. Model-supplied workspace
files and URLs cannot create the host validation attestation used by this module.
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, Field

from groupagent.artifacts import ArtifactScope, ArtifactStore, ArtifactVersion, ValidationRecord
from groupagent.artifacts.models import canonical_artifact_format
from groupagent.domain import DomainError
from groupagent.media import MediaRef, MediaService, MediaSource
from groupagent.media.models import ImageInfo
from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway

from .urls import parse_public_url

ImageFormat = Literal["png", "jpeg", "webp", "gif"]
_IMAGE_FORMATS = {"png", "jpeg", "jpg", "webp", "gif"}
_DECODE_CHECKS = {"sha256", "full_image_decode", "pixel_limit", "source_association"}


class ImageConvertInput(ToolData):
    file_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    version_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    format: ImageFormat


class ConvertedImageRef(MediaRef):
    page_citation: str | None = None
    image_citation: str | None = None


class ImageTransformReceipt(ToolData):
    schema_version: Literal[1] = 1
    operation: Literal["format_convert", "verified_copy"]
    parent_group_id: str = Field(min_length=1)
    parent_work_id: str = Field(min_length=1)
    parent_created_by: str = Field(min_length=1)
    parent_file_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    parent_version_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    parent_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_pixel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_pixel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_format: ImageFormat
    width: int = Field(gt=0, le=40_000_000)
    height: int = Field(gt=0, le=40_000_000)
    source_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @property
    def validation_check(self) -> str:
        return (
            "trusted_image_conversion"
            if self.operation == "format_convert"
            else "trusted_image_copy"
        )

    @property
    def parent_scope(self) -> ArtifactScope:
        return ArtifactScope(self.parent_group_id, self.parent_work_id, self.parent_created_by)


class _PublicImageEvidence(ToolData):
    requested_url: str = Field(min_length=1, max_length=8192)
    final_url: str = Field(min_length=1, max_length=8192)
    page_url: str = Field(min_length=1, max_length=8192)
    page_final_url: str = Field(min_length=1, max_length=8192)
    page_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    page_retrieved_at: AwareDatetime
    retrieved_at: AwareDatetime
    discovery: Literal["img_src", "img_srcset", "open_graph", "twitter", "image_link"]
    content_type: str = Field(min_length=1, max_length=100)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    width: int = Field(gt=0, le=40_000_000)
    height: int = Field(gt=0, le=40_000_000)
    static_first_frame: bool


@dataclass(frozen=True)
class PublicImageLineage:
    source_sha256: str
    public_image: dict[str, Any]
    source: MediaSource
    versions: tuple[ArtifactVersion, ...]

    @property
    def page_citation(self) -> str:
        from .images import _access_citation

        return _access_citation(
            self.public_image["page_final_url"],
            datetime.fromisoformat(self.public_image["page_retrieved_at"].replace("Z", "+00:00")),
        )

    @property
    def image_citation(self) -> str:
        from .images import _access_citation

        return _access_citation(
            self.public_image["final_url"],
            datetime.fromisoformat(self.public_image["retrieved_at"].replace("Z", "+00:00")),
            image=True,
        )


class PublicImageValidationError(DomainError):
    """A delivery constraint failure with host-produced, repairable facts."""

    def __init__(self, details: dict[str, Any]) -> None:
        self.details = details
        counts = details["validated_distinct_count"]
        requested = details["required_count"]
        formats = details["missing_count_by_format"]
        suffix = "; ".join(f"{name}: {count} missing" for name, count in formats.items())
        super().__init__(
            "validation_failed",
            f"Public image delivery has {counts}/{requested} verified distinct sources"
            + (f"; {suffix}" if suffix else "")
            + ". Reuse validated imported images; use image_convert for a required format, then artifact_deliver.",
        )


def validate_public_image_delivery(
    store: ArtifactStore,
    scope: ArtifactScope,
    selected: list[ArtifactVersion],
    *,
    required_count: int,
    required_formats: set[str],
    reusable: list[ArtifactVersion] | None = None,
) -> None:
    """Count verified original sources, never encodings of the same source twice."""
    image_formats = {
        canonical_artifact_format(value) for value in required_formats & _IMAGE_FORMATS
    }
    identities: set[str] = set()
    by_format: dict[str, set[str]] = {name: set() for name in sorted(image_formats)}
    duplicates, invalid, available = [], [], []
    for version in selected:
        if version.format not in _IMAGE_FORMATS:
            continue
        try:
            lineage = resolve_public_image(store, scope, version)
            if lineage is None:
                invalid.append(
                    {
                        "file_id": version.file_id,
                        "version_id": version.version_id,
                        "reason": "public_source_missing",
                    }
                )
                continue
        except DomainError as error:
            invalid.append(
                {"file_id": version.file_id, "version_id": version.version_id, "reason": error.code}
            )
            continue
        if lineage.source_sha256 in identities:
            duplicates.append(version.version_id)
        identities.add(lineage.source_sha256)
        format_name = canonical_artifact_format(version.format)
        if format_name in by_format:
            by_format[format_name].add(lineage.source_sha256)
    missing = {
        name: required_count - len(values)
        for name, values in by_format.items()
        if len(values) < required_count
    }
    if len(identities) >= required_count and not missing:
        return
    for version in reusable or selected:
        if version.format not in _IMAGE_FORMATS:
            continue
        try:
            lineage = resolve_public_image(store, scope, version)
        except DomainError:
            continue
        if lineage:
            available.append(
                {
                    "file_id": version.file_id,
                    "version_id": version.version_id,
                    "format": version.format,
                    "source_sha256": lineage.source_sha256,
                }
            )
    raise PublicImageValidationError(
        {
            "constraint": "public_images",
            "required_count": required_count,
            "validated_distinct_count": len(identities),
            "required_image_formats": sorted(image_formats),
            "validated_distinct_count_by_format": {
                name: len(values) for name, values in by_format.items()
            },
            "missing_count_by_format": missing,
            "missing_formats": sorted(missing),
            "duplicate_version_ids": duplicates,
            "invalid_images": invalid,
            "reusable_images": available,
        }
    )


def _invalid(message: str) -> DomainError:
    return DomainError("public_image_evidence_invalid", message)


def _source(version: ArtifactVersion) -> MediaSource:
    try:
        source = MediaSource.model_validate(
            {
                key: version.provenance[key]
                for key in MediaSource.model_fields
                if key in version.provenance
            }
        )
    except ValueError:
        raise _invalid("Image has no valid immutable source association") from None
    if (source.group_id, source.work_id) != (
        version.group_id,
        version.work_id,
    ) or not source.available:
        raise _invalid("Image source is unavailable or belongs to another work")
    return source


def resolve_public_image(
    store: ArtifactStore,
    scope: ArtifactScope,
    version: ArtifactVersion,
    *,
    _seen: frozenset[str] = frozenset(),
) -> PublicImageLineage | None:
    """Verify exact bytes, worker attestations and all parents; return root identity.

    A direct conversion is confined to the same work and actor. Only an explicit
    host-validated input copy can cross that boundary, retaining the exact parent
    bytes and revocable source dependencies. No caller-provided URL is authority.
    """
    if version.format not in _IMAGE_FORMATS:
        return None
    if version.version_id in _seen or len(_seen) >= 16:
        raise _invalid("Image transform ancestry is cyclic or exceeds its bounded depth")
    if (version.group_id, version.work_id, version.created_by) != (
        scope.group_id,
        scope.work_id,
        scope.actor_id,
    ):
        raise _invalid("Image belongs to another group, work or producing actor")
    actual = store.get(scope, version.file_id, version.version_id)
    if actual != version:
        raise _invalid("Image metadata differs from its immutable stored version")
    payload = store.read(scope, version.file_id, version.version_id)
    if hashlib.sha256(payload).hexdigest() != version.sha256:
        raise _invalid("Image bytes differ from their immutable source hash")
    raw = version.provenance.get("public_image")
    transform_raw = version.provenance.get("image_transform")
    if raw is None and transform_raw is None:
        return None
    source = _source(version)
    try:
        info = ImageInfo.model_validate(version.provenance["media_metadata"])
        if (
            info.source_sha256 != version.sha256
            or info.byte_length != version.size_bytes
            or info.format != canonical_artifact_format(version.format)
            or not 0 < info.width * info.height <= 40_000_000
        ):
            raise ValueError("Decoded metadata does not match stored bytes")
        records = store.validations(scope, version.file_id, version.version_id)
        verified = [
            record
            for record in records
            if record.passed and record.media_decoded and _DECODE_CHECKS.issubset(record.checks)
        ]
        if not verified:
            raise ValueError("No successful bounded decoder attestation")
        if transform_raw is not None:
            receipt = ImageTransformReceipt.model_validate(transform_raw)
            if (
                receipt.parent_group_id != scope.group_id
                or (receipt.operation == "format_convert" and receipt.parent_scope != scope)
                or receipt.output_sha256 != version.sha256
                or receipt.output_format != info.format
                or receipt.output_pixel_sha256 != info.pixel_sha256
                or (receipt.width, receipt.height) != (info.width, info.height)
                or not any(
                    receipt.validation_check in record.checks
                    and "parent_sha256:" + receipt.parent_sha256 in record.checks
                    for record in verified
                )
            ):
                raise ValueError("Transform receipt does not attest this scoped output")
            parent = store.get(
                receipt.parent_scope, receipt.parent_file_id, receipt.parent_version_id
            )
            if parent.sha256 != receipt.parent_sha256:
                raise ValueError("Transform parent bytes changed")
            parent_info = ImageInfo.model_validate(parent.provenance["media_metadata"])
            if (
                receipt.parent_pixel_sha256 != parent_info.pixel_sha256
                or (parent_info.width, parent_info.height) != (info.width, info.height)
                or (
                    receipt.operation == "verified_copy"
                    and (
                        parent.sha256 != version.sha256
                        or canonical_artifact_format(parent.format) != info.format
                    )
                )
                or (
                    receipt.operation == "format_convert"
                    and info.format in {"png", "webp"}
                    and parent_info.pixel_sha256 != info.pixel_sha256
                )
            ):
                raise ValueError("Transform differs from its parent pixels or exact copy")
            lineage = resolve_public_image(
                store, receipt.parent_scope, parent, _seen=_seen | {version.version_id}
            )
            if lineage is None:
                if raw is not None or receipt.source_sha256 is not None:
                    raise ValueError("A generated image cannot acquire public provenance")
                return None
            if (
                raw != lineage.public_image
                or receipt.source_sha256 != lineage.source_sha256
                or source.origin_kind != "research"
                or source.source_url != lineage.source.source_url
                or source.source_event_id != lineage.source.source_event_id
            ):
                raise ValueError("Transform public provenance differs from its verified parent")
            return PublicImageLineage(
                lineage.source_sha256,
                lineage.public_image,
                lineage.source,
                (version, *lineage.versions),
            )
        if not isinstance(raw, dict):
            raise ValueError("Original public image evidence must be an object")
        evidence = _PublicImageEvidence.model_validate(raw)
        if (
            source.origin_kind != "research"
            or source.source_url != evidence.final_url
            or evidence.sha256 != version.sha256
            or evidence.content_type != info.mime_type
            or (evidence.width, evidence.height) != (info.width, info.height)
            or evidence.static_first_frame != info.static_first_frame
            or any(
                parse_public_url(url)[0] != url
                for url in (
                    evidence.requested_url,
                    evidence.final_url,
                    evidence.page_url,
                    evidence.page_final_url,
                )
            )
            or any(
                not isinstance(raw[key], str) or len(raw[key]) > 64
                for key in ("page_retrieved_at", "retrieved_at")
            )
        ):
            raise ValueError("Original public download evidence is unbound")
        return PublicImageLineage(version.sha256, dict(raw), source, (version,))
    except (ValueError, KeyError, TypeError, AttributeError):
        raise _invalid("Stored image provenance or trusted transform evidence is invalid") from None


def convert_image(
    content: bytes,
    *,
    output_format: ImageFormat,
    max_bytes: int = 15 * 1024 * 1024,
    max_pixels: int = 40_000_000,
) -> tuple[dict[str, Any], bytes]:
    """Worker-only conversion of the canonical decoded static view."""
    from PIL import Image

    from groupagent.media.validation import inspect_image

    if output_format not in {"png", "jpeg", "webp", "gif"}:
        raise DomainError("unsupported_image", "Unsupported image conversion format")
    parent, view = inspect_image(content, max_bytes=max_bytes, max_pixels=max_pixels)
    if output_format == "png":
        output = view
    else:
        with Image.open(io.BytesIO(view)) as image:
            result = io.BytesIO()
            options = {"lossless": True} if output_format == "webp" else {}
            image.save(result, format=output_format.upper(), **options)
            output = result.getvalue()
    info, _ = inspect_image(output, max_bytes=max_bytes, max_pixels=max_pixels)
    if info.format != output_format or (info.width, info.height) != (parent.width, parent.height):
        raise DomainError("worker_integrity_failed", "Converted image format or dimensions differ")
    return {
        "parent": parent.model_dump(mode="json"),
        "output": info.model_dump(mode="json"),
    }, output


class ImageTransformService:
    def __init__(self, media: MediaService):
        self.media = media

    async def convert(
        self, context: ToolContext, file_id: str, version_id: str, output_format: ImageFormat
    ) -> ConvertedImageRef:
        scope = self.media.scope(context)
        parent = self.media.store.get(scope, file_id, version_id)
        if parent.created_by != scope.actor_id or parent.format not in _IMAGE_FORMATS:
            raise DomainError(
                "scope_violation", "Convert an image imported into this work and actor scope"
            )
        reference = self.media.load_ref(context, file_id, version_id)
        content = await self.media._content(context, reference)
        lineage = resolve_public_image(self.media.store, scope, parent)
        result, outputs = await self.media.worker.run(
            context,
            "image_convert",
            (content,),
            parameters={"format": output_format, **self.media._image_parameters()},
            outputs=("converted." + output_format,),
        )
        try:
            parent_info = ImageInfo.model_validate(result["parent"])
            claimed = ImageInfo.model_validate(result["output"])
            output = outputs[0]
            if (
                len(outputs) != 1
                or parent_info.source_sha256 != parent.sha256
                or parent_info != ImageInfo.model_validate(parent.provenance["media_metadata"])
            ):
                raise ValueError("Conversion parent does not match")
        except (ValueError, KeyError, TypeError, IndexError):
            raise DomainError(
                "worker_integrity_failed", "Image conversion returned invalid evidence"
            ) from None
        checked, _ = await self.media.worker.run(
            context,
            "image_validate",
            (output,),
            parameters=self.media._image_parameters(),
            outputs=("view.png",),
        )
        info = ImageInfo.model_validate(checked)
        if (
            info != claimed
            or info.source_sha256 != hashlib.sha256(output).hexdigest()
            or info.format != output_format
            or info.byte_length != len(output)
            or not 0 < info.width * info.height <= self.media.max_image_pixels
            or (info.width, info.height) != (parent_info.width, parent_info.height)
            or (output_format in {"png", "webp"} and info.pixel_sha256 != parent_info.pixel_sha256)
        ):
            raise DomainError(
                "worker_integrity_failed",
                "Converted bytes do not match trusted conversion evidence",
            )
        # Recheck exact parent bytes and live source authority after worker awaits.
        if await self.media._content(context, reference) != content:
            raise DomainError("worker_integrity_failed", "Image conversion parent changed")
        if lineage != resolve_public_image(self.media.store, scope, parent):
            raise _invalid("Image source lineage changed during conversion")
        saved = self._save(context, parent, reference.source, output, parent_info, info, lineage)
        return ConvertedImageRef(
            **saved.model_dump(),
            page_citation=lineage.page_citation if lineage else None,
            image_citation=lineage.image_citation if lineage else None,
        )

    def _save(
        self,
        context: ToolContext,
        parent: ArtifactVersion,
        source: MediaSource,
        payload: bytes,
        parent_info: ImageInfo,
        info: ImageInfo,
        lineage: PublicImageLineage | None,
        *,
        operation: Literal["format_convert", "verified_copy"] = "format_convert",
        source_event_ids: tuple[str, ...] = (),
    ) -> MediaRef:
        receipt = ImageTransformReceipt(
            operation=operation,
            parent_group_id=parent.group_id,
            parent_work_id=parent.work_id,
            parent_created_by=parent.created_by,
            parent_file_id=parent.file_id,
            parent_version_id=parent.version_id,
            parent_sha256=parent.sha256,
            parent_pixel_sha256=parent_info.pixel_sha256,
            output_sha256=info.source_sha256,
            output_pixel_sha256=info.pixel_sha256,
            output_format=info.format,
            width=info.width,
            height=info.height,
            source_sha256=lineage.source_sha256 if lineage else None,
        )
        scope = self.media.scope(context)
        version = self.media.store.create(
            scope,
            "image-" + info.source_sha256[:16] + "." + info.format,
            payload,
            provenance={
                **source.model_dump(mode="json"),
                "media_metadata": info.model_dump(mode="json"),
                "image_transform": receipt.model_dump(mode="json"),
                **({"public_image": lineage.public_image} if lineage else {}),
                **({"source_event_ids": list(source_event_ids)} if source_event_ids else {}),
            },
        )
        self.media.store.record_validation(
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
                checks=(
                    *sorted(_DECODE_CHECKS),
                    receipt.validation_check,
                    "parent_sha256:" + parent.sha256,
                ),
                limitations=(
                    "Conversion uses the decoded static first frame; transparent pixels are composited on white",
                ),
            ),
        )
        return self.media.load_ref(context, version.file_id, version.version_id)

    async def copy_verified(
        self,
        context: ToolContext,
        original: ArtifactVersion,
        payload: bytes,
        *,
        source: MediaSource,
        source_event_ids: tuple[str, ...] = (),
    ) -> MediaRef:
        """Host-only input-copy API; caller must authorize the original catalog first."""
        scope = await self.media._check(context, (source,))
        if original.group_id != scope.group_id:
            raise DomainError("scope_violation", "Image inputs cannot cross groups")
        original_scope = ArtifactScope(original.group_id, original.work_id, original.created_by)
        lineage = resolve_public_image(self.media.store, original_scope, original)
        if (
            lineage is None
            or self.media.store.read(original_scope, original.file_id, original.version_id)
            != payload
        ):
            raise _invalid("Input copy does not match a verified public source version")
        result, _ = await self.media.worker.run(
            context,
            "image_validate",
            (payload,),
            parameters=self.media._image_parameters(),
            outputs=("view.png",),
        )
        info = ImageInfo.model_validate(result)
        parent_info = ImageInfo.model_validate(original.provenance["media_metadata"])
        if info != parent_info or info.source_sha256 != hashlib.sha256(payload).hexdigest():
            raise DomainError(
                "worker_integrity_failed", "Input copy pixels differ from its exact parent bytes"
            )
        await self.media._check(context, (source,))
        resolve_public_image(self.media.store, original_scope, original)
        return self._save(
            context,
            original,
            source,
            payload,
            parent_info,
            info,
            lineage,
            operation="verified_copy",
            source_event_ids=source_event_ids,
        )


def register_image_transform_tools(gateway: ToolGateway, service: ImageTransformService) -> None:
    async def convert(arguments: ImageConvertInput, context: ToolContext) -> ConvertedImageRef:
        return await service.convert(
            context, arguments.file_id, arguments.version_id, arguments.format
        )

    gateway.register(
        ToolDefinition(
            name="image_convert",
            description=(
                "Convert an existing scoped image to PNG/JPEG/WebP/GIF using a bounded isolated decoder. "
                "Preserves verifiable original public-image sources and counts different formats of one source once. "
                "Reuse imported file_id/version_id; do not search or download again for a format change. "
                "Uses the static first frame, composites transparency on white; JPEG/GIF may lose color detail. "
                "Returns a real artifact for artifact_deliver."
            ),
            input_model=ImageConvertInput,
            output_model=ConvertedImageRef,
            handler=convert,
            resource_scopes=("group", "work", "artifacts"),
            timeout_seconds=370,
            max_input_bytes=2048,
            max_output_bytes=40000,
            max_artifact_bytes=service.media.max_image_bytes,
            cost_meter="no_provider_charge",
            idempotency_mode=IdempotencyMode.RECONCILE_BEFORE_RETRY,
        )
    )
