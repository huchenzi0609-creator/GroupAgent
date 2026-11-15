"""Scoped, independently routed image and audio capabilities. No video processing."""

from .isolation import IsolatedMediaWorker
from .models import (
    Capability,
    MediaRef,
    MediaSource,
    OCRResult,
    ProviderRoute,
    Transcript,
    VisionResult,
)
from .providers import CapabilityRegistry, MediaHTTPProvider
from .service import MediaService
from .tools import register_media_tools

__all__ = [
    "Capability",
    "CapabilityRegistry",
    "IsolatedMediaWorker",
    "MediaHTTPProvider",
    "MediaService",
    "MediaRef",
    "MediaSource",
    "OCRResult",
    "ProviderRoute",
    "Transcript",
    "VisionResult",
    "register_media_tools",
]
