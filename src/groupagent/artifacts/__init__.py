"""Scoped immutable artifacts and the bounded document foundation."""

from .documents import DocumentService
from .models import (
    ArtifactError,
    ArtifactScope,
    ArtifactVersion,
    Deck,
    DocumentRead,
    DocumentUnit,
    Report,
    Section,
    Slide,
    TableData,
    ValidationRecord,
)
from .rendering import DocumentRenderer
from .store import ArtifactStore

__all__ = [
    "ArtifactError",
    "ArtifactScope",
    "ArtifactStore",
    "ArtifactVersion",
    "Deck",
    "DocumentRead",
    "DocumentRenderer",
    "DocumentService",
    "DocumentUnit",
    "Report",
    "Section",
    "Slide",
    "TableData",
    "ValidationRecord",
]
