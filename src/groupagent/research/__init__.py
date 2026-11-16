"""Optional research adapters. No paid request occurs on import or registration."""

from .citations import source_citation, validate_research_citations
from .deepseek import DeepSeekResearchClient
from .evidence import EvidenceStore, project_research_sources
from .images import PublicImageRef, PublicImageService, register_public_image_tools
from .models import (
    EvidenceRef,
    FetchInput,
    FetchOutput,
    ResearchSource,
    SearchInput,
    SearchOutput,
    SourceReadInput,
    SourceReadOutput,
)
from .session import ResearchSession
from .tools import register_research_tools
from .urls import PublicURLPolicy

__all__ = [
    "DeepSeekResearchClient",
    "EvidenceRef",
    "EvidenceStore",
    "FetchInput",
    "FetchOutput",
    "PublicURLPolicy",
    "PublicImageService",
    "PublicImageRef",
    "ResearchSource",
    "ResearchSession",
    "SearchInput",
    "SearchOutput",
    "SourceReadInput",
    "SourceReadOutput",
    "project_research_sources",
    "register_research_tools",
    "register_public_image_tools",
    "source_citation",
    "validate_research_citations",
]
