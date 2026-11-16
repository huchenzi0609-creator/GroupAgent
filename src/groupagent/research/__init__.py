"""Optional research adapters. No paid request occurs on import or registration."""

from .citations import source_citation, validate_research_citations
from .deepseek import DeepSeekResearchClient
from .images import PublicImageRef, PublicImageService, register_public_image_tools
from .models import FetchInput, FetchOutput, ResearchSource, SearchInput, SearchOutput
from .tools import register_research_tools
from .urls import PublicURLPolicy

__all__ = [
    "DeepSeekResearchClient",
    "FetchInput",
    "FetchOutput",
    "PublicURLPolicy",
    "PublicImageService",
    "PublicImageRef",
    "ResearchSource",
    "SearchInput",
    "SearchOutput",
    "register_research_tools",
    "register_public_image_tools",
    "source_citation",
    "validate_research_citations",
]
