"""Optional research adapters. No paid request occurs on import or registration."""

from .deepseek import DeepSeekResearchClient
from .models import FetchInput, FetchOutput, ResearchSource, SearchInput, SearchOutput
from .tools import register_research_tools
from .urls import PublicURLPolicy

__all__ = [
    "DeepSeekResearchClient",
    "FetchInput",
    "FetchOutput",
    "PublicURLPolicy",
    "ResearchSource",
    "SearchInput",
    "SearchOutput",
    "register_research_tools",
]
