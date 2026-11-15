from typing import Protocol

from groupagent.tools import ToolContext

from .models import ProcessResult, ProcessSpec


class SandboxRunner(Protocol):
    allowed_programs: frozenset[str]

    async def run(self, spec: ProcessSpec, context: ToolContext) -> ProcessResult: ...
