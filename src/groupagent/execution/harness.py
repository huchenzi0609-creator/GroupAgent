"""Read-compatible import for the retired Harness backend.

Native Harness execution is intentionally unavailable. Existing checkpoints
remain data; the control plane must migrate or reconcile them before running
the single LangGraph backend. This class cannot launch a legacy worker.
"""

from groupagent.domain import DomainError


class HarnessEngine:
    def __init__(self, *args, **kwargs):
        raise DomainError(
            "execution_backend_retired",
            "Harness execution was replaced by LangGraphEngine; migrate the checkpoint first",
        )
