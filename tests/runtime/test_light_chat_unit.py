"""Light reply text contracts.

Batch priority is covered by test_batch_priority.py, including real PostgreSQL
invocation, receipt, source and feature-gate checks rather than content filters.
"""

from __future__ import annotations

import pytest

from groupagent.conversation.service import _work_admission_text


@pytest.mark.parametrize(
    "tools,formats,research,phrase",
    [
        (["python_compute"], ["text"], False, "实际运行 Python"),
        ([], ["docx", "pdf"], False, "Word、PDF"),
        (["web_fetch"], ["text"], True, "核对来源"),
    ],
)
def test_work_acceptance_describes_known_future_work_without_claiming_completion(
    tools, formats, research, phrase
):
    text = _work_admission_text(tools, formats, research_required=research)
    assert phrase in text and "会" in text
    assert "收到" not in text and "已完成" not in text
