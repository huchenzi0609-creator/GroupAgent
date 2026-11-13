import os

import pytest

from groupagent.artifacts import DocumentRenderer
from groupagent.service import run_demo


@pytest.mark.postgres
async def test_two_real_pg_artifact_runs_and_replay_dedup(tmp_path):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    first = await run_demo(dsn, tmp_path)
    second = await run_demo(dsn, tmp_path)
    assert first["work_id"] != second["work_id"]
    assert first["state"] == second["state"] == "completed"
    assert first["validation"]["passed"]
    assert first["request_replay_deduplicated"]
    assert first["live_model"] is False and first["live_channel"] is False


@pytest.mark.postgres
@pytest.mark.render
async def test_rendered_docx_reaches_completion_only_after_validation(tmp_path):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn or os.environ.get("GROUPAGENT_RENDER_TESTS") != "1":
        pytest.skip("Requires isolated PostgreSQL, LibreOffice and Poppler")
    result = await run_demo(
        dsn,
        tmp_path,
        document_format="docx",
        renderer=DocumentRenderer(pdftotext=os.environ.get("GROUPAGENT_PDFTOTEXT")),
    )
    assert result["state"] == "completed"
    assert result["validation"]["rendered"] and result["validation"]["layout_passed"]
