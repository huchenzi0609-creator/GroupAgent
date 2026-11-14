from __future__ import annotations


async def test_real_isolated_browser_executes_dom_without_network(isolated_worker, media_context):
    content = b"""<!doctype html><html><head><title>Synthetic browser source</title></head><body>
    <h1>Course results</h1><p id="answer">The result is 87</p>
    <script>document.getElementById('answer').textContent='SCRIPT EXECUTED';
    fetch('http://127.0.0.1/secret').then(()=>document.body.innerHTML='NETWORK ESCAPED').catch(()=>{});</script>
    </body></html>"""
    result, outputs = await isolated_worker.run(
        media_context, "browser_snapshot", (content,), outputs=("screenshot.png",)
    )
    assert result["title"] == "Synthetic browser source" and "The result is 87" in result["text"]
    assert "NETWORK ESCAPED" not in result["text"] and result["external_resources_blocked"]
    assert "SCRIPT EXECUTED" not in result["text"] and result["javascript_enabled"] is False
    assert result["active_content_removed"] >= 1
    assert outputs[0].startswith(b"\x89PNG")


async def test_browser_marks_rendered_challenge_instead_of_bypassing(
    isolated_worker, media_context
):
    result, _ = await isolated_worker.run(
        media_context,
        "browser_snapshot",
        (b"<html><body><h1>Verify you are human</h1></body></html>",),
        outputs=("screenshot.png",),
    )
    assert result["access_blocked"]
