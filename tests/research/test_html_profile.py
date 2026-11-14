from __future__ import annotations

import base64
import io

from PIL import Image

from groupagent.research.html_profile import static_html


def test_active_media_and_scripts_are_removed_before_browser_parsing():
    source = b"""<html><body><p>Source result 87</p><video autoplay src="data:video/mp4;base64,AAAA"><source src="video.mp4"></video>
    <script>document.createElement('video')</script><iframe src="http://localhost/private"></iframe></body></html>"""
    result, removed = static_html(source)
    assert "Source result 87" in result
    assert (
        "<video" not in result
        and "data:video" not in result
        and "<script" not in result
        and "<iframe" not in result
    )
    assert removed >= 4


def test_inert_html_preserves_visible_content_and_normalizes_inline_pixels():
    output = io.BytesIO()
    Image.new("RGB", (20, 20), "blue").save(output, "PNG")
    image = base64.b64encode(output.getvalue()).decode()
    source = f'<html><body><h1>Course</h1><p style="color:blue" onclick="steal()">Read 24</p><img src="data:image/png;base64,{image}"></body></html>'.encode()
    result, _ = static_html(source)
    assert "Read 24" in result and "color:blue" in result
    assert "onclick" not in result and "steal()" not in result
    assert "data:image/png;base64," in result
