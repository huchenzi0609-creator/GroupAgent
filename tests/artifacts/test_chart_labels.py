from __future__ import annotations

import io

import pytest
from PIL import Image, ImageDraw

from groupagent.artifacts import ChartData, ChartSeries
from groupagent.artifacts.visuals import chart_png


def intersects(segment, rectangle):
    """Independent segment/rectangle clipping oracle for actual draw geometry."""
    start, end = segment
    lower, upper = 0.0, 1.0
    for index in (0, 1):
        delta = end[index] - start[index]
        minimum, maximum = rectangle[index], rectangle[index + 2]
        if delta == 0:
            if not minimum <= start[index] <= maximum:
                return False
        else:
            left, right = sorted(
                ((minimum - start[index]) / delta, (maximum - start[index]) / delta)
            )
            lower, upper = max(lower, left), min(upper, right)
            if lower > upper:
                return False
    return True


@pytest.mark.parametrize("values", [(2, 4, 3, 7), (-2, -4, -3, -7), (2, 2, 2, 2), (0, 0, 0, 0)])
def test_real_line_chart_value_glyphs_do_not_intersect_trend_segments(monkeypatch, values):
    labels, segments, draw_order = [], [], []
    original_draw = ImageDraw.Draw

    class TrackRealDrawing:
        def __init__(self, raster):
            self.draw = original_draw(raster)

        def __getattr__(self, name):
            return getattr(self.draw, name)

        def text(self, location, text, **kwargs):
            if kwargs.get("fill") == "#245D8C" and text in {str(value) for value in values}:
                labels.append(
                    self.draw.textbbox(
                        location, text, font=kwargs["font"], anchor=kwargs.get("anchor")
                    )
                )
                draw_order.append("value")
            return self.draw.text(location, text, **kwargs)

        def line(self, points, **kwargs):
            if kwargs.get("width") == 4:
                segments.extend(zip(points, points[1:], strict=False))
                draw_order.append("line")
            return self.draw.line(points, **kwargs)

    monkeypatch.setattr(ImageDraw, "Draw", TrackRealDrawing)
    result = chart_png(
        ChartData(
            "line", "Synthetic Trend", ("W1", "W2", "W3", "W4"), (ChartSeries("Value", values),)
        )
    )
    with Image.open(io.BytesIO(result)) as image:
        assert image.size == (1200, 720) and image.format == "PNG"
    assert len(labels) == 4 and len(segments) == 3
    assert draw_order == ["line", "value", "value", "value", "value"]
    assert not any(intersects(segment, rectangle) for segment in segments for rectangle in labels)
    assert all(65 < box[1] < box[3] < 600 for box in labels)
