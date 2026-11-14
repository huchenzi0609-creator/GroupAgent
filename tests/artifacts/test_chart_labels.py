from __future__ import annotations

import io

import pytest
from PIL import Image, ImageDraw

from groupagent.artifacts import ArtifactError, ArtifactScope, ChartData, ChartSeries
from groupagent.artifacts.visuals import chart_png

from .test_isolated_matrix import document_runtime

__all__ = ["document_runtime"]


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
            if (
                kwargs.get("fill") == "#111111"
                and kwargs.get("anchor") in {"mt", "mb"}
                and text in {str(value) for value in values}
            ):
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


@pytest.mark.parametrize(
    "categories",
    [
        ("Alpha", "Beta", "Gamma"),
        ("春季", "夏季", "秋季"),
        ("Long category name", "Another long category"),
        tuple(f"W{index}" for index in range(1, 21)),
    ],
)
def test_category_labels_keep_original_text_centered_in_bounds(monkeypatch, categories):
    observed = []
    original_draw = ImageDraw.Draw

    class MeasureActualLabels:
        def __init__(self, raster):
            self.draw = original_draw(raster)

        def __getattr__(self, name):
            return getattr(self.draw, name)

        def text(self, location, text, **kwargs):
            if text in categories:
                box = self.draw.textbbox(
                    location, text, font=kwargs["font"], anchor=kwargs.get("anchor")
                )
                observed.append((text, location, box, kwargs["font"].size))
            return self.draw.text(location, text, **kwargs)

    monkeypatch.setattr(ImageDraw, "Draw", MeasureActualLabels)
    raw = chart_png(
        ChartData(
            "line",
            "Labels",
            categories,
            (ChartSeries("Value", tuple(range(1, len(categories) + 1))),),
        )
    )
    with Image.open(io.BytesIO(raw)) as image:
        width, height = image.size
    assert height == 720 and 1200 <= width <= 3200
    if len(categories) <= 4:
        assert width == 1200
    else:
        assert width > 1200
    assert [item[0] for item in observed] == list(categories)
    for _text, position, box, size in observed:
        assert size == 28 and 0 <= box[0] < box[2] < width and 0 < box[1] < box[3] < height
        assert abs((box[0] + box[2]) / 2 - position[0]) <= 1
    assert all(
        left[2][2] < right[2][0] for left, right in zip(observed, observed[1:], strict=False)
    )


def test_unreadably_wide_labels_are_rejected_instead_of_clipped_or_replaced():
    categories = ("Long unchanged category label",) * 20
    with pytest.raises(ArtifactError, match="split the chart"):
        chart_png(ChartData("bar", "Labels", categories, (ChartSeries("Value", (1,) * 20),)))


@pytest.mark.sandbox
@pytest.mark.parametrize(
    "chart",
    [
        ChartData(
            "line",
            "Synthetic Trend",
            ("W1", "W2", "W3", "W4"),
            (ChartSeries("Value", (2, 4, 3, 7)),),
        ),
        ChartData(
            "bar", "合成测量", ("春季", "夏季", "秋季"), (ChartSeries("数值", (11, 24, 38)),)
        ),
    ],
)
async def test_real_worker_chart_labels_are_read_by_unchanged_ocr(document_runtime, chart):
    service, context = document_runtime
    version = await service.render_chart(context, "labels.png", chart)
    raw = service.store.read(
        ArtifactScope(context.group_id, context.work_id, context.actor.member_id),
        version.file_id,
        version.version_id,
    )
    recognized, _ = await service.worker.run(
        context, "ocr_image", (raw,), parameters={"languages": "eng+chi_sim"}
    )
    assert recognized["method"] == "tesseract" and recognized["segmentation_mode"] == 6
    assert all(label.casefold() in recognized["text"].casefold() for label in chart.categories), (
        recognized["text"]
    )
