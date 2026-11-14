"""Bounded reproducible charts and inline images. Worker-only for attachment bytes."""

from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION
from pptx.util import Inches, Pt

from .models import ArtifactError, ChartData, ImageData, Slide


def check_chart(chart: ChartData) -> None:
    if chart.kind not in {"bar", "line", "pie"} or not chart.title or len(chart.title) > 120:
        raise ArtifactError(
            "unsupported_chart", "Only bounded bar, line and pie charts are supported"
        )
    if not 1 <= len(chart.categories) <= 20 or not 1 <= len(chart.series) <= 4:
        raise ArtifactError("chart_limit", "Chart supports 1-20 categories and 1-4 series")
    if chart.kind == "pie" and len(chart.series) != 1:
        raise ArtifactError("unsupported_chart", "Pie charts need exactly one series")
    for category in chart.categories:
        if not category or len(category) > 40:
            raise ArtifactError("chart_limit", "Chart labels must be nonempty and short")
    for series in chart.series:
        if not series.name or len(series.name) > 60 or len(series.values) != len(chart.categories):
            raise ArtifactError("invalid_chart", "Chart series must match its categories")
        if any(not math.isfinite(value) or abs(value) > 1e12 for value in series.values):
            raise ArtifactError("invalid_chart", "Chart values must be finite and bounded")
        if chart.kind == "pie" and (
            any(value < 0 for value in series.values) or sum(series.values) <= 0
        ):
            raise ArtifactError(
                "invalid_chart", "Pie chart values must be nonnegative with a positive total"
            )


def validated_image(image: ImageData) -> tuple[bytes, int, int]:
    from groupagent.media.validation import inspect_image

    if (
        not image.alt_text.strip()
        or len(image.alt_text) > 500
        or not 1 <= image.width_inches <= 6.5
    ):
        raise ArtifactError(
            "invalid_image_layout", "Inline image needs alt text and a bounded width"
        )
    info, content = inspect_image(image.content)
    return content, info.width, info.height


def _font(size: int):
    for path in (
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ):
        if Path(path).is_file():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default(size=size)


def chart_png(chart: ChartData) -> bytes:
    """A deterministic data chart, never represented as an image-generation model."""
    check_chart(chart)
    image = Image.new("RGB", (1200, 720), "white")
    draw = ImageDraw.Draw(image)
    colors = ("#245D8C", "#E39A35", "#3D9470", "#A36599")
    draw.text((70, 25), chart.title, fill="#132536", font=_font(32))
    label_font = _font(18)
    if chart.kind == "pie":
        total = sum(chart.series[0].values)
        angle = -90.0
        for index, (category, value) in enumerate(
            zip(chart.categories, chart.series[0].values, strict=True)
        ):
            end = angle + 360 * value / total
            draw.pieslice(
                (100, 110, 650, 660),
                angle,
                end,
                fill=colors[index % len(colors)],
                outline="white",
                width=3,
            )
            draw.text(
                (720, 140 + index * 25),
                f"{category}: {value:g}",
                fill=colors[index % len(colors)],
                font=label_font,
            )
            angle = end
    else:
        x0, y0, x1, y1 = 95, 120, 1140, 590
        values = [value for series in chart.series for value in series.values]
        low, high = min(0.0, min(values)), max(0.0, max(values))
        if low == high:
            high = low + 1
        span = high - low

        def y(value: float) -> float:
            return y1 - (value - low) / span * (y1 - y0)

        for tick in range(6):
            value = low + span * tick / 5
            position = y(value)
            draw.line((x0, position, x1, position), fill="#DAE0E5", width=1)
            draw.text((5, position - 10), f"{value:g}", fill="#394A59", font=label_font)
        group_width = (x1 - x0) / len(chart.categories)
        for index, category in enumerate(chart.categories):
            middle = x0 + group_width * (index + 0.5)
            draw.text((middle - 22, y1 + 20), category, fill="#132536", font=label_font)
        line_labels = []
        for series_index, series in enumerate(chart.series):
            color = colors[series_index]
            points = []
            for index, value in enumerate(series.values):
                middle = x0 + group_width * (index + 0.5)
                position = y(value)
                if chart.kind == "bar":
                    width = group_width * 0.7 / len(chart.series)
                    left = middle - group_width * 0.35 + width * series_index
                    draw.rectangle(
                        (left, min(y(0), position), left + width - 3, max(y(0), position)),
                        fill=color,
                    )
                    draw.text(
                        (left, position - 23 if value >= 0 else position + 3),
                        f"{value:g}",
                        fill=color,
                        font=label_font,
                    )
                else:
                    points.append((middle, position))
            if len(points) > 1:
                draw.line(points, fill=color, width=4)
            for middle, position in points:
                draw.ellipse((middle - 5, position - 5, middle + 5, position + 5), fill=color)
            if points:
                line_labels.append((color, points, series.values))
            draw.text((110 + series_index * 260, 670), series.name, fill=color, font=label_font)
        # Paint values after every line, outside the point marker. At a valley
        # use the lower side so an ascending segment cannot cross the glyphs.
        value_font = _font(20)
        for color, points, series_values in line_labels:
            for index, ((middle, position), value) in enumerate(
                zip(points, series_values, strict=True)
            ):
                neighbors = points[max(0, index - 1) : index] + points[index + 1 : index + 2]
                below = bool(neighbors) and position > sum(p[1] for p in neighbors) / len(neighbors)
                if position > y1 - 50:
                    below = False  # Keep labels clear of the category axis.
                location = (middle, position + 14 if below else position - 14)
                anchor = "mt" if below else "mb"
                text = f"{value:g}"
                bounds = draw.textbbox(location, text, font=value_font, anchor=anchor)
                draw.rectangle(
                    (bounds[0] - 3, bounds[1] - 3, bounds[2] + 3, bounds[3] + 3), fill="white"
                )
                draw.text(location, text, fill=color, font=value_font, anchor=anchor)
    output = io.BytesIO()
    image.save(output, "PNG")
    return output.getvalue()


def chart_summary(chart: ChartData) -> str:
    return (
        chart.title
        + ": "
        + "; ".join(
            series.name
            + " "
            + ", ".join(
                f"{label}={value:g}"
                for label, value in zip(chart.categories, series.values, strict=True)
            )
            for series in chart.series
        )
    )


def add_slide_visuals(slide: Any, data: Slide, font_family: str) -> None:
    entries: list[tuple[str, Any]] = []
    if data.table:
        entries.append(("table", data.table))
    if data.image:
        entries.append(("image", data.image))
    if data.chart:
        entries.append(("chart", data.chart))
    if len(entries) > 2:
        raise ArtifactError("unsupported_layout", "A slide supports at most two visual blocks")
    width = 11.7 if len(entries) == 1 else 5.65
    for index, (kind, value) in enumerate(entries):
        left, top, height = 0.8 + index * 6.05, 3.5, 3.0
        if kind == "image":
            content, image_width, image_height = validated_image(value)
            fitted_width = min(width, height * image_width / image_height)
            fitted_height = fitted_width * image_height / image_width
            shape = slide.shapes.add_picture(
                io.BytesIO(content),
                Inches(left + (width - fitted_width) / 2),
                Inches(top),
                width=Inches(fitted_width),
                height=Inches(fitted_height),
            )
            shape.name = "groupagent.image"
            shape._element.xpath(".//p:cNvPr")[0].set("descr", value.alt_text)
        elif kind == "chart":
            check_chart(value)
            chart_data = CategoryChartData()
            chart_data.categories = value.categories
            for series in value.series:
                chart_data.add_series(series.name, series.values)
            chart_type = {
                "bar": XL_CHART_TYPE.COLUMN_CLUSTERED,
                "line": XL_CHART_TYPE.LINE,
                "pie": XL_CHART_TYPE.PIE,
            }[value.kind]
            shape = slide.shapes.add_chart(
                chart_type, Inches(left), Inches(top), Inches(width), Inches(height), chart_data
            )
            shape.name = "groupagent.chart"
            chart = shape.chart
            chart.has_title = True
            chart.chart_title.text_frame.text = value.title
            chart.has_legend = len(value.series) > 1 or value.kind == "pie"
            plot = chart.plots[0]
            plot.has_data_labels = True
            plot.data_labels.font.size = Pt(11)
            if value.kind == "bar":
                plot.data_labels.position = XL_LABEL_POSITION.OUTSIDE_END
        else:
            rows = (value.headers, *value.rows)
            shape = slide.shapes.add_table(
                len(rows), len(rows[0]), Inches(left), Inches(top), Inches(width), Inches(height)
            )
            shape.name = "groupagent.table"
            for row_index, row in enumerate(rows):
                for column_index, text in enumerate(row):
                    cell = shape.table.cell(row_index, column_index)
                    cell.text = text
                    cell.margin_left = cell.margin_right = Inches(0.08)
                    cell.fill.solid()
                    cell.fill.fore_color.rgb = RGBColor(
                        *((232, 239, 245) if row_index == 0 else (255, 255, 255))
                    )
                    for paragraph in cell.text_frame.paragraphs:
                        paragraph.font.name = font_family
                        paragraph.font.size = Pt(15)
                        paragraph.font.bold = row_index == 0
                        paragraph.font.color.rgb = RGBColor(20, 33, 46)
