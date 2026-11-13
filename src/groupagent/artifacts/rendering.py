"""Bounded subprocess rendering and evidence, not a security sandbox or OCR."""

from __future__ import annotations

import hashlib
import io
import math
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

from docx import Document
from PIL import Image, ImageChops
from pptx import Presentation
from pptx.presentation import Presentation as PPTXPresentation
from pptx.text.text import TextFrame
from reportlab.pdfbase import pdfmetrics

from .formats import DECK_MARKER, REPORT_MARKER, inspect_office, open_pdf
from .models import ArtifactError


def normalized_text(text: str) -> str:
    return re.sub(r"[\s|\u00ad]+", "", text)


@dataclass(frozen=True)
class RenderResult:
    previews: tuple[bytes, ...]
    page_count: int
    checks: tuple[str, ...]
    issues: tuple[str, ...]
    renderer: str
    layout_supported: bool

    @property
    def preview_sha256(self) -> tuple[str, ...]:
        return tuple(hashlib.sha256(data).hexdigest() for data in self.previews)


class DocumentRenderer:
    def __init__(
        self,
        *,
        soffice: str | None = None,
        pdftoppm: str | None = None,
        pdftotext: str | None = None,
        timeout_seconds: float = 60,
        font_directories: tuple[Path, ...] = (),
    ):
        self.soffice = soffice or shutil.which("soffice")
        self.pdftoppm = pdftoppm or shutil.which("pdftoppm")
        self.pdftotext = pdftotext or shutil.which("pdftotext")
        if not 0 < timeout_seconds <= 180:
            raise ArtifactError("invalid_limit", "Rendering timeout must be within 180 seconds")
        self.timeout_seconds = timeout_seconds
        self.font_directories = tuple(Path(directory) for directory in font_directories)
        if any(not directory.is_dir() for directory in self.font_directories):
            raise ArtifactError("font_unavailable", "Configured font directory is missing")

    def _run(self, args: list[str], cwd: Path) -> None:
        environment = {**os.environ, "TMPDIR": str(cwd)}
        font_config = cwd / "fonts.conf"
        if font_config.exists():
            environment["FONTCONFIG_FILE"] = str(font_config)
            environment["FONTCONFIG_PATH"] = str(cwd)
            environment["SAL_FONTPATH"] = ":".join(str(path) for path in self.font_directories)
        try:
            result = subprocess.run(
                args,
                cwd=cwd,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
                env=environment,
            )
        except subprocess.TimeoutExpired as exc:
            raise ArtifactError("render_timeout", "Document rendering timed out") from exc
        except OSError as exc:
            raise ArtifactError(
                "renderer_unavailable", "Document renderer cannot be executed"
            ) from exc
        if result.returncode:
            # Logs may contain document text; do not expose them across the worker boundary.
            raise ArtifactError(
                "render_failed", f"Document renderer exited with {result.returncode}"
            )

    def render(
        self, content: bytes, format_name: str, *, expected_text: tuple[str, ...]
    ) -> RenderResult:
        if format_name not in {"pdf", "docx", "pptx"}:
            raise ArtifactError("unsupported_format", "This format has no paginated renderer")
        if not self.pdftoppm or not self.pdftotext or (format_name != "pdf" and not self.soffice):
            raise ArtifactError(
                "renderer_unavailable", "LibreOffice and Poppler tools are required"
            )
        issues: list[str] = []
        checks = [
            "rendered_pdf_open",
            "rendered_expected_text",
            "page_word_bounds",
            "page_text_overlap",
            "nonblank_raster",
            "preview_count",
        ]
        supported = False
        if format_name in {"docx", "pptx"}:
            inspect_office(content, format_name, for_render=True)
            if format_name == "docx":
                document = Document(io.BytesIO(content))
                supported = document.core_properties.keywords == REPORT_MARKER
            else:
                presentation = Presentation(io.BytesIO(content))
                supported = presentation.core_properties.keywords == DECK_MARKER
                issues.extend(self._slide_geometry(presentation))
                checks.extend(("slide_shape_bounds_and_overlap", "slide_text_capacity"))
        else:
            reader = open_pdf(content)
            supported = reader.metadata is not None and reader.metadata.subject == REPORT_MARKER
        with tempfile.TemporaryDirectory(prefix="groupagent-render-") as directory:
            work = Path(directory)
            if self.font_directories:
                font_dirs = "".join(
                    f"<dir>{escape(str(path.absolute()))}</dir>" for path in self.font_directories
                )
                (work / "fonts.conf").write_text(
                    '<?xml version="1.0"?><fontconfig>'
                    + font_dirs
                    + f"<cachedir>{escape(str(work / 'font-cache'))}</cachedir></fontconfig>",
                    encoding="utf-8",
                )
            source = work / f"source.{format_name}"
            source.write_bytes(content)
            if format_name != "pdf":
                profile = (work / "profile").as_uri()
                assert self.soffice is not None
                self._run(
                    [
                        self.soffice,
                        f"-env:UserInstallation={profile}",
                        "--headless",
                        "--nologo",
                        "--nodefault",
                        "--nolockcheck",
                        "--norestore",
                        "--convert-to",
                        "pdf",
                        "--outdir",
                        str(work),
                        str(source),
                    ],
                    work,
                )
            pdf = work / "source.pdf"
            if not pdf.is_file():
                raise ArtifactError("render_failed", "Renderer did not produce a PDF")
            rendered_pdf = pdf.read_bytes()
            if len(rendered_pdf) > 50 * 1024 * 1024:
                raise ArtifactError("render_limit", "Rendered PDF exceeds 50 MiB")
            reader = open_pdf(rendered_pdf)
            count = len(reader.pages)
            if format_name == "pptx" and count != len(presentation.slides):
                issues.append("rendered_slide_count_mismatch")
            text = normalized_text("\n".join(page.extract_text() or "" for page in reader.pages))
            for index, expected in enumerate(expected_text):
                if normalized_text(expected) not in text:
                    issues.append(f"rendered_missing_content:{index}")
            bbox = work / "bounds.html"
            self._run([self.pdftotext, "-bbox", "-enc", "UTF-8", str(pdf), str(bbox)], work)
            issues.extend(self._page_geometry(bbox))
            self._run(
                [
                    self.pdftoppm,
                    "-r",
                    "110",
                    "-scale-to",
                    "1800",
                    "-png",
                    str(pdf),
                    str(work / "page"),
                ],
                work,
            )
            pages = sorted(work.glob("page-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
            if len(pages) != count:
                issues.append("preview_count_mismatch")
            previews = []
            for index, path in enumerate(pages):
                if path.stat().st_size > 10 * 1024 * 1024:
                    raise ArtifactError("render_limit", "Page preview exceeds 10 MiB")
                data = path.read_bytes()
                previews.append(data)
                with Image.open(io.BytesIO(data)) as raster:
                    if raster.width * raster.height > 4_000_000:
                        raise ArtifactError("render_limit", "Page preview dimensions are too large")
                    gray = raster.convert("L")
                    ink = ImageChops.difference(gray, Image.new("L", gray.size, 255))
                    if ink.getbbox() is None or sum(gray.histogram()[:220]) < 20:
                        issues.append(f"blank_page:{index + 1}")
            if sum(map(len, previews)) > 100 * 1024 * 1024:
                raise ArtifactError("render_limit", "Combined previews exceed 100 MiB")
            if not supported:
                issues.append("layout_policy_unsupported")
            renderer = "poppler" if format_name == "pdf" else "libreoffice+poppler"
            return RenderResult(
                tuple(previews), count, tuple(checks), tuple(issues), renderer, supported
            )

    @staticmethod
    def _slide_geometry(presentation: PPTXPresentation) -> list[str]:
        issues = []
        # python-pptx's dynamic shape types expose dimensions at runtime.
        for index, slide in enumerate(presentation.slides):
            boxes: list[tuple[float, float, float, float]] = []
            for shape in slide.shapes:
                left, top, right, bottom = (
                    shape.left,
                    shape.top,
                    shape.left + shape.width,
                    shape.top + shape.height,
                )
                if (
                    left < 0
                    or top < 0
                    or right > presentation.slide_width
                    or bottom > presentation.slide_height
                ):
                    issues.append(f"shape_outside_slide:{index + 1}:{shape.shape_id}")
                if shape.has_text_frame or shape.has_table:
                    for prior in boxes:
                        if min(right, prior[2]) > max(left, prior[0]) and min(
                            bottom, prior[3]
                        ) > max(top, prior[1]):
                            issues.append(f"shape_overlap:{index + 1}:{shape.shape_id}")
                    boxes.append((left, top, right, bottom))
                if shape.has_text_frame and not DocumentRenderer._frame_fits(
                    shape.text_frame, shape.width, shape.height
                ):
                    issues.append(f"text_frame_overflow:{index + 1}:{shape.shape_id}")
                if shape.has_table:
                    for row_index, row in enumerate(shape.table.rows):
                        for col_index, cell in enumerate(row.cells):
                            width = shape.table.columns[col_index].width
                            if not DocumentRenderer._frame_fits(cell.text_frame, width, row.height):
                                issues.append(
                                    f"table_cell_overflow:{index + 1}:{row_index}:{col_index}"
                                )
            # Rendered word checks supplement these geometry checks; shape bounds
            # alone do not prove that a text frame fits its text.
        return issues

    @staticmethod
    def _frame_fits(frame: TextFrame, width: float, height: float) -> bool:
        """Conservative capacity estimate for the fixed Arial report/deck template.

        It is an additional rejection check, never a substitute for actual rendering.
        """
        available_width = (width - frame.margin_left - frame.margin_right) / 12700
        available_height = (height - frame.margin_top - frame.margin_bottom) / 12700
        used_height = 0.0
        paragraphs = frame.paragraphs
        for index, paragraph in enumerate(paragraphs):
            font_size = paragraph.font.size.pt if paragraph.font.size else 18
            line_width = 0.0
            lines = 1
            # Greedy character wrapping is intentionally conservative for CJK;
            # word-wrap remainders for Latin text add a 10% width allowance.
            for char in paragraph.text:
                char_width = (
                    font_size
                    if unicodedata.east_asian_width(char) in {"W", "F"}
                    else (pdfmetrics.stringWidth(char, "Helvetica", font_size) * 1.1)
                )
                if char == "\n" or line_width + char_width > available_width:
                    lines += 1
                    line_width = 0.0
                if char != "\n":
                    line_width += char_width
            used_height += lines * font_size * 1.2
            if index + 1 < len(paragraphs):
                used_height += paragraph.space_after.pt if paragraph.space_after else 0
        return used_height <= available_height + 1

    @staticmethod
    def _page_geometry(bbox: Path) -> list[str]:
        if bbox.stat().st_size > 20 * 1024 * 1024:
            raise ArtifactError("render_limit", "Rendered text bounds exceed the limit")
        try:
            root = ET.parse(bbox).getroot()
        except (ET.ParseError, OSError) as exc:
            raise ArtifactError("render_failed", "Cannot inspect rendered text bounds") from exc
        issues = []
        for index, page in enumerate(root.iter("{http://www.w3.org/1999/xhtml}page")):
            width, height = float(page.attrib["width"]), float(page.attrib["height"])
            words = []
            for word in page.iter("{http://www.w3.org/1999/xhtml}word"):
                coordinates = tuple(
                    float(word.attrib[key]) for key in ("xMin", "yMin", "xMax", "yMax")
                )
                x0, y0, x1, y1 = coordinates
                if not all(math.isfinite(v) for v in coordinates) or not (
                    -0.5 <= x0 <= x1 <= width + 0.5 and -0.5 <= y0 <= y1 <= height + 0.5
                ):
                    issues.append(f"text_outside_page:{index + 1}")
                words.append(coordinates)
            if not words:
                issues.append(f"no_page_text:{index + 1}")
            if len(words) > 25000:
                raise ArtifactError("render_limit", "Page contains too many text elements")
            # Sweep by vertical coordinate to catch substantial duplicate/overlaid
            # glyph boxes without quadratic work on ordinary report paragraphs.
            active: list[tuple[float, ...]] = []
            for box in sorted(words, key=lambda w: w[1]):
                active = [prior for prior in active if prior[3] > box[1] + 1]
                for prior in active:
                    overlap_w = min(box[2], prior[2]) - max(box[0], prior[0])
                    overlap_h = min(box[3], prior[3]) - max(box[1], prior[1])
                    if (
                        overlap_w > min(box[2] - box[0], prior[2] - prior[0]) * 0.35
                        and overlap_h > min(box[3] - box[1], prior[3] - prior[1]) * 0.35
                    ):
                        issues.append(f"text_overlap:{index + 1}")
                        break
                active.append(box)
        return sorted(set(issues))
