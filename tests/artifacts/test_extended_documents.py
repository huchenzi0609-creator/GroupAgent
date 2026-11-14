from __future__ import annotations

import io

import pytest
from docx import Document
from PIL import Image, ImageDraw
from pptx import Presentation
from pypdf import PdfReader

from groupagent.artifacts import (
    ChartData,
    ChartSeries,
    Deck,
    ImageData,
    Report,
    Section,
    Slide,
    TableData,
)
from groupagent.artifacts.advanced import (
    annotate_pdf,
    embedded_image,
    fill_template,
    pdf_pages,
    replace_chart,
    replace_embedded_image,
)


def picture(color="blue"):
    image = Image.new("RGB", (400, 240), "white")
    ImageDraw.Draw(image).rectangle((80, 50, 320, 190), fill=color)
    output = io.BytesIO()
    image.save(output, "PNG")
    return ImageData(output.getvalue(), "Synthetic experiment diagram")


def chart(kind="bar"):
    return ChartData(
        kind, "Study results", ("Alpha", "Beta", "Gamma"), (ChartSeries("Score", (23, 56, 87)),)
    )


def matrix_document(case, extension):
    table = TableData(("Measure", "Result"), (("Sample count", "24"), ("Score", "87")))
    if extension == "pptx":
        if case == 9:
            return Deck(
                "Course Matrix 9",
                tuple(
                    Slide(
                        f"Section {index + 1}",
                        (f"Result 87 from 24 students on page {index + 1}.",),
                    )
                    for index in range(10)
                ),
            )
        slide = Slide(
            "研究结果" if case == 1 else "Findings",
            ("Result 87 from 24 synthetic students.",),
            table if case in {2, 7} else None,
            picture() if case in {3, 7, 8} else None,
            chart(("bar", "line", "pie")[case - 4])
            if case in {4, 5, 6}
            else chart()
            if case == 8
            else None,
        )
        return Deck(
            f"Course Matrix {case}",
            (slide, Slide("Next Steps", ("Repeat the classroom exercise.",))),
        )
    paragraphs = ("Result 87 from 24 synthetic students.",)
    if case == 9:
        paragraphs = (
            tuple(
                f"Paragraph {index + 1}. The synthetic course study compares two approaches and records the observed learning outcomes."
                for index in range(36)
            )
            + paragraphs
        )
    return Report(
        f"Course Matrix {case}",
        (
            Section(
                "研究目的" if case == 1 else "Purpose", ("Compare two classroom study methods.",)
            ),
            Section(
                "Findings",
                paragraphs,
                (table,) if case in {2, 3, 7} else (),
                (picture(),) if case in {4, 7, 8} else (),
                (chart("bar" if case == 5 else "line" if case == 6 else "pie"),)
                if case in {5, 6, 8}
                else (),
            ),
        ),
    )


@pytest.mark.parametrize("extension", ["md", "docx", "pdf", "pptx"])
@pytest.mark.parametrize("case", range(10))
def test_ten_representative_structural_cases_per_format(service, scope, extension, case):
    version = service.generate(
        scope, f"matrix-{case}.{extension}", matrix_document(case, extension)
    )
    read = service.read(scope, version.file_id)
    assert "87" in read.text
    assert version.size_bytes > 0
    if extension == "pptx" and case == 9:
        # Read units identify individual native shapes; slide count remains a
        # structural/rendered property, not the number of editable text units.
        assert len({unit.locator.split(":")[1] for unit in read.units}) == 10


def test_docx_section_edit_preserves_inline_images_and_table(service, store, scope):
    report = Report(
        "Course Report",
        (
            Section("Purpose", ("First section.",)),
            Section(
                "Findings",
                ("Replace this paragraph.",),
                tables=(TableData(("Count",), (("24",),)),),
                images=(picture(),),
            ),
        ),
    )
    original = service.generate(scope, "report.docx", report)
    edited = service.edit_section(
        scope,
        original.file_id,
        expected_version_id=original.version_id,
        section_index=1,
        paragraphs=("Short findings.",),
    )
    before = Document(io.BytesIO(store.read(scope, original.file_id, original.version_id)))
    after = Document(io.BytesIO(store.read(scope, edited.file_id, edited.version_id)))
    assert len(after.inline_shapes) == len(before.inline_shapes) == 1
    assert after.tables[0]._tbl.xml == before.tables[0]._tbl.xml
    assert (
        embedded_image(store.read(scope, original.file_id, original.version_id), "docx", 0)[0]
        == embedded_image(store.read(scope, edited.file_id), "docx", 0)[0]
    )


@pytest.mark.parametrize("extension", ["docx", "pptx"])
def test_template_replacement_and_image_replacement_are_real(service, store, scope, extension):
    model = (
        Report("{{COURSE}}", (Section("Findings", ("Original text.",), images=(picture(),)),))
        if extension == "docx"
        else Deck("Course", (Slide("{{COURSE}}", ("Original text.",), image=picture()),))
    )
    original = service.generate(scope, "template." + extension, model)
    data = store.read(scope, original.file_id)
    filled = fill_template(data, extension, {"{{COURSE}}": "Updated Course"})
    new = store.create(scope, "filled." + extension, filled)
    assert "Updated Course" in service.read(scope, new.file_id).text
    replaced = replace_embedded_image(filled, extension, 0, picture("red"))
    pixels, _ = embedded_image(replaced, extension, 0)
    with Image.open(io.BytesIO(pixels)) as image:
        assert image.getpixel((150, 100))[:3] == (255, 0, 0)


def test_native_chart_data_point_edit_retains_other_slide(service, store, scope):
    original = service.generate(
        scope,
        "chart.pptx",
        Deck(
            "Course",
            (
                Slide("Chart", ("Results.",), chart=chart()),
                Slide("Unchanged", ("Keep this slide.",)),
            ),
        ),
    )
    data = store.read(scope, original.file_id)
    changed = replace_chart(
        data,
        0,
        "groupagent.chart",
        ChartData("bar", "Revised results", ("Alpha", "Beta"), (ChartSeries("Score", (10, 20)),)),
    )
    before, after = Presentation(io.BytesIO(data)), Presentation(io.BytesIO(changed))
    assert before.slides[1]._element.xml == after.slides[1]._element.xml
    actual = next(shape.chart for shape in after.slides[0].shapes if shape.has_chart)
    assert tuple(actual.series[0].values) == (10, 20)


def test_pdf_page_copy_rotation_and_annotations(service, store, scope, report):
    original = service.generate(scope, "report.pdf", report)
    data = store.read(scope, original.file_id)
    selected = pdf_pages(data, (0, 0), {0: 90})
    reader = PdfReader(io.BytesIO(selected))
    assert len(reader.pages) == 2 and all(page.rotation == 90 for page in reader.pages)
    marked = annotate_pdf(
        data,
        (
            {"page_index": 0, "rect": (54, 600, 250, 625), "kind": "highlight"},
            {
                "page_index": 0,
                "rect": (300, 600, 325, 625),
                "kind": "note",
                "text": "Review synthetic result",
            },
        ),
    )
    result = PdfReader(io.BytesIO(marked))
    assert len(result.pages[0]["/Annots"]) == 2
    assert result.pages[0]["/Annots"][1].get_object()["/Contents"] == "Review synthetic result"
    assert store.read(scope, original.file_id) == data


def test_pdf_passive_open_destination_allowed_but_javascript_rejected():
    from pypdf import PdfWriter
    from pypdf.generic import (
        ArrayObject,
        DictionaryObject,
        NameObject,
        NullObject,
        NumberObject,
        TextStringObject,
    )

    from groupagent.artifacts import ArtifactError
    from groupagent.artifacts.formats import open_pdf

    writer = PdfWriter()
    writer.add_blank_page(width=600, height=800)
    writer._root_object[NameObject("/OpenAction")] = ArrayObject(
        [
            writer.pages[0].indirect_reference,
            NameObject("/XYZ"),
            NullObject(),
            NullObject(),
            NumberObject(0),
        ]
    )
    output = io.BytesIO()
    writer.write(output)
    assert len(open_pdf(output.getvalue()).pages) == 1
    writer._root_object[NameObject("/OpenAction")] = DictionaryObject(
        {
            NameObject("/S"): NameObject("/JavaScript"),
            NameObject("/JS"): TextStringObject("app.alert('synthetic fixture');"),
        }
    )
    output = io.BytesIO()
    writer.write(output)
    with pytest.raises(ArtifactError) as error:
        open_pdf(output.getvalue())
    assert error.value.code == "unsupported_feature"
