# Artifact and document foundation

Status: isolated implementation and synthetic local experiments, 2026-11-13. This is not production acceptance or a claim that the complete FILE-002 matrix is implemented. No real group files, member identifiers or credentials were used. Video tools are absent; video extensions return `unsupported_video` before document generation.

## Public interface

All public types are exported from `groupagent.artifacts`.

```python
scope = ArtifactScope(group_id="synthetic-group", work_id="synthetic-work", actor_id="member")
with ArtifactStore(storage_root) as store:
    documents = DocumentService(store)
    report = Report(
        title="Course Project Report",
        sections=(
            Section("Purpose", ("Compare two study methods.",)),
            Section("Findings", ("The study included 24 students.",), (
                TableData(("Measure", "Result"), (("Sample count", "24"),)),
            )),
        ),
    )
    original = documents.generate(scope, "course.docx", report)
    revised = documents.edit_section(
        scope, original.file_id, expected_version_id=original.version_id,
        section_index=1, paragraphs=("24 students participated.",),
    )
    validation = documents.validate(
        scope, revised.file_id, revised.version_id,
        expected_text=("24 students", "Sample count"), render=True,
    )
```

`Deck(title, slides)` contains `Slide(title, bullets, table=None)` values. Each supplied Slide produces exactly one slide; the deck title appears in its footer. Report/section/table collections are tuples. Section and slide edit indices are zero-based; read locators (`page:1`, `slide:1`, `block:1`) are one-based.

Store methods are `create`, `revise(expected_version_id=...)`, `get`, `read`, scoped `list(name_contains=...)`, append-only `record_validation`, `validations`, and `read_preview`. Document methods are `generate`, `read`, `search`, `edit_section`, `edit_slide`, explicit `rebuild_pdf`, `validate`, and optional trusted `attest_visual_review`. Errors expose a stable `.code`, including `scope_denied`, `version_conflict`, `integrity_mismatch`, `unsupported_feature`, `encrypted_document`, `renderer_unavailable` and `render_timeout`.

The caller derives ArtifactScope from trusted actor/work authorization. It is not an authorization token and must not be constructed from model-chosen identities. This adapter enforces exact group and work equality; it deliberately has no cross-work sharing shortcut. The control layer remains responsible for roles, membership changes, retention, budgets and outbox state.

## Storage semantics

Each file has an opaque internal ID, immutable metadata and immutable version directories. Version metadata records the group, work, actor, timestamp, original filename, parent version, SHA-256 and byte count. Same-name creation produces another file ID. Current-version publication uses a staging directory, fsync, rename and an atomic pointer replacement under a POSIX file lock. Two writers with the same expected version cannot both advance current. A failed pointer publication leaves the prior current usable; an orphaned new directory can remain for future recovery or retention tooling.

Payload paths are internal. Caller names cannot contain path components, and references must be internal hex IDs. Traversal below the configured root uses directory descriptors and `O_NOFOLLOW`; payload reads reject symlinks, nonregular files and hardlinks, and verify the stored size/hash. The configured storage root is trusted deployment configuration; it must not itself be a symlink. The worker process owner can still alter its own storage, and a root administrator is outside this adapter's threat model. This is an integrity/access adapter, not an OS sandbox or a replacement for worker isolation.

Validation records and preview bytes are append-only, scope checked, and bound to the exact artifact hash. A later edit does not inherit validation. Previews are accessed through the same scoped API and checked against their stored hashes. No database is used inside this adapter; PostgreSQL control-state integration belongs to the parent coordinator.

## Implemented document range

| Format | Implemented here | Explicit boundary |
| --- | --- | --- |
| MD | UTF-8 read, block search, report creation, replacing a numbered section's plain paragraphs while retaining pipe tables | Complex Markdown section structures reject the edit; arbitrary encoding/fidelity is not promised |
| DOCX | Read ordinary paragraphs/tables, create styled report, replace Heading 1 section paragraphs and retain table XML | Tracked edits, embedded objects, drawings inside edited paragraphs, nested headings and other advanced structures reject the edit; no image/template authoring API yet |
| PDF | Real ReportLab output, pypdf text/page extraction, PNG previews, explicit complete rebuilding into a new version | No arbitrary in-place rewrite, OCR, form editing or page-composition API yet; encrypted PDFs reject |
| PPTX | Real python-pptx slides, body text and tables, slide text search, targeted title/body replacement preserving other slides and tables | Editing is limited to this generated deck layout; arbitrary charts, animations, images and templates are not implemented |

Office ZIP checks reject duplicate or unsafe member paths, encryption, excessive entry count/expansion, custom XML entities, macros and embedded binaries. Normal Office printer-settings binaries are allowed. Rendering refuses external Office relationships instead of fetching them. No document instructions are executed. PDF extraction does not supply OCR; pages without extractable text return a warning.

Limits currently include 30 MiB per stored document, 120 MiB Office expansion, 4096 ZIP entries, 200 PDF pages, 100 slides, bounded report/deck input shapes, 50 MiB converted PDF, 10 MiB per preview and 100 MiB combined previews. Rendering has a per-subprocess timeout and separate temporary LibreOffice profile. This does not implement cgroup/process memory limits, network isolation, a job supervisor or all spec workload/admission limits.

## Validation and evidence

`ValidationRecord` reports `structural_passed`, `content_passed`, `rendered`, `layout_passed`, `visual_reviewed`, `checks`, `issues`, `limitations`, preview hashes and `passed`/`status` separately. Empty completion assertions never imply completed content. Markdown can pass after structural/hash and explicit text assertions. Office/PDF require actual conversion/rasterization plus successful supported-layout checks before automatic `passed=True`; structural-only success is never enough.

Actual rendering checks PDF readability, caller-specified text retention, per-page word bounds and substantial text-box overlap, nonblank page rasters and preview/page counts. Generated PPTX also checks shape bounds/overlap and conservative text-frame/table-cell capacity. Automatic layout approval is scoped to the report/deck template marker and these checks. It does not prove arbitrary fidelity, detect all missing glyphs, interpret images, or judge a report's claims. Complex or unknown layouts return `layout_policy_unsupported`. Optional visual-review attestation records a reviewer and note without overwriting prior validation; it is not a blanket human gate or a model tool.

Local manual experiment used bundled Python packages: python-docx 1.2.0, python-pptx 1.0.2, pypdf 6.10.0, ReportLab 4.4.9 and Pillow 13.0.0. These describe that one experiment, not the project's pinned dependency resolution. LibreOfficeDev was `26.8.0.0.alpha0`, revision `2c87e51eeaa2b413ff4ae097b2705eea1995d8e5`; Poppler pdftotext was `26.05.0`.

The synthetic classroom fixture generated/read MD, DOCX, PDF and PPTX. Actual DOCX and PDF rendered as one page each; PPTX rendered as three slides. Text assertions included the findings, `87 percent` table result and next steps. All five final representative pages were opened as PNGs. A DOCX title-style border was removed after visual inspection, table padding was improved, and the revised output was rendered and inspected again. The documents skill's `render_docx.py` also rendered the revised fixture. These are synthetic local checks, not a real group/client delivery test.

A separate Chinese fixture exposed a real failure in the default host font setup: CJK glyphs were absent and validation failed. The fix is explicit deployment font configuration, not weaker validation. `DocumentRenderer(font_directories=(...))` creates a temporary Fontconfig configuration/cache using existing font directories; `DocumentService(office_font_family=..., pdf_font_path=...)` selects an explicit Office family and embeds a configured TrueType subset into PDF. The local run used the existing macOS `Arial Unicode MS` family and `/System/Library/Fonts/Supplemental/Arial Unicode.ttf`; no proprietary font files were copied into the repository or installed globally. Chinese DOCX, PDF and PPTX then passed the same text, geometry and raster checks, and their four page previews were visually inspected. A deployment must configure fonts that cover its intended languages; bare default CJK rendering is not certified by this experiment.

Final project-venv verification on 2026-11-13: **45 artifact tests passed**, including eight real-render tests (English report/deck, edited report, three Chinese formats and a deliberately overflowing slide that must fail validation). Ruff lint/format and mypy cover the six artifact source modules. The project run used python-docx 1.2.0, python-pptx 1.0.2, pypdf 6.21.4, ReportLab 4.5.1, Pillow 12.3.0 and pytest 9.2.0. Unit tests alone skip the optional render suite; they must not be presented as rendering evidence.

Tests under `tests/artifacts` cover scope rejection, concurrent expected-version conflicts, failed-pointer publication, old-version preservation, traversal/symlink/hardlink attempts, hash changes, append-only validation, all format parsers and scoped edits, corrupted/encrypted/unsafe Office packages and video rejection. The optional render suite exercises actual external tools:

```sh
.venv/bin/pytest tests/artifacts
GROUPAGENT_RENDER_TESTS=1 GROUPAGENT_PDFTOTEXT=/absolute/path/to/pdftotext \
  .venv/bin/pytest tests/artifacts -m render
```

The Chinese render tests also require `GROUPAGENT_FONT_DIRS` (platform path separator), `GROUPAGENT_PDF_FONT` and `GROUPAGENT_OFFICE_FONT`. These are test settings; production callers pass trusted configuration to the constructors instead of accepting paths from a model.

`DocumentRenderer` resolves `soffice`, `pdftoppm` and `pdftotext` from PATH or accepts explicitly configured executable paths. There are no user-specific executable paths in product code. The local bundled `pdftotext` was not on PATH and was provided through constructor/test configuration.

## Upstream references

Accessed 2026-11-13. Library behavior was checked against primary documentation; automated and manual experiments above establish only the locally observed subset.

- [python-docx 1.2.0 quickstart](https://python-docx.readthedocs.io/en/latest/user/quickstart.html): creation of real headings, paragraphs and tables.
- [python-docx API concepts](https://python-docx.readthedocs.io/en/latest/user/api-concepts.html): block-level document structure.
- [python-pptx text API](https://python-pptx.readthedocs.io/en/stable/api/text.html): text-frame replacement and formatting properties; replacement discards the replaced frame's original paragraph/run formatting.
- [python-pptx tables](https://python-pptx.readthedocs.io/en/latest/api/table.html): grid/cell access used to retain the separate table during targeted text changes.
- [pypdf text extraction documentation](https://github.com/py-pdf/pypdf/blob/main/docs/user/extract-text.md): PDF text has no reliable semantic layer and pypdf does not perform OCR.
- [Fontconfig configuration reference](https://www.freedesktop.org/software/fontconfig/fontconfig-user.html): configured font/cache directories and `FONTCONFIG_FILE`/`FONTCONFIG_PATH` overrides.
- [ReportLab fonts](https://docs.reportlab.com/reportlab/userguide/ch3_fonts/): Unicode TrueType embedding through `TTFont` and `registerFont`.
