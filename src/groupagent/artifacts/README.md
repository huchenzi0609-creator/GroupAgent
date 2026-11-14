# Immutable files and bounded document operations

`ArtifactStore(root)` stores group/work-scoped immutable file versions with SHA-256,
version compare-and-swap, validation records and preview hashes. Filesystem
operations reject traversal, redirected directories, symlinks and unsafe links.
A caller cannot read another work by changing its scope. Trusted retention
handlers can `purge`, `purge_work` or `purge_event`; tombstones prevent resurrection.
The runtime's `ArtifactRegistry` authorizes explicit cross-work versions before
copying them into a new independent work scope and tracks source dependencies.

For untrusted input use `IsolatedDocumentService(store, worker, policy_guard=...)`.
It stages only declared input bytes in the shared Docker worker, runs parsing and
rendering there, and rechecks scope/lease policy before publishing outputs.
`DocumentService` is the low-level parser/renderer used inside that worker and
trusted synthetic fixtures; it is not a host-side attachment sandbox.

Supported operations are real `.md`, `.docx`, `.pdf` and `.pptx` creation,
bounded reading/search, section/slide edits, exact text-target edits, template
placeholder replacement, inline-picture extraction/replacement, native PPTX
bar/line/pie chart data edits, and PDF page selection/rotation/combination and
note/highlight annotations. `Report`, `Section`, `Deck`, `Slide`, `ImageData`,
`TableData`, `ChartData` and `ChartSeries` are the authoring inputs. `chart_render`
produces a deterministic validated PNG; it is not an illustration model.
Edits require the current immutable version and do not overwrite the source.
The twenty-first and later extracted picture retains the message sequence;
picture position lives in `image_locator`, not the bounded message sequence.

Office parsing rejects macros, active/external relationships, unsafe archive
paths, unsupported revisions and embedded video. PDF parsing rejects encryption,
JavaScript/Launch actions and embedded media. Passive page-view destinations
from LibreOffice are permitted. Editing signed/interactive PDFs is refused;
annotation coordinates currently require uncropped pages with a zero origin.
Image extraction and OCR keep their parent source/version association.

`ValidationRecord.passed` is separate from structural parsing. Office/PDF delivery
requires actual rendering, expected-content checks, nonblank raster previews,
page text bounds and overlap checks, and the supported layout profile. Slides
also check shape bounds, text capacity and later opaque objects hiding text.
Ordinary single-column Word templates and simple unrotated native slide shapes
are supported without a private marker. Unsupported complex layouts remain
unvalidated; there is no claim of arbitrary round-trip fidelity, semantic visual
review, exact font-glyph equivalence or accessibility certification. Human visual
attestation is optional, not a blanket requirement for automatic delivery.

The structural matrix contains ten representative cases per format (40 total),
including CJK, tables, inline images, bar/line/pie charts, mixed layouts and a
10-slide deck. The isolated matrix validates these via real LibreOffice/Poppler;
negative tests cover malformed/encrypted/active files, archive escape, video,
version/scope errors, text overflow, unsupported layouts and image occlusion.
Separate template tests exercise real placeholder edits and rendering without
GroupAgent markers. Structural success alone is never reported as rendered
success. See `tests/artifacts` and the historical foundation evidence in
`docs/development/ARTIFACT_POC.md`.
