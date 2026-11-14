# Scoped image, OCR and speech services

`MediaService(ArtifactStore, IsolatedMediaWorker, providers=..., budget=..., source_guard=...)`
accepts a trusted `ToolContext` and immutable `MediaSource`/`MediaRef` values.
It exposes `ingest_image`, `ingest_audio`, `ocr`, `analyze_images`, `transcribe`
and `synthesize`. `register_media_tools` registers these operations without
expanding the work's tool grant. The model cannot choose a role, credential,
host path, speech permission or provider price.

Image ingestion verifies the whole file and decodes real pixels under the worker
limit (15 MiB, 40 megapixels). JPEG, PNG, WebP and GIF are supported. Analysis
uses an EXIF-oriented PNG view, composites alpha on white, and marks GIF input
as **static first frame**. Original bytes, source message/member/quote IDs,
sequence and hashes remain associated. Image metadata is not an image input to
vision: the provider receives the actual normalized pixels.

Audio ingestion accepts supported audio-only containers up to 25 MiB/600 seconds,
checks streams, fully decodes and normalizes to mono 16 kHz PCM. Long audio is
split into at most six bounded ASR requests. A video declaration, filename/MIME
or video stream is refused; there is no video download, frame extraction or
video-audio extraction tool. ASR text is untrusted. Provider confidence is never
invented; missing/low confidence marks matched member names, numbers and dates
for clarification. TTS requires trusted explicit speech intent, uses reviewed
preset voices, and validates returned PCM by producing and decoding a real WAV.
PCM tone fixtures establish decoding only, not semantic recognition accuracy.

Local OCR runs Tesseract with English and simplified Chinese data. PDF extraction
retains page numbers and uses Poppler plus OCR for scanned pages. OCR word boxes
include pixel coordinates; supported uncropped PDF pages also have point
coordinates. Explicit page ranges and incomplete extraction remain visible.
Text extraction and OCR do not establish arbitrary visual fidelity.

## Provider routes and monetary accounting

Official schemas were checked on 2026-11-14. Providers are constructed with an
explicit secret and an optional injected `httpx.AsyncClient`; construction never
reads an environment file or starts a request. Unknown model/capability pairs,
redirects, oversized responses and incomplete provider results are rejected.

| Capability | Reviewed DeepSeek model or endpoint label | API | Billable units |
| --- | --- | --- | --- |
| Vision | `deepseek-flash` | `/chat/completions` | `input_tokens`, `output_tokens` |
| ASR | `tools/asr` | `/v1/tools/asr` | `audio_seconds` |
| TTS | `tools/tts` | `/v1/tools/tts` | `characters` |
| OCR | `tools/ocr` | `/v1/tools/ocr` | `pages` |

The tools labels identify endpoint contracts, not undocumented underlying speech
models. References: [vision](https://api-docs.deepseek.com/guides/vision/),
[ASR](https://api-docs.deepseek.com/guides/asr/),
[TTS](https://api-docs.deepseek.com/guides/tts/),
[OCR](https://api-docs.deepseek.com/guides/ocr/).

Every paid operation needs an explicit `ProviderRoute.price_id` and durable
budget ledger. Operations are `media.vision`, `media.asr`, `media.tts` and
`media.ocr`. A new reservation and successful `begin` must precede HTTP.
Success settles reported usage and saves the receipt ID. Unknown outcomes keep
the reservation; replay cannot silently call again. Fixture budgets are accepted
only with an explicit mock HTTP transport. There is no implicit free production
mode or inference that a text API supports all modalities.

The user excluded independent illustration generation from this release on
2026-11-14. The existing optional OpenAI image adapter is disabled extension code,
not a release capability or acceptance gate. The default catalog excludes it and
`image_generate` is not registered without an explicitly configured extension
route. Programmatic charts remain supported by the document worker.

## Channel input and verification

The trusted `runtime.inputs.InputProcessor` consumes already persisted channel
events. It downloads through scoped attachment handles, caches validated bytes,
and records original-audio `observe` plus a deterministic derived `QUOTE`
atomically. The derived event preserves the original sender and timestamp and
replies to the audio event. `ga_media_inputs` records source fingerprints,
claims, at most two ASR attempts and receipts. The narrowly scoped input budget
binds charges to the original `source_event_id`, without inventing a work order.
Accepted bot receipts are checked before ASR. The runtime must also gate
unreconciled same-account sends before preprocessing.

`import_for_work` accepts only canonical `attachment_event_ids` and
`input_versions`, rechecks the current execution lease/source fingerprints
before and after I/O, and copies authorized bytes into an independent work
scope. Cross-work versions require their transitive source dependencies in the
new work's authorization snapshot. Forget/expiry invalidation blocks cache reuse;
a source tombstone must also accompany physical purge.

Evidence is separated by layer. Provider contracts use `httpx.MockTransport`.
Real Docker tests exercise pixel/audio decoding, English/Chinese OCR, scanned
PDF page attribution, and static Chromium rendering with network disabled.
`tests/runtime/test_inputs.py` has 16 passing local PostgreSQL cases, including a
real Docker decoder plus mock ASR, concurrent claims, source withdrawal,
crash/retry accounting, receipt echoes and immutable scoped copies (2026-11-14).
They do not establish live WeChat delivery or noisy speech accuracy.

`media.acceptance.make_image_cases()` creates 30 varied gold-scored synthetic
images. `voice_cases()` defines 20 original task sentences;
`run_live_acceptance(service, context, *, vision_route, tts_route, asr_route,
run_id, ...)` is an explicit opt-in harness with per-case evidence and receipts.
It makes no calls on import. The parent acceptance run passed 30/30 images and
20/20 synthesized-speech cases against the authorized shared ledger; synthesized
speech is not representative of noisy human recordings. Its image-generation
status is `NOT_APPLICABLE_user_scope_change`.
