# Continuous execution context and checkpoint evidence

Date: 2026-11-16. Local base revision: `2d8c84e829d574fc793a31feb8333bffa3d6bd32`
on `feat/pot-2gb-server`; this note describes the working-tree helpers and synthetic offline
tests. It is not PostgreSQL, provider, 2 GB host, native Harness, or production acceptance.

## Model request projection

`execution/compaction.py:compact_model_request` measures the full serialized request,
including tool schemas, against the existing per-request byte limit. It makes no model calls.
System messages, every accepted user input, the latest complete tool batch, the latest
assistant state, and every incomplete or ambiguous assistant/tool exchange stay exact.
Older complete exchanges can become clearly labeled archives of quoted untrusted data.
Archives retain source/citation/evidence-limit fields, artifact/file/version references,
receipt status/error fields, bounded scalar facts and raw-message hashes. Omission is explicit;
an omitted source is never represented as fully read or as an authorization grant.

Only the outbound `ModelRequest` changes. Raw messages, all receipts, the idempotency ledger
and native session remain intact. The helper reports `model_context_too_large` and returns
the original request if protected inputs, schemas or essential retained metadata cannot fit.
Per-message evidence metadata can still accumulate; this is a bounded projection with an
explicit anomaly path, not a promise that infinitely many distinct facts fit in finite context.

Synthetic eight-result example with repeated Chinese text:

- Original outbound request: 434,876 UTF-8 JSON bytes.
- Compacted request: 6,644 bytes under an 18,000-byte request limit.
- Sixteen older messages archived; the current assistant/tool exchange is unchanged.

## Lossless checkpoint encoding

`runtime/checkpoint_codec.py` keeps legacy version-1 JSON bodies readable and writes small
checkpoints in that format. Bodies larger than 1 MiB use a versioned gzip/base64 envelope
with level-1 compression, fixed gzip timestamp, exact decoded byte length and SHA-256.
The decoder verifies strict base64, one complete gzip stream, no trailing stream/data,
exact length/hash, the checkpoint model and a matching status header. It inflates in
64 KiB output chunks with an expected-length-plus-one check; a huge declared length does
not cause eager allocation of that declared size. Corruption raises `CheckpointCodecError`;
the display header cannot be used as recovery or authorization input.

The small header carries actual state/error/usage/sequence/safety fields and up to eight
recent failure receipt metadata records. Every complete receipt/output, ledger entry,
message, pending input and native session remains in the compressed payload.
Same-sequence CAS must compare decoded checkpoint semantics across legacy/envelope formats.

The synthetic codec fixture is 9,049,362 decoded bytes, exceeding the former 8 MiB check.
Its encoded envelope is 62,436 bytes and round-trips exactly. This deliberately repetitive
fixture demonstrates preservation and the storage threshold boundary, not a representative
compression ratio for every tool output.

Compression reduces stored/wire payload; decoded Pydantic state and raw execution history
still grow resident. Entropy-rich outputs may compress poorly, and serialization/compression
also allocate memory. The existing serial heavy-worker/resource limits remain necessary.
A future genuinely bounded resident-state design needs append-only/paged receipt and raw
message storage with small checkpoint references, preserving recovery fingerprints and
source invalidation. That is a separate storage/schema change. The native Harness session
archive's independent 8 MiB compressed/expanded limits in `harness_worker.py` are unchanged.

## Verification

Focused verification: 55 synthetic offline tests passed across
`tests/execution/test_history_compaction.py` and `tests/runtime/test_checkpoint_codec.py`.
These cover exact byte boundaries, Unicode, unchanged authority/objectives, pending tool
closure, ambiguous history preservation, source-unavailable truth and citations, artifact
versions, deterministic projection/encoding, lossless receipts/ledger/session recovery,
strict corrupt-input rejection, entropy-rich multi-chunk decompression, the exact 1 MiB
encoding boundary and status-header forgery rejection. Ruff and module mypy
checks passed. Runtime persistence integration is owned separately; no database or server
was contacted for this evidence.
