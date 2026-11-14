# Local readiness implementation contract

Authorized 2026-11-14: implement and locally verify all pre-production functionality in Spec v0.1. Keep existing production unchanged. Video remains excluded. Independent illustration-generation APIs were also excluded by the user on 2026-11-14; image understanding, OCR and reproducible programmatic graphics remain required. Work on `feat/local-readiness` stacked on the reviewed foundation; no shared-history rewrite.

## Ownership and integration

- Parent: coordinator/conversation, runtime daemon and dispatch, monetary budgets, natural-language scheduling orchestration, operations/backup/migration, CLI/config/dependencies/CI and end-to-end acceptance. Own additive migrations 005_runtime.sql and 006_budget_ops.sql.
- Persistence agent: `memory/**`, members/identity/summary/retrieval/forget/retention, required TeamTask accessors in existing persistence, `tests/memory/**`, additive 004_memory.sql, and memory module documentation. Existing migrations001—003 never change. Coordinate any edits to existing persistence before the parent integrates.
- Execution agent: `execution/**`, `tools/**`, new `sandbox/**`, Harness runtime integration, guarded Python/Shell execution and its isolation validation; tests under execution/sandbox; deployment worker definitions. No application daemon, coordinator, config or pyproject edits. Changes to existing EnginePort must be compatible or communicated first.
- Artifacts agent: `artifacts/**`, `media/**`, `research/**`, media-provider capability registry and image-understanding/OCR/ASR/TTS adapters, reproducible programmatic graphics; document capability completion and controlled browser; tests in matching directories. No pyproject, coordinator or runtime daemon edits.

All agents use synthetic fixtures; never read legacy credentials or production data as test input. Parent alone updates dependencies, stages and publishes commits. No branch changes or commits by subagents. Exact source, package and license evidence must accompany external integrations. Do not invent upstream APIs or describe a deterministic model fixture as real semantic validation.

Draft new migrations use `.sql.draft` until syntax/constraints have been verified in a transaction that rolls back. Rename once stable; after any applied migration, amendments require another migration. Allocated migration numbers allow modules to progress independently; report freezes before shared test runs.

## Local completion target

1. One configurable service accepts synthetic/local and authenticated channel events, supervises durable execution, polls schedules, dispatches ordered validated outputs, reconciles unknown sends, and resumes after restart.
2. Natural participation, no-prefix self-assignment versus agent work, continuity, topic evidence, quiet/cooldown policies, reminders, updates and cancellation are exercised through the same coordinator.
3. Durable ID/name roster, evidence-backed facts/tasks, retrieval and compaction survive restart; correction/forget/retention invalidate derived memory and prevent resurrection.
4. Required media/document/research/computation abilities expose real code paths and scoped tools. Actual format/render/OCR/process boundaries are tested; provider and live-WeChat gates remain separately labeled.
5. Reserve/settle monetary budget before paid operations; no silent unlimited route. Operational diagnostics, replay, backup/restore, synthetic legacy migration, retention and single-sender control have local exercises.
6. An A01—A48 matrix records PASS/FAIL/BLOCKED/NOT_RUN by layer, with fixture denominators and evidence. Finish feasible local implementation, not merely extra interface stubs; external acceptance cannot be substituted by mocks.

## Integration conventions

- Reuse trusted `ActorContext`, `DomainError`, `PostgresStore`, `GroupEvent`, work and delivery state machines.
- Public module services accept `store`/trusted policy and explicit provider objects. Provider credentials are explicit secret arguments and never serialized to model state, worker requests or logs.
- Expensive provider calls and sandbox commands occur outside DB transactions. Every subsequent write rechecks policy/revision/lease.
- Service APIs should remain callable with injected clocks, HTTP transports and providers, enabling deterministic tests without a live account.
- Agents publish concrete public signatures early. Parent composes functions rather than copying private implementations.
