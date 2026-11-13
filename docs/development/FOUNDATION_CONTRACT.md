# Foundation implementation coordination

Implementation is now authorized by the user. Current work builds and verifies an isolated foundation and vertical slice; production deployment still requires the spec gates. Video is out of scope. No legacy credentials, group database or live messages may be used as fixtures.

Shared implementation choices for this batch:

- Python >=3.11, src/groupagent package, uv lock, pytest/ruff/mypy.
- PostgreSQL 16+ for actual persistence and integration tests. No SQLite production substitute.
- `domain.py` is owned by the persistence agent. It defines `DomainError(code, message)`, `EventKind`, `Role`, `GroupEvent`, `ActorContext`, work/delivery enums and models as needed.
- `GroupEvent` fields: id, source_key, group_id, actor_id, kind, text (default empty), occurred_at, received_at, reply_to (optional), attachments (list, default empty). IDs remain strings; datetimes must be aware. Additional provenance fields may be added without breaking constructors.
- `ActorContext`: group_id, member_id, role, policy_revision (default 1). Caller-facing adapters derive it from trusted identity/configuration; a model cannot choose a role.
- Package boundaries: parent owns config/coordinator/CLI/CI/integration; persistence agent owns domain and persistence; engine agent owns execution and tools; artifact agent owns artifacts and documents. Agents must not switch branches, commit, modify pyproject, or overwrite each other's paths. Report public interfaces promptly.
- Tests use separate files/subdirectories per owner. Shared dependencies are added by parent only.
- Deterministic demo/model fakes must be explicitly named; never claim them as real WeChat, live model, OS sandbox, or production acceptance.
- Engine and artifact implementations should be independently usable without a PostgreSQL connection. Parent composes them through the durable control layer.
- Actor, group, work and artifact scopes must be validated before side effects. Video is rejected before model/tool calls.

This document coordinates the current development batch, not a replacement for the product spec.
