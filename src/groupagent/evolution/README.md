# Pot strategy development

Set `runtime.evolution_enabled=true` for the selected test deployment. Pot exposes
`strategy_list` to direct member and owner work. The scoped tool catalog's
`evolution` capability reveals the other tools. An owner can sponsor a candidate
with `strategy_propose` and load it for a dry run with `strategy_trial` in the same
work generation. `strategy_use` loads active guidance and binds its evidence to
the consuming work. Guidance inherits the work's existing goal and permissions.

Each version records the group, work generation, proposing identity, source
fingerprints, content digest, and review state. PostgreSQL serializes review and
activation with source withdrawal. Expiry, recall, and forget remove affected
proposal bodies and active pointers. Existing work provenance then governs any
downstream results. Review and promotion are operator commands bound to a
configured maintainer identity distinct from the proposer.

## Strategy review

Run `python -m groupagent.evolution --config <test-config> --operator <maintainer>`
with `GROUPAGENT_EVOLUTION_DSN` set in the operator environment. Supply one of the
following subcommands.

| Subcommand | Purpose |
| --- | --- |
| `strategy-export <id> <private-output.json>` | Export the exact version and guide for independent review. |
| `strategy-record <review.json>` | Save an immutable evaluation for the exported version. |
| `strategy-activate <id> --expected-active none` | Select the evaluated version for group use. |
| `strategy-activate <prior-id> --expected-active <current-id>` | Roll back to a previously evaluated version with available sources. |
| `deactivate strategy <current-id>` | Clear the active pointer. |

The independent reviewer checks the guide's scope, runs it against a separate
synthetic or authorized scenario, and records the fixture digest and each
observed result. `review.json` uses `version_id`, `body_sha256`, `suite_sha256`, and
`checks` (a boolean map with `scope` and `behavior`). Keep the fixture and observed
evidence in the private review directory. Strategy evaluation is an operator
attestation. Every check must pass before the separate activation command.

## Isolated code experiments

Set `runtime.evolution_manifest` to the canonical absolute path of an
operator-prepared manifest. Place only reviewed development Python source in its
sibling `source/` directory, preserving the `src/<package>/...` paths. The
manifest declares a short `name`, a `files` map of path to SHA-256, and a list of
`editable` paths. Use a null hash to authorize the creation of a specific new
Python file. Keep runtime config, keys, operational records, evaluation suites,
and previous review output out of the source directory.

Pot can inspect the designated editable files and submit bounded full-file
replacements, additions, or deletions with preimage hashes. Protected source
paths and evaluation fixtures are outside the model's edit surface. The
development operator provides a separate evaluation suite with a Python
`module`, `function`, and cases containing `name`, one JSON `argument`,
`baseline_expected`, and `expected`. Choose a testable function whose dependencies
exist in the curated snapshot or pinned worker image.

| Subcommand | Purpose |
| --- | --- |
| `code-export <id> <private-proposal.json>` | Export the reviewed proposal for an offline evaluator. |
| `code-evaluate <manifest> <suite> <new-dir> --version-id <id> --image <immutable-image> --docker-host <unix-socket>` | Compile and probe the baseline and candidate in isolated workers, write the source archive, diff, and report, and record evidence in the development registry. |
| `code-activate <id> --artifact <evaluated-dir> --expected-active none` | Select the verified archive as the development version. |
| `code-activate <prior-id> --artifact <prior-dir> --expected-active <current-id>` | Restore a previously verified development artifact. |

For offline evaluation, use `--bundle <private-proposal.json>` in place of
`--version-id`. This produces standalone results. The trusted registry route
records evidence directly from the evaluator. The worker sees the relevant
source and each test input, while the external oracle retains expected values
and constructs the report. The report binds the proposal, baseline, candidate,
suite and pinned image. A selected archive is available for operator
experimentation in a development checkout. The repository release procedure
governs separate deployment.
