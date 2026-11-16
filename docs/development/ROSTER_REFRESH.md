# Targeted room nickname refresh

Reviewed on 2026-11-16 against AgentWechat **2.2.1**, commit
`811d090770dd7b682b5bf69d54fc6fbd522cbe9b`. This is the adapter and identity
contract; offline tests do not establish production acceptance.

## Native API evidence

- [Targeted refresh route and read-scope admission](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/actions.rs#L282-L333):
  `POST /v1/accounts/{account}/groups/{room}/members/refresh` takes no request
  body or idempotency key and starts a `metadata` operation with roster version
  zero. This is distinct from the account-wide contact/group sync routes.
- [Native metadata work](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/metadata.rs#L864-L884):
  the operation requests the selected room contact and a native room roster,
  projects the result, then reports `memberCount`, `profileProjection` and
  `completeRoster`. [Projection](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/metadata.rs#L1120-L1227)
  carries `room_display_name` into the room member patch.
- [Operation polling](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/lib.rs#L654-L665)
  uses `GET /v1/accounts/{account}/operations/{id}`. The response binds account,
  operation ID and kind, but its metadata result does not repeat the room ID.
  The adapter therefore polls only an operation obtained from its configured
  room refresh request. [Native error projection](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/actions.rs#L38-L96)
  preserves authentication/identity errors but reduces other failures to
  `operation_failed`; this API alone cannot diagnose every native failure.
- [Cached roster reads](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/profile.rs#L220-L227)
  remain cache reads even after refresh. The upstream
  [reader](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-profile/src/reader.rs#L162-L176)
  permits remark/personal nickname fallbacks in `displayName` through its label,
  while `roomDisplayName` remains the room field. Only `roomDisplayName` is
  admitted by GroupAgent.
- [Account capabilities](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/actions.rs#L130-L144)
  advertise partial incoming group updates, but have no dedicated roster-refresh
  flag. `roster_refresh_supported` records reviewed pinned-contract support,
  not a live refresh or complete update coverage. A completed native roster
  projection emits a
  [scoped `group.updated`](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/metadata.rs#L638-L662).

## Application contract

`AgentWechatAdapter.refresh_members(internal_group_id, timeout_seconds=20)`
returns `MemberRefreshReceipt` only after the exact created operation succeeds
and reports a complete profile projection. Its `complete_roster` flag describes
roster coverage, never whether all names are available. Failed, cancelled,
unknown, inconsistent or timed-out operations do not return a success receipt.
Lost submission responses are reported as metadata uncertainty without an
automatic retry.

An in-process lock serializes refresh requests. A known operation that outlives
the caller's wait is retained and reused for 120 seconds; the receipt exposes
`reused`. After that window the adapter reconciles the old operation before
allowing another POST. An unresolved old operation blocks another submission.
This state is process-local and does not establish cross-restart deduplication.
The wait deadline bounds the adapter call; the native work has its own
[120-second metadata deadline](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/metadata.rs#L826-L835),
and a timed-out adapter call does not prove cancellation of the native RPC.

`EventBatch.roster_updates` contains only configured-room `group.updated`
cursors, without copying control payloads or another room's data. Runtime may
use these observations to synchronize the changed daemon cache, without another
remote refresh. A natural member/nickname query may first request a targeted
remote refresh with a shorter runtime deadline. These paths do not require a
periodic bulk roster job.

`MemberService.sync_runtime(..., reason="group_updated" | "member_query")`
requires the trusted `service:runtime` maintainer identity. It deduplicates a
stable source request/cursor through the existing roster-run store and checks
snapshot scope. Partial or missing cached names preserve an already known room
nickname. Cached roster absence never establishes departure, and null names
mean unavailable/unknown rather than proving that a user has not set a name.
Sender IDs and role authority remain independent of display names.

## Verification

- `tests/channels/test_roster_refresh.py` uses synthetic `httpx.MockTransport`
  responses for targeted completion, operation scope, failure/unknown states,
  bounded waiting, pending-operation reuse, unresolved-operation reconciliation,
  room-field privacy and scoped update cursors.
- 2026-11-16: **133 channel tests passed**, five PostgreSQL tests excluded, with
  `pytest -q -m 'not postgres' tests/channels`.
- **31 member tests passed** against the isolated worker PostgreSQL database,
  covering existing identity rules plus runtime authority, durable request
  deduplication, rename application, partial-null preservation and foreign scope.
- Ruff formatting/lint and mypy passed for the affected channel and member code.

The native source already exposes the required targeted operation; no upstream
patch is needed to initiate it. A remote success can still yield null room
names. Runtime and deployment verification must report the actual observed
outcome, without replacing nulls with personal names or asserting that every
member's room nickname has been retrieved.
