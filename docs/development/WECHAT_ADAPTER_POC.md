# AgentWechat adapter foundation

Status: implemented and verified with synthetic HTTP transport fixtures on 2026-11-14. No actual account credentials, private group data, WeChat connection, live send, read-state update or production change was used. This is a bounded HTTP adapter implementation, not E1 live acceptance, SDK-runtime acceptance or OS-worker isolation evidence.

## Reviewed upstream contract

The GitHub `v2.2.1` tag resolved to commit **`811d090770dd7b682b5bf69d54fc6fbd522cbe9b`**. Its generated TypeScript types and Rust HTTP implementation were read directly, alongside the public documentation. Access dates were 2026-11-13 and 2026-11-14, Asia/Shanghai. The sources of implementation behavior are pinned:

- [Tag reference](https://api.github.com/repos/thisnick/agent-wechat/git/ref/tags/v2.2.1).
- [Generated HTTP contracts](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/shared/src/native.generated.ts): message, quote, event, roster, media and receipt shapes.
- [Rust contracts](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/contracts.rs): field semantics, including string server IDs, cached roster membership and acceptance states.
- [HTTP routes](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/http_route_table.rs) and [HTTP handlers](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/lib.rs): JSON event pages, decimal cursors, explicit consumer acknowledgments and API errors.
- [Capability and key-lookup handlers](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/actions.rs): per-kind upload/send limits and lookup by idempotency key.
- [Media handlers](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-api/src/media.rs) and [media contracts](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/media_types.rs): metadata, transfer jobs, cached content, staging and media sends.
- [Event storage](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-store/src/operations.rs), [media events](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-store/src/media.rs), and [recall events](https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-store/src/recalls.rs): event retention, control notifications and recall notifications that can precede a local message row.

The moving [HTTP API guide](https://thisnick.github.io/agent-wechat/use/api/), [media guide](https://thisnick.github.io/agent-wechat/use/media/) and [send-recovery guide](https://thisnick.github.io/agent-wechat/how-it-works/sends/) were supporting orientation only. Later documentation is not evidence that a deployed v2.2.1 instance passed a feature. Upstream source was inspected, not vendored or executed.

## Public API and configuration

All public types are exported from `groupagent.channels`. Credentials are a separate explicit constructor argument, held as `SecretStr`. There is no environment-token discovery, login/profile lookup or credential file access.

```python
config = AgentWechatConfig(
    base_url="https://channel.example.invalid",
    account_id="configured-account",
    external_group_id="configured-room@chatroom",
    internal_group_id="configured-group",
    source_epoch="deployment-managed-profile-epoch",
    expected_version="2.2.1",
    expected_daemon_server_id="previously-recorded-daemon-storage-id",
)
adapter = AgentWechatAdapter(config, token=explicit_token)
```

`expected_daemon_server_id` is optional for initial inspection; persist the reported identity and supply it when resuming a durable deployment. It is the daemon data-directory identity, not the daemon process generation. `source_epoch` must change deliberately after account/profile replacement, re-login migration that remaps local IDs, or another source reset. Do not derive it from the roster's `sourceGeneration`: that is a mutable profile-view fingerprint.

The default client verifies TLS, ignores ambient proxy configuration, disables redirects, and sets time/response-size bounds. HTTP is allowed only for loopback endpoints. Base URLs cannot include credentials, a query, a fragment or a path. A trusted caller may inject `httpx.AsyncClient` for test transport or deployment wiring; it owns that client's closure and security configuration.

Methods are asynchronous:

| Method | Result and scope |
| --- | --- |
| `capabilities()` | `ChannelCapabilities`; checks reported product/API/version and runtime limits; no claim of live acceptance |
| `consume(after_cursor=None, limit=100)` | `EventBatch`; each `entries` item has `event` and its bound `cursor`; `events` is a convenience property |
| `ack_committed(cursor)` | Explicit upstream acknowledgment after the caller durably commits; never marks a message read |
| `read_message(message_id)` | Normalized `GroupEvent` after account/group/message identity checks |
| `attachment_handles(message_id)` | Current metadata handles for supported attachments; no content download |
| `fetch_attachment(handle)` | Bounded cached bytes after fresh source/asset/scope/hash checks |
| `request_attachment_download(handle)` | Explicit supported pending-media transfer; returns a job receipt, without polling or retry |
| `members(cursor=None, limit=100)` / `resolve_member(member_id)` | Cached roster records containing only the member ID and `roomDisplayName` as the name |
| `send_text(group_id, text, idempotency_key)` | A typed send receipt; the group must match configuration |
| `send_file(group_id, name, content, idempotency_key)` | Bounded MD/DOCX/PDF/PPTX staging and send; upload name, size and SHA-256 are verified |
| `lookup_delivery(group_id, key)` | Existing typed receipt, or `None` only for an explicit API `not_found` response |

The constructor does not create an authenticated session by itself. Public documentation calls during development did not use this adapter against any live account.

## Intake, identity and cursors

Account/group configuration is trusted control-layer data. The adapter rejects returned messages, assets, member lookups and send receipts whose identities disagree with that scope. Account event pages contain metadata for multiple chats; a foreign thread's `message.committed` entry is skipped before its message body is fetched. Private chat intake is not enabled.

Sender identity comes only from the authenticated message's `senderId`; payload `role`, names, text and quotes cannot create an `ActorContext` or grant authority. The coordinator must derive roles from its own configuration. `roomDisplayName` is the sole roster name exposed. `displayName`, private remarks, nicknames and alleged roles are discarded. Missing roster rows are `unconfirmed`, not proof that a member left.

Message `source_key` uses account/group plus the platform's server ID where available. IDs remain strings, including values larger than JavaScript's safe integer range. The normalized event UUID is deterministic from that key. Only a missing server ID uses local message ID plus the explicit source epoch. Recall notifications use a separate event key and reference the original event; their actor is a system identity because a recall notification does not prove who performed the action.

`ChannelCursor` binds account, internal/external group, source epoch, daemon storage identity and upstream decimal cursor. `serialize()` is suitable for the durable control layer's cursor string. Cursor regression, inconsistent page endpoints, an expired cursor, or a changed storage identity fail explicitly. Known account control events are reported in `batch.skipped`; unknown kinds fail instead of silently advancing. Recall events can be represented from server metadata even when no local message row exists.

The adapter never acknowledges automatically. Persist admitted events and the corresponding cursor before calling `ack_committed`. Persist the batch's final cursor separately when it advances over only skipped control/foreign-group metadata; there may be no new group message to insert. Consumer names and send keys are namespaced deterministically by group, avoiding collisions between two adapters for the same account. Acknowledgments are unrelated to WeChat read state.

## Attachments and video boundary

Ordinary text, images, voice messages, supported files, embedded quote excerpts, simple forwarded-chat text, link cards and system/recall notices have explicit normalization. An authenticated message with an unsupported numeric type/app subtype becomes a metadata-only `SYSTEM` event with an `unsupported_content` marker, its stable sender/message IDs and the numeric discriminants. Its body, XML, location coordinates, payment amounts and arbitrary card fields are discarded. This allows durable observation and cursor progress without pretending to understand the content. Missing/invalid identity or timestamp fields, unknown event-stream kinds, invalid cursor ordering and malformed envelope schemas still fail closed. Unsupported content cannot acquire the synthetic system identity reserved for documented system notices.

An intake event stores an immutable message-level `AttachmentHandle` with `availability="unresolved"` and no asset ID. Mutable availability, checked hash and byte length are queried through `attachment_handles`; they do not alter the original event on replay. Cached fetch resolves the reference again, checks its platform message identity, verifies ownership, bounds the response and checks size/SHA-256. A previously issued hash cannot silently change. Metadata `contentUrl` and filesystem-looking fields are never used as request targets; the client constructs only pinned API routes on the configured origin.

Explicit uncached transfers require a known checked or advertised size within the local limit. A pending/expired/unsupported asset, unknown size or an unknown status is diagnostic, not an empty successful attachment. Transfer completion is not assumed; query fresh metadata or retain/poll through later control-layer integration. The daemon may independently cache voice notes under its own settings; this client does not enable or change those settings.

Video messages produce `unsupported_video` metadata without any media lookup. Video quotes keep the comment and unsupported status without retrieving video bytes. A video media kind, MIME type or recognized video filename encountered while resolving a file is rejected before content/download requests. There is no video send, decoder, frame extraction, audio extraction from video, model call or video-generation path. Malicious mislabeling of bytes still requires downstream file-format checks; declared metadata is not a general content-scanning guarantee.

## Sends and evidence

`ChannelReceipt.state` preserves the known upstream state; `delivery_state` maps it to the project's state model. `queued`, `submitting` and `running` are pending. `needs_review` is unknown. Only an `accepted` receipt carrying a stable server message ID is accepted. `recipient_read` is always `None`. `observed_at` is when this adapter saw the receipt, not a fabricated platform acceptance or reading timestamp.

A lost send response raises `delivery_unknown`, with the same stable key and no automatic retry. HTTP 202 alone does not imply acceptance. Unknown statuses, inconsistent receipt identities and foreign destinations fail. Native text sends preserve the same wire key across retries; changed content under the key is left to the upstream idempotency conflict check.

File staging is a separate effect. The adapter verifies the upload against the supplied bytes, then submits the owned source ID. Within one instance, identical file retries reconcile the same receipt and changed bytes/name conflict. After process restart, an existing key causes `delivery_requires_reconciliation`; the caller must use `lookup_delivery` with the durable immutable intent. The lookup contract does not expose enough source hashing to prove arbitrary newly supplied bytes equal that earlier send. An in-process lock does not replace the durable outbox's single-writer/fencing rules. Crashes before send submission can leave an unused staged source.

The control layer must establish artifact validation and authorization before sending. This adapter does not make a document validated merely because its extension is allowed.

## Verification and remaining gates

On 2026-11-14, **49 channel tests passed** using `httpx.MockTransport`; Ruff formatting/lint and mypy passed for the four channel source modules:

```sh
.venv/bin/pytest -q tests/channels
.venv/bin/ruff check src/groupagent/channels tests/channels
.venv/bin/ruff format --check src/groupagent/channels tests/channels
.venv/bin/mypy src/groupagent/channels
```

The fixtures test normalization, large stable IDs, epoch and storage-reset rules, quote/recall handling, immutable intake during media availability changes, missing identities, private-name/role exclusion, unknown types/statuses, foreign scopes, no video fetching, explicit download/ack behavior, hashes, response bounds, redirects, timeouts and file-send reconciliation. Fixtures contain synthetic values only. They verify the client contract and local safety decisions, not the daemon or WeChat implementation.

Unpassed release gates include real scoped-token/account access, account/profile migration and local-reference remapping, mixed/all-skipped batch cursor integration with PostgreSQL, complete control-event coverage, automatic wakeup/reconciliation of media jobs, media-format/client-display acceptance, native-send receipts under real disconnect/restart faults, echo attribution for shared accounts, live roster freshness, full WeChat message parity, real delivery and monitoring, and sandboxed worker execution. Attachment references from a replaced profile must be migrated or retired explicitly; changing an epoch must not silently relabel old local IDs.

Production remains unchanged. No old bot credentials, running data or production sender were inspected or reused.

## Image and voice sending extension (2026-11-14)

The pinned v2.2.1 transport now exposes:

```python
await adapter.send_media(
    group_id, name, content,
    kind="image",  # or "voice"
    idempotency_key=stable_key,
    duration_seconds=None,
    allow_file_fallback=False,
)
```

Image/voice availability and byte ceilings come from explicit runtime
capabilities, bounded by the application's own limits. Image sending does not
weaken `send_file`'s four-document-format allowlist. Voice requires validated
duration metadata; native voice has a 60-second ceiling. An unavailable native
voice capability or a longer validated clip can use an audio-file fallback only
when the trusted caller explicitly permits it. The returned receipt carries
`requested_kind`, `wire_kind` and `fallback_reason`; a file is never described as
a native voice bubble. No video kind or container is accepted.

Kind, selected wire mode, filename, duration and content hash participate in
idempotency. In-process concurrent sends stage once, restart lookups reconcile
existing keys, and uncertain sends are not retried blindly. Receipt lookup never
converts accepted into read. Real pixel/audio decoding remains the isolated
artifact-validation gate; channel magic/size checks are additional transport
checks, not a host parser substitute. Contract tests cover capabilities, limits,
explicit fallback, changed-content keys, concurrency and unknown receipts (64
channel contract cases passed). Actual WeChat recipient-client image/voice
acceptance remains an external gate without real channel credentials.

## Unsupported-content cursor recovery and link cards (2026-11-14)

The pinned generated contract explicitly supplies `NativeMessage.link` with a
`LinkCard` containing nullable `title` and `url` (plus description/sourceName,
which this adapter intentionally discards). Type 49, subtype 5 is normalized
from those two fields into a text link and a structured `kind=link` attachment
marked `untrusted_content=true`, `fetched=false`. The adapter never parses raw
message XML, guesses a URL from the body, follows the URL, or fetches a thumbnail.
Absent, malformed or unsupported card metadata produces a metadata-only event;
credentials in a URL, unsupported URL schemes and explicit video-file links are
not projected. A later public Fetch still needs its own URL/DNS authorization.

Source verification used the fixed generated contract and Rust contract linked
above, read again on 2026-11-14. The GitHub connector provided the pinned file
contents when the web fetch cache could not retrieve them. No moving-branch
schema or inferred XML format was used.

Validation: **93 channel tests passed**, including the existing HTTP contracts
and five real PostgreSQL intake tests. The added mixed batch contains text, a
location, a payment-card subtype, a documented link, a video and following text.
All six canonical events persist before ACK, the unsupported/system/video events
are observed without model requests, the final cursor persists, and restart
resumes from that cursor. Malformed identity in the middle of that same batch
still refuses the batch without ACK. These are synthetic transport/database
tests, not live WeChat acceptance.
