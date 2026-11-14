# Members and evidence-backed memory

This module implements the local ID/MEM data path over PostgreSQL migration 004. Identity/retention/replay tests use synthetic groups and actual PostgreSQL. HTTP contract tests use fixtures. A deterministic summarizer is explicitly named and never constitutes live semantic acceptance. Production WeChat membership events and model quality remain separate acceptance layers.

## Identity

`MemberService(store)` provides `get`, `list`, `resolve_name`, `resolve_unique`, `confirm_name`, `sync_startup`, `refresh`, and `apply_membership`. Stable member IDs own tasks and facts. Display names prefer the current room nickname, then an owner-confirmed exception, then `群昵称待补充`; personal WeChat names and private remarks are not roster inputs. Duplicate display names remain ambiguous and never transfer ownership.

`sync_startup(actor, run_id, provider)` fetches once per durable process-run key, including across retries or service-object restart. Failed or partial snapshots preserve previous members. Missing roster members are never inferred to have left, and cached presence cannot revive a reliably departed identity. `refresh(actor, request_id, provider)` requires an explicit owner request. `ChannelRosterProvider(adapter)` consumes bounded `members(cursor=..., limit=...)` pages and rejects a generation change, repeated cursor or foreign-group page.

`apply_membership(transport_actor, member_id, present, event_id, verified_channel=True)` accepts only authenticated membership-event evidence from a trusted owner/maintenance context. Replayed and older evidence cannot reverse a newer state. Reliable departure revokes group transactions for member/observer/owner identities, pauses unfinished work and schedules, and revokes execution leases. Maintenance retains only its pre-existing technical permissions. Cached roster data is never substituted for a reliable live departure event.

## Retrieval and explicit long-term records

`MemoryService(store, summarizer=None)` exposes `create_topic`, `link_event`, `topics_for_event`, `retrieve`, and `query_context`. Native quote or direct event evidence is required for topic links. A participant-restricted topic is checked both for direct reads and derived records; group summaries cannot absorb restricted sources. Recent context uses event time while literal keyword search reaches older retained evidence. Results distinguish raw events, facts, decisions, human tasks and lossy summaries, and carry author IDs, source IDs, times, validity and retrieval reasons.

`record_fact(actor, predicate, value, source_event_ids=..., explicit=True, kind='fact'|'decision', ...)` stores only an explicit statement with available same-group evidence. Personal facts default to the current stable member ID. `correct_fact(..., expected_revision=..., source_event_ids=..., explicit=True)` preserves revision history and invalidates affected summaries. `invalidate_fact` removes an old assertion from active retrieval. A summary never writes a fact or TeamTask automatically. Human tasks have independent `tx.list_team_tasks` / `tx.find_team_tasks` accessors and survive ordinary raw-message retention.

`update_team_task(..., due_at=<aware datetime>)` changes the due date; omitting `due_at` preserves it and explicit `None` clears it. Work revisions may atomically replace `source_fingerprints` and `retained_fact_source_ids`. Retaining an expired source requires an active, readable fact that actually cites it. Forgotten/recalled sources are never retained authority. Use `tx.event_fingerprint(event_id)` for the canonical SHA-256 format.

## Atomic provider context and compaction

`snapshot_in_tx(tx, query='', topic_id=None, limit=20, source_event_ids=None)` takes retrieval hits, `privacy_epoch`, and canonical source fingerprints in one short transaction under the memory lock. Pass the current input-event IDs explicitly. After the provider call, `assert_snapshot_in_tx(tx, snapshot)` rejects changed evidence, privacy epochs, actors or policy revisions. Ordinary new messages do not change the privacy epoch; corrections, invalidations, name changes and privacy operations do.

For coordinator mutation transactions, call `MemoryService.prepare_in_tx(tx)` **before** `tx.get_decision(...)`, then use `memory.in_tx(tx)` and `members.in_tx(tx)` to perform writes on that same connection. Bound services neither open nor commit another transaction. Provider calls through a bound service are rejected. The resulting business write, decision and reply can commit or roll back together.

`await compact(actor, topic_id=None, token_budget=4000, trigger_ratio=.75, force=False, ...)` selects a bounded source window according to context estimates and the configured batch limit. A durable lease permits one effective compactor. The provider runs outside database transactions. Only a complete, matching source-coverage result with a current lease/epoch can atomically write its summary and advance the checkpoint. Failure, cancellation, deletion, policy change and stale results leave sources unacknowledged. Invalidating a summary rewinds its checkpoint for reconstruction from still-available evidence. An individual message exceeding the permitted input budget returns a clear error rather than silently truncating and advancing.

`ModelPortSummarizer(metered_model)` accepts the application's existing metered `ModelPort` and reports actual token usage. It never loads keys or creates a separate billing path. `DeepSeekSummarizer(explicit_key, model=..., client=...)` is a real HTTP implementation for explicitly configured callers; its caller must reserve budget before invoking it. Its protocol follows the [DeepSeek JSON output](https://api-docs.deepseek.com/guides/json_mode/) and [chat completion](https://api-docs.deepseek.com/api/create-chat-completion/) contracts, accessed 2026-11-14. `DeterministicFixtureSummarizer` is only a plumbing fixture.

## Forget, recall, retention and cleanup

`forget(actor, event_ids=..., fact_ids=...)` checks source ownership or group-owner authority. Forgetting a fact also covers its owned source history; shared-source deletion requires owner authorization. `recall(maintenance_actor, recall_event_id)` requires authenticated native recall evidence and can create a tombstone before the original message arrives. `expire(maintenance_actor, before=..., now=...)` applies an already-authorized retention cutoff in bounded batches.

These transactions redact raw bodies, attachment snapshots, derived summaries, cached decisions and linked text outbox content. Explicit forget/recall also removes affected fact values/history and redacts/cancels derived WorkOrders, including RAG references in `scope.source_fingerprints`. Receipt IDs and accepted status remain as metadata: deletion does not claim to retract an already accepted platform send. Ordinary retention preserves explicit long-term facts and human task records with minimal evidence IDs.

Source-key, event-ID and platform-ID tombstones prevent message replay from restoring content. Quote descendants retain the quoting member's own body, while embedded original text and quote-image handles become `privacy_redacted` / unavailable. Their prior decisions, derived facts/summaries, work and artifacts are invalidated; their canonical fingerprints change even though their own body remains active.

`pending_cleanup(actor, now=...)` returns scoped event/work purge jobs and delayed backup-tombstone jobs. Artifact refs use `artifact:{file_id}:{version_id}`. `complete_cleanup(..., expected_revision=..., evidence_ref=...)` records actual adapter evidence and requires a trusted maintenance/owner identity. `source_available` / `source_guard` let media registries deny access immediately while physical purge runs. Backups retain their declared seven-day removal deadline; the module does not pretend all backup copies or external provider history disappear instantly. Backup restoration must carry forward newer deletion tombstones before admitting traffic.

### 人类任务字段来源（迁移 011）

`TeamTask` 额外提供 `created_by`、`content_state`、`field_sources`、`source_event_ids`、`provenance_kind`。创建时绑定原始 canonical event；`claim_team_task(..., source_event_id=...)` 和 `update_team_task(..., source_event_id=...)` 绑定本次真实作者事件。受信内部旧调用可以省略来源，但不能虚构原始事件或创建人。默认字段覆盖以新事件替换该字段来源；真实拼接时，受信调用方传 `append_source_fields={'progress'}` 等明确字段，保留贡献来源并集。忘记任一追加贡献消息会清整个受影响字段。

raw expiry 保留长期任务及最小来源 ID。显式删除 title 来源会 tombstone 整任务；progress/evidence/due 清空，owner 来源删除恢复 NULL/unassigned，status 来源删除恢复 open，不能继续声称已完成。列表及检索不返回 forgotten 任务。`forget(..., task_ids=[...])` 只允许真实创建者或 group owner；旧导入没有已知创建者时只允许 owner。绑定任务包含其他作者来源时，非 owner 仍不能借 task_ids 删除那些作者的原文。

没有 event 的旧任务标为 `legacy_unattributed`，普通受信无事件创建标为 `independent`，不会合成消息充当来源。MemoryHit/snapshot 的 `source_task_revisions` 用任务逻辑身份与 revision 补足这条依赖；服务应把实际展示的 map 持久化到 decision 和 Work.scope（修订 API 支持同名参数）。新快照与 Work admission 校验当前 revision；已接单工作和历史文档不因后来无关进展而自动失效。直接 Task 输入自动补齐其当前字段事件来源，经过验证的固定 input artifact 保留自己的来源链。部分字段删除按真实事件来源级联，避免误删更早且无关的 Task 版本产物；整个 Task 删除则按逻辑身份跨版本撤销。物理文件仍由可靠 cleanup/quiesce 完成。

外置删除日志支持独立 Task 墓碑；恢复早于该任务首次导入的归档时创建 ID-only tombstone，阻止 deterministic legacy import 复活。日志写盘先于 DB commit；没有同步 sink 时不能声称具有这项灾难恢复保证。
