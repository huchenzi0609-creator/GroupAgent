# 本地运维与恢复边界

本模块实现受信控制面接口，不注册为模型工具。2026-11-14 使用 PostgreSQL 16.15、本机私有 Unix socket、合成事件和独立临时数据库演练；没有读取旧项目数据库、群数据、凭据，也没有连接真实微信或调用付费模型。迁移 `008_operations.sql` 已冻结，001—007、009—010 不由本模块改写。

后续 `011_task_provenance.sql` 经事务试执行及回滚后冻结。独立 Task 删除有单独外置墓碑，恢复时即使归档早于 Task 首次导入，也保留 ID-only tombstone 防止重新导入复活。含新 Task 删除知识的恢复必须使用支持该协议/schema 的构建；不支持的旧构建或旧 schema 保持隔离，不能忽略未知墓碑后启用。

`012_task_creator_authority.sql` 只修正早于 011 的旧记录创建者：有 `team_task_created` 审计才采用真实操作人，否则置 NULL，不能把引用消息的作者猜成业务记录创建者。011 以后由服务写入的可信 `created_by` 不受这条修复影响。

## 公共接口

- `BackupManager(store, root, pg_bin_dir=None, authorized_group_ids=None, retention_days=7)`：`create(actor)`、`verify(actor, backup_id)`、`restore(actor, backup_id, target_dsn)`、`expire(actor)`。仅 owner/maintainer 控制面可用，默认范围只含当前群；多群备份要求明确配置范围及 maintainer 身份。在线恢复目前要求单群，避免漏同步另一群删除日志。
- `RestorationService(target_store, authorized_group_ids=None)`：`status(actor)`、`maintenance(actor)`、`reconcile_delivery(...)`、`release(...)`。默认单群；多群解除隔离须显式授权所有群。
- `LegacyImporter(store).plan(actor, sqlite_path, legacy_group_id=...)` 只读 dry-run；`apply(actor, sqlite_path, plan_id, expected_source_hash=...)` 原子、幂等应用；`reviews(actor, plan_id)` 查看待确认项。路径只允许调用方明确提供的合成或另外授权源，本次测试均为合成数据库。
- `DeletionJournal(root)`：`append_tombstones(...)`、`sync(store, actor)`、`load(group_id)`。
- `MemoryService(store, summarizer=None, deletion_sink=journal)` 在 forget/recall/expiry 的数据库提交前同步写入外置删除日志；`in_tx` 保留同一 sink，不另开连接。
- `CleanupRunner(memory, handler, journal=journal, backups=backup_manager).run(technical)` 返回 `completed`、`failed`、`errors[job_id]` 和同步墓碑数量。任务回执必须非空；错误只返回机器码，不把异常中的原文或路径复制进审计。
- `RuntimeCleanupHandler(store, artifacts, runtime_root, technical=technical, channel=channel, is_busy=callback, quiesce=stop_writers, deletion_sink=journal, audit_days=90)` 位于 `runtime/cleanup.py`。`callback(work_ids, event_ids)` 为同步布尔，`stop_writers(work_ids, event_ids)` 为异步关闭握手；先验证工作已停止，再等待 supervisor/input，最后复查。存在活动 writer 时返回 `cleanup_busy`，任务继续 pending。

CleanupRunner 同时调用 handler 的 `expire_artifacts`：按 catalog 每个版本的 expires_at 物理删除到期 payload、校验和预览，保留尚未到期的兄弟版本；清理精确关联的本地投递副本并保留回执。结果含 `expired_artifact_versions` 和 `deleted_audit_events`。审计按配置期限分批删除（默认 90 天），不删除长期身份或 TeamTask。活动工作不会为文件到期被自动停掉，先保持不可读并报告 busy，待可靠停机握手后物理清理。

生产装配应让 MemoryService、RuntimeCleanupHandler、CleanupRunner 和 BackupManager 使用同一个 journal 实例。未配置备份管理器时，延迟的 backup_tombstone 任务不会被通用回调假确认。

## 备份、恢复、删除

备份是 PostgreSQL custom archive，不包含本地文档、worker image、微信会话或密钥。`pg_dump --snapshot` 与清单的表行数、排序 JSON SHA-256 使用同一 REPEATABLE READ 快照。清单还记录归档 SHA-256/大小、schema 校验和、构建引用、服务器及工具版本、7 天以内有效期。存在尚未完成的派生内容清理时拒绝产生新备份，避免重新延长已删除内容的保留期。

目录 0700、文件 0600；数据库密码通过临时私有 PGPASSFILE，不能出现在命令行或诊断输出。独立删除日志不放进可恢复归档。日志按 source key 保留最强墓碑（forgotten > recalled > expired），校验和损坏或不完整记录会阻止恢复。日志写入失败使数据库删除事务回滚；若写盘成功后数据库事务回滚，日志仍保留，恢复可能保守地多删除，绝不撤销已持久化删除指令。未注入 sink 的离线单元服务仍可使用，但没有同步外置删除保证。

恢复只允许独立空数据库，并拒绝任何用户表/视图/序列/函数/枚举、其他客户端和并发应用访问。恢复前创建不在归档中的 `groupagent_recovery.guard`；pg_restore 即使失败，应用仍受隔离保护。恢复后先验证表摘要，再覆盖比备份更新的删除墓碑，暂停 work/schedule/trigger、撤销执行尝试、将未确认投递及预算尝试转为 unknown、封锁预算。

解除隔离要求具体证据、所有原 key 投递已核对、已知非备份清理任务完成、每群预算仍被封锁。解除隔离不会自动恢复工作或放开预算。失败恢复在 public schema 不存在时仍可查看隔离状态。恢复测试仅创建和销毁 `groupagent_ops_<随机32位>` 临时数据库；从不清空共享 groupagent_test。

原文到期与显式忘记分开：到期清理 raw/input 缓存和未保留产物，已验证且尚在独立保留期的文档可继续读取和经授权复用；忘记/撤回会硬撤销来源并清理全部可证明关联的副本。清理会清空 checkpoint、needs_input 等运行事件正文，不仅设置失效标志。accepted 回执元数据保留，无法据此撤回别人已经收到的消息。

LocalChannel 提供精确事件/平台ID/投递key清理，保留流序号和回执，阻止旧输入及附件重放。真实上游如果没有可靠 purge 接口，返回 `upstream_cleanup_unsupported` 并保持待处理，绝不扫描或删除微信 profile。未决发送回执返回 `delivery_unknown`；原 key 后续回执与 bot echo 的清理完成前不确认任务。备份按最多 7 天到期物理删除，不能宣称所有历史副本瞬间删除。人工复制出受管理目录的备份不在本模块删除范围。

## 合成旧 SQLite 迁移

只读 sqlite URI `mode=ro&immutable=1`；拒绝 symlink、WAL/journal、不匹配 schema，读取前后校验文件 SHA-256。apply 再校验源指纹、计划摘要与目标群；同计划第二次执行返回已应用，源变化必须重新 dry-run。

稳定 sender ID 与群内姓名保留；manual 姓名只进入待确认名单，不能直接制造 owner 姓名例外。现有可靠群昵称、离群状态不会被旧缓存覆盖。旧 task 仅成为 TeamTask；负责人空缺保留 NULL，可通过 `claim_team_task(expected_revision=...)` 并发认领。旧任务不会成为 WorkOrder，旧提醒/打卡只进入待确认清单，不恢复自动发送。原始对话不复制。

本次仅检查邻接旧项目的 Python schema 源码，未执行旧模块。审阅源文件 hash：

| 文件 | SHA-256 |
| --- | --- |
| leader.py | 65df4623799d1b5aab68c59c3422409d7a91508e69145106131163c7c723816f |
| memory.py | f2ad5df5ad93bb32f1565890a03b6a7d2ebc3f13c014081d87f3f3c0d461cd3e |

这些 hash 是审阅版本证据，不是实际旧数据迁移验收。

## 验证与来源

真实 PG 用例覆盖一致快照 dump/restore、非空目标拒绝、归档损坏、restore 失败隔离、外置墓碑覆盖旧备份、预算隔离、凭据不入 argv/日志、7 天删除、合成迁移幂等/冲突/空负责人、并发认领、source scope、FS symlink 哨兵、raw expiry 文档保留、forget 清理、journal 写盘失败回滚。测试目录 `tests/ops`；最终数量以同一构建的测试报告为准。

技术依据（查阅 2026-11-14）：[PostgreSQL pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html)、[pg_restore](https://www.postgresql.org/docs/16/app-pgrestore.html)、[libpq password file](https://www.postgresql.org/docs/16/libpq-pgpass.html)、[SQLite URI read-only/immutable](https://www.sqlite.org/uri.html)。恢复仅面向本系统产生、校验通过的受控归档；不导入不可信来源的 SQL/归档。
