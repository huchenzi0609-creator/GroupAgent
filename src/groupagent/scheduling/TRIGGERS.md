# 有限事件规则

`EventTriggerService` 只接受 `task_completed`、`work_completed`、`work_failed`、`attachment_received`，不接受监听代码、任意表达式或模型自报事件。构造时注入 `PostgresStore` 与可信的 `actor_resolver(member_id)`；`in_tx(tx)` 可把规则动作与参与决定原子提交。

```python
rules = EventTriggerService(store, resolve_actor)
rule = rules.create(
    actor,
    "task_completed",
    task_id,
    "完成后整理跟进说明",
    source_event_id=request.id,
    request_key="stable-action-key",
)
occurrences = rules.poll(control_actor)
```

`create` 可选 `max_occurrences=1`（最多 10）、`expires_at`（默认 30 天）、`source_work_id`、可信测试时钟 `now`。`get/list/occurrences` 读取规则；`pause/resume/cancel` 使用 `expected_revision`。`notify(control_actor,event_type,resource_id,event_key=...)` 只查已提交的规范事实，不能凭调用参数生成事实。通常父运行层直接轮询 `poll` 即可；每轮默认最多处理 100 个 occurrence，支持 1—1000。

迁移 009 的数据库触发器只在 TeamTask 的状态实际转为 completed，或 WorkOrder 实际转为 completed/failed 时记录事实。同状态修改不会再次记录；回滚会一起撤销。工作完成仍受既有 validation 与 accepted delivery 条件约束。事实只含资源、成员、来源 ID 和版本，不保存原文，也不回填旧历史。公开 API 的 `business:<id>` 必须对应同群同资源事实。任务重开后再次完成，过时完成事实被明确跳过；工作重试后旧失败事实也不重放。

接入字段来源后，task_completed 入队还绑定当前 Task.status 的真实完成确认来源及 source_task_revisions。撤回／忘记完成确认会撤销已经排队的后续 Work；删除没有消息来源的独立 Task 也会通过逻辑依赖撤销 Work。冻结的 009 不改写；没有来源时不会捏造一条完成消息。

附件规则使用 `member:<id>`、`work:<id>`、`group:<id>` 明确选择器。Member 仅能选择本人或本人 Work；Owner 可选择在群成员或本群。work 关联须由规范消息的 `reply_to` 指向工作原请求或已接受的投递，不能由模型提供“相关附件”声明。`attachment:<event_id>:<index>` 指向原消息中的确切槽位。kind、有效 channel handle、群／账户／epoch范围和视频 MIME／扩展名均会校验。`unresolved`、`pending`、`available` 表示授权附件消息已到达，均可入队；expired、unsupported、failed、recalled 或视频不能触发。真实接入的 canonical unresolved handle 不因稍后下载完成而改变。子工作显式保存 attachment_event_ids，由 InputProcessor 实际下载、验证并导入。消息到达不等于已下载、已解码或执行成功，没有宿主解析器快捷路径。

每次轮询都重新解析原创建者身份，并使用原角色与当前角色的权限交集；升为 Owner 不扩大旧 Member 规则授权。离群、角色撤销、政策版本变化、授权来源撤回／遗忘／过期、资源易主都会停止新入队。变更政策只能由原创建者显式恢复；恢复从当前时刻开始，不补跑暂停期间事件。get/list 对群内可信读者可见；写入仅允许本人或 Owner，Maintainer 只能调度，不获得业务写入身份。

时间规则与事件规则共用每群 10 条 active 配额、同一 advisory lock；锁顺序为 privacy memory lock → scheduler lock → 数据行。重复事件键不新增工作；子工作未结束或仍有 unknown/dispatching 投递时，下一事件明确记录 `overlap_busy` 跳过，不积压自动重试。只有实际入队计入 occurrence 上限。停止规则也暂停／取消已产生的非终态子工作；不会撤销已接受的投递。最后一次触发达到上限后，子工作仍可通过规则暂停／恢复。

每次 admission 在同一个数据库事务写合成 system 来源、已处理参与决定、WorkOrder、occurrence 和计数；不直接发送。子工作保留授权请求与触发来源的指纹，设置 `derived_depth=1`、`schedule_creation_allowed=False`。衍生工作不能创建新的规则，也不能作为另一个规则的 work 触发目标。没有模型完成就直接完成业务工作或跳过 outbox 的通道。

本地证据：2026-11-14，真实 PostgreSQL 的 `tests/scheduling/test_event_triggers.py` 29 项通过，含并发、崩溃回滚、权限、政策、完成确认撤回、任务删除、视频变体、附件状态、配额与真实工作完成条件。Supervisor 另有 unresolved canonical 消息→pending→重启→同 Work 导入→accepted 后完成测试（真实 DB/InputProcessor，显式合成 channel/decoder）。迁移先在 rollback-only 事务验证 DDL、CHECK 和状态变更行为再冻结。未连接真实微信群，未调用付费模型。附件内容是否合法仍由媒体／文档验证层决定；服务持有 DSN 属于可信边界，数据库超级用户不属于模型威胁模型。

## 工作内调度工具

`runtime.scheduling_tools.register_scheduling_tools(gateway,store,scheduler,triggers,work,lease,actor)` 注册 schedule_list/create/update/control 与 trigger_create/control；父 toolkit 仍决定是否授予这些名称。当前 source work 必须明确 `schedule_creation_allowed=True`、derived_depth=0，有有效的本人 lease 和人类请求证据。工具参数不能替换 sourcework、member、角色、scope 或投递目标。生成的规则／子工作保持一层衍生限制。

创建、更新、恢复的有效期硬限从当前起 30 天，事件次数 1—10，配额仍与普通 Scheduler/TriggerService 共用。列表和变更通过来源指纹及 private-topic 检查，只暴露授权公共来源的规则；不返回原始scope。所有业务写入与精简幂等回执在 memory→scheduler 锁序的同一事务完成；跨 Gateway 重启重放同键返回该次原始效果，参数变化拒绝。回执只存 ID／状态／版本／时间，目标原文不重复写入 audit。已确认事务回滚是 NO_EFFECT；提交结果不明确仍为 UNKNOWN。`tests/scheduling/test_tools.py` 10 项真实 PG 回归通过，包含并发同键、崩溃回滚、TTL、配额、权限和私有主题过滤。
