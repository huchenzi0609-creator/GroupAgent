# 本地身份与记忆验收 v1

2026-11-14：`test_identity_acceptance.py` 在项目专用 PostgreSQL 上运行，
**51 passed in 4.95s**：50 个独立群空间案例，加 1 个固定 fixture 哈希检查。
Ruff 和格式检查通过；没有调用模型、微信、Docker 或付费 API，也没有修改产品代码。

对应 `docs/spec/ACCEPTANCE_V0.1.md:90` 的身份／记忆样例数量要求。
固定数据为 10 类边界 × 5 个不同合成上下文（课程、实验室、访谈项目、志愿活动、工作坊）。
每个参数化案例建立两名不同稳定 ID 成员、两份有原话来源的正式任务与两份本人事实。
每例检查任务负责人不会因同名、文本中的“我”、纠错或摘要而串人；被撤回的创建来源
应移除对应任务内容和归属，不转给另一人。

| 类别 | 每类案例数 | 核验内容 |
| --- | ---: | --- |
| duplicate_names | 5 | 同群同名拒绝唯一解析，两个稳定 ID 保持区分 |
| rename_swap | 5 | 局部改名与随后互换群昵称，已有任务归属不变 |
| nickname_priority | 5 | 群昵称优先于 Owner 确认姓名；缺失昵称时才使用姓名例外 |
| two_speakers_i | 5 | 第二人引用第一条消息仍保留本人作者，“我”不能授权写另一人的事实 |
| own_correction | 5 | 本人纠错更新当前事实与任务标题，旧摘要失效，另一人不变 |
| foreign_correction_denied | 5 | 普通成员不能修改另一人的事实或任务，失败不提交 |
| recall_progress | 5 | 已验证撤回删除关联进度与事实，保留独立原任务及负责人 |
| recall_then_replay | 5 | 原创建消息撤回、重放后仍有墓碑；另一人的任务保持有效 |
| compact_restart | 5 | 压缩游标、来源、作者 ID 跨 MemoryService 重建保留；摘要标为不完整 |
| private_topic_compaction | 5 | 私有话题不进入群摘要、不供其他成员检索，公开任务归属不变 |

Canonical fixture SHA256（UTF-8 JSON，字段排序，无额外空格；不含运行时随机 group/event ID）：

`503c4042ad0ce25d4c0cf34cce7927ed4c73150bb929809769e3fabc9b688a6b`

这些是确定性的后端身份／来源验收。动作和期望由固定 fixture 明确指定，压缩使用
`DeterministicFixtureSummarizer`；不能报告为真实模型抽取准确率、人工语义标注结果、
真实微信验收或未见样本泛化估计。50 组均通过不替代既有权限、并发、保留期等回归。
首次运行的测试断言误把删除占位标题要求为空串，已改为核验原文不再出现且负责人为空；
产品原有的受控删除占位符不构成失败，也没有为通过验收调整产品实现或固定场景。

复现：设置专用合成测试库 `GROUPAGENT_TEST_DSN`，单进程运行
`.venv/bin/python -B -m pytest -q tests/memory/test_identity_acceptance.py`。
没有 DSN 时既有 fixture 会 skip；skip 不应计为验收通过。

## 性能证据边界

Spec PERF-001 和验收规格第 92 行要求从**持久接收**到 outbox 的样本分布：
长任务接单 P95 ≤ 5 秒、短回答 P95 ≤ 20 秒、取消入库并阻止新工具 P95 ≤ 2 秒。
本次只检索并定位已有证据，没有添加性能计时或宣称上述门槛通过。

- `tests/channels/test_intake_integration.py::test_committed_intake_precedes_model_and_ack`
  验证真实 PostgreSQL 提交先于 channel ack 与模型决策；HTTP 通道为 MockTransport。
- 同文件 `test_authenticated_channel_request_can_receive_a_bound_reply`
  验证已入库事件产生关联 outbox；决策为显式合成 provider。
- `tests/runtime/test_app.py::test_end_to_end_local_spool_actual_database_and_restart`
  验证本地 spool、真实数据库、接单与结果回执、重启不重放；模型为 ScriptedFakeModel。
- 同文件 `test_stop_runs_while_execution_is_waiting_for_model` 和
  `test_application_controls.py::test_control_preempts_a_different_slow_conversation_route`
  验证控制不等待慢模型、工作状态已取消／暂停。
- `tests/execution/test_engine.py::test_cancel_interrupts_model_and_prevents_new_tools`
  验证引擎取消后不启动新工具；不包含真实数据库或通道计时。

这些测试的等待上限和总测试耗时不是 P95。现有 live interaction 的 provider 候选路由耗时
也不能替代接单或取消链路。需要另行固定采样起止点、样本数、负载和 provider 类型后测量；
真实微信运输时间仍应单独统计。
