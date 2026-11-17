# GroupAgent / Pot

大学微信群协作智能体。工程名 GroupAgent，群内名 **Pot**，文本带 **【Pot】** 标识。

当前迁移分支为 `refactor/pot-durable-agent-core`，使用 LangChain 标准 Agent Loop 与 LangGraph 持久检查点。前后台复用同一内核、独立会话；现有微信接入、成员群昵称、长期记忆、任务分工、文档与媒体工具、定时规则和交付回执继续复用。旧 Scoped 自写循环与原生 Harness 执行路径已经退出。

本轮已更新至 `test_2`，通过本地任务、故障恢复与真实群功能及共存验收；10 秒首次回复目标尚未达到，详见[验收记录](docs/development/DURABLE_CORE_VALIDATION.zh-CN.md)。任务、成员和有效成果保留，到期原文与原始输入沿用原保留策略。2GB 服务器重型工具单并发，按实时内存准入并为小游戏保留空间。

用户已取消金额、token 和目标累计步数预算；保留费用记录、单次工具资源约束与异常退避。语音输入识别在范围内，TTS、插画生成服务和视频处理不在当前计划。

## 开发与验收

Python 3.11+、uv 0.12.18、PostgreSQL 16+；Office/PDF 与浏览器在隔离 Linux worker 中执行。密钥、聊天、数据库及附件只存受控运行目录，不入 Git。

```sh
UV_CACHE_DIR="$PWD/.cache/uv" uv --no-config sync --locked
.venv/bin/groupagent check-config docs/spec/config.example.json
```

完整的本地测试、真实任务、故障注入和发布步骤见 [当前运行手册](docs/development/DURABLE_CORE_RUNBOOK.md)。本地通道接受与微信接受分别记录；模型自称完成、离线脚本模型和未运行的检查都不能替代真实验收。

## 架构与工程资料

- [标准持久内核决策](docs/adr/0006-standard-durable-agent-core.md)与[执行器契约](src/groupagent/execution/README.md)
- [Spec](docs/SPEC_V0.1.zh-CN.md)、[技术契约](docs/spec/TECHNICAL_CONTRACTS_V0.1.md)、[工程纪律](docs/ENGINEERING.md)
- [2GB 部署档](deploy/SERVER_2GB.md)、[worker 构建](deploy/worker/README.md)
- [历史本地运行与验收记录](docs/development/LOCAL_RUNBOOK.md)、[前期修复报告](docs/development/FOUR_FAILURE_REPAIR.md)
- [资料来源](docs/SOURCES.md)

历史报告中 Harness、有限预算和旧阶段未运行项保留其原测试语境；当前实现、政策与发布状态以本轮决策及验收记录为准。
