# GroupAgent

微信群协作智能体。GroupAgent 是暂用工程名，按 [Spec v0.1](docs/SPEC_V0.1.zh-CN.md) 分阶段开发。

当前提供可运行的 **开发底座与离线交付闭环**：真实 PostgreSQL 持久工作单、受控工具循环、四种文档格式、增量通道接收、发送 outbox 与持久调度。合成模型和通道在演示中有明确标记；它们不代表自然对话质量或真实微信验收。

新版尚未满足生产发布门槛，现有机器人未改动。视频识别、抽帧、音轨分析和生成均不在当前范围；语音消息仍在完整版本范围，但尚未接入。

旧原型位于相邻的 `Project_WechatLeader` 目录。本仓库不复制其密钥、登录资料、群成员名单、聊天记录或数据库；研究只引用代码行为和公开资料。

## 开始开发

需要维护中的 Python 3.11 或 3.13、uv 0.12.18、PostgreSQL 16+。DOCX/PDF/PPTX 渲染另需 LibreOffice、Poppler 和适当字体。以下命令只连接独立开发数据库，不连接微信或付费模型：

```sh
uv sync --locked
uv run --no-sync groupagent check-config docs/spec/config.example.json
uv run --no-sync python scripts/dev_postgres.py start
uv run --no-sync groupagent migrate --dsn-file runtime/postgres/dsn.txt
uv run --no-sync groupagent demo --dsn-file runtime/postgres/dsn.txt --format md
```

演示会创建合成群身份，完成“接收并去重 → 工作单 → 工具生成文件 → 验证 → 模拟平台接受回执 → 完成”，并把证据保存在被 Git 忽略的 `runtime/demo`。`candidate` 只表示执行器产出候选结果，业务 `completed` 必须经过验证和回执门槛。

```sh
export GROUPAGENT_TEST_DSN="$(uv run --no-sync python scripts/dev_postgres.py dsn)"
uv run --no-sync pytest -m "not render" -q
uv run --no-sync ruff check src tests scripts
uv run --no-sync ruff format --check src tests scripts
uv run --no-sync mypy src
uv build
uv run --no-sync python scripts/dev_postgres.py stop
```

完整渲染测试、中文字体、通道接口、已验证范围与剩余门槛见 [开发与验证状态](docs/development/IMPLEMENTATION_STATUS.md)。配置示例默认禁止启用生产；`check-config --production` 检查配置是否齐全，不替代运行时、角色、预算、媒体、隔离或实际交付验收。

## 文档

- [初版 Spec v0.1](docs/SPEC_V0.1.zh-CN.md)
- [技术契约与状态机](docs/spec/TECHNICAL_CONTRACTS_V0.1.md)
- [48项验收场景与发布门槛](docs/spec/ACCEPTANCE_V0.1.md)
- [开发任务与依赖](docs/spec/IMPLEMENTATION_PLAN_V0.1.md)
- [规格变更记录](docs/CHANGELOG.md)
- [开发前评估报告](docs/PRE_SPEC_REPORT.zh-CN.md)
- [开发流程与纪律草案](docs/ENGINEERING.md)
- [证据与来源登记](docs/SOURCES.md)
- [早期 spec 准备清单（历史）](docs/SPEC_READINESS.md)
- [实现与验收状态](docs/development/IMPLEMENTATION_STATUS.md)
- [Harness 接入实验](docs/development/HARNESS_POC.md)
- [微信适配器契约实验](docs/development/WECHAT_ADAPTER_POC.md)
- [文档结构和渲染实验](docs/development/ARTIFACT_POC.md)

报告和开发流程中的选型、时间、性能与成本数字均有明确状态：已核实事实、设计建议、待验证假设。没有把候选架构当成已经实现的功能。

## 仓库状态

私有远程仓库为 [huchenzi0609-creator/GroupAgent](https://github.com/huchenzi0609-creator/GroupAgent)。开发通过独立分支与 PR 推进，CI 定义位于 `.github/workflows/ci.yml`；实际运行状态与尚未启用的控制在开发状态文档记录，不把配置文件当作已经通过的检查。

研究报告与初版 Spec 的本地文档分支保留。真实 Harness、OS 沙箱、生产模型与微信交付需继续验证；长期记忆、多模态供应商、费用及运维等剩余工作按原 Spec 推进，没有因为底座完成而降低完整版本的验收要求。
