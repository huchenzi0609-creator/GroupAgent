# GroupAgent

**CI字体兼容小修：** 完整30/30实模验收绑定c411源码。最终提交另含`5c9c4640ac097c4a4c94b7a9c7a26708eb6ddd5a4ee982c0cb134b9b8880029e`的负数折线图标签避让修复；Linux DejaVu字体已复现原FAIL并验证修复PASS，正数验收样例及恒定/零值PNG字节不变。仅图表相关回归（13项及文档绘图选集172项）和必要CI重新验证，未重跑付费30项，旧CI失败记录保留。

微信群协作智能体，按 [Spec v0.1](docs/SPEC_V0.1.zh-CN.md) 开发。GroupAgent 是暂用工程名。

当前处于 **本地集成开发与验收中**，工作分支为 `feat/local-readiness`。已接通 PostgreSQL 持久状态、自然语言协调、真实 Harness SDK、无宿主挂载的隔离 worker、四种文档、图片/OCR、ASR/TTS、搜索、程序绘图、长期记忆和有限调度。正在用固定样本检查实际结果、产物字节、费用及投递回执；尚未完成整版发布验收。

现有线上机器人保持不变。本轮没有接入实际微信群、生产服务器、旧凭据或旧数据库。独立插画生成服务及所有视频处理不在本版范围；图片理解、OCR、代码生成图表/流程图和语音消息仍在范围内。

## 离线开发入口

需要 Python 3.11 或 3.13、uv 0.12.18、PostgreSQL 16+。Office/PDF 宿主渲染另需 LibreOffice、Poppler 和中文字体。以下演示使用真实数据库与文档工具、显式合成模型及本地通道，不调用付费模型，也不连接微信：

```sh
UV_CACHE_DIR="$PWD/.cache/uv" uv --no-config sync --locked
.venv/bin/groupagent check-config docs/spec/config.example.json
.venv/bin/python scripts/dev_postgres.py start
.venv/bin/groupagent migrate --dsn-file runtime/postgres/dsn.txt
.venv/bin/groupagent demo --dsn-file runtime/postgres/dsn.txt --format md
```

演示完成“持久接收并去重 → 工作单 → 文件生成 → 验证 → 本地通道接受 → 结案”，证据写入 Git 忽略的 `runtime/demo`。执行器的 `candidate` 只是候选结果；业务 `completed` 还需要结果验证和必需投递的接受回执。接受回执不表示成员已阅读。

```sh
export GROUPAGENT_TEST_DSN="$(.venv/bin/python scripts/dev_postgres.py dsn)"
.venv/bin/pytest --strict-markers --strict-config -m "not render and not sandbox and not live" -q
.venv/bin/ruff check src tests scripts
.venv/bin/ruff format --check src tests scripts
.venv/bin/mypy --no-incremental src
uv --no-config build --no-build-isolation
.venv/bin/python scripts/dev_postgres.py stop
```

只将独立测试库交给测试套件。上述 core 命令会跳过需要镜像或外部渲染工具的检查；skip 不是该能力通过。真实容器、中文渲染、本地持续服务、备份与恢复命令见 [本地运行手册](docs/development/LOCAL_RUNBOOK.md)。`serve` 即使使用本地消息通道，也会调用真实模型并计费，不属于离线演示。

## 验收与发布状态

当前最终候选为 **c411860f… / 122py / freeze18**：core18为2134 PASS、52 SKIP、31 DESELECTED（158.30秒），worker19为110 PASS、零skip（284.07秒），覆盖新增的core worker skip；静态与images18同源检查通过。其他skip不算能力通过。

固定30项本地综合验收已 **30/30 PASS**，全部使用同一冻结源码和镜像。本轮真实模型执行及必要的独立代理内容复核已完成；流程图的原始OCR漏字记录和一次同图真实OCR核验保留。结果见[修复报告](docs/development/FOUR_FAILURE_REPAIR.md)、[验证进展](docs/development/LOCAL_VALIDATION_PROGRESS.md)及[A01—A48矩阵](docs/development/LOCAL_ACCEPTANCE_MATRIX.md)。

原累计预算daily/total均50 CNY、revision1未改。用户新报告累计实扣11.65 CNY已由可信Owner对账审计282332应用，原5笔unknown/held0.562140保留；最终有效上界17.266165包含后续suffix保守估算，非新实扣。对账是同一50元预算的计量校准，不是退款或追加额度；报告时间口径与证据见[修复报告](docs/development/FOUR_FAILURE_REPAIR.md)。

[PR #8](https://github.com/huchenzi0609-creator/GroupAgent/pull/8)仍draft、base [PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1)尚未合并；最新提交CI以[动态checks页面](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准。真实微信L5、目标Linux容量、至少七天shadow、真实迁移及单发送者切换全部 **NOT_RUN**，本轮不部署。配置样例默认关闭群处理，`check-config --production`仅检查配置字段。

## 文档

- [Spec v0.1](docs/SPEC_V0.1.zh-CN.md)
- [技术契约与状态机](docs/spec/TECHNICAL_CONTRACTS_V0.1.md)
- [48项验收与发布门槛](docs/spec/ACCEPTANCE_V0.1.md)
- [开发任务与依赖](docs/spec/IMPLEMENTATION_PLAN_V0.1.md)
- [工程纪律](docs/ENGINEERING.md)
- [实现与验证状态](docs/development/IMPLEMENTATION_STATUS.md)
- [四项失败修复报告](docs/development/FOUR_FAILURE_REPAIR.md)
- [本地运行手册](docs/development/LOCAL_RUNBOOK.md)
- [分层验收矩阵](docs/development/LOCAL_ACCEPTANCE_MATRIX.md)
- [Harness 实验](docs/development/HARNESS_POC.md)、[微信契约实验](docs/development/WECHAT_ADAPTER_POC.md)、[worker 构建与容量](deploy/worker/README.md)
- [规格变更记录](docs/CHANGELOG.md)、[开发前报告](docs/PRE_SPEC_REPORT.zh-CN.md)、[来源登记](docs/SOURCES.md)

私有远程为 [huchenzi0609-creator/GroupAgent](https://github.com/huchenzi0609-creator/GroupAgent)。分支保护受当前仓库套餐限制，尚不能声称受到平台强制保护；现行控制为 PR 审阅与人工合并纪律。根流程统一提交和发布，运行数据、密钥、真实群资料和构建缓存均不得入库。
