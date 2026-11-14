# GroupAgent

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

[实现状态](docs/development/IMPLEMENTATION_STATUS.md) 区分已接通代码和已取得证据；[本地验证进展](docs/development/LOCAL_VALIDATION_PROGRESS.md) 保留成功与失败；[A01—A48证据矩阵](docs/development/LOCAL_ACCEPTANCE_MATRIX.md) 按 L1—L5 分层记录。测试总数、候选语义分数和整项业务验收使用不同分母。

当前冻结源码为 `25b1be61…`，源码提交 `974edd7…`，后续测试提交 `946ce37…` 未改产品源码。最终core11为 **1727 PASS、51 SKIP、28 DESELECTED，106.98秒**；09版worker/Harness均核对120个Python文件与此源码一致。快受理后的真实Harness执行三例已 **3/3 PASS**：Markdown、独立解码核对3页/标题的PPT、实际Python工具回执输出391并本地交付。

当前09版worker整套 **97 PASS、258.17秒**。最终同源码短答、接单、取消各30项全部正确，P95保守上界分别 **6.2003 / 2.0442 / 1.1754秒**，达到20/5/2秒门槛。短答实模，长任务受理为真实规则快路且held执行lane，取消为固定provider/真实PG与gateway；实际执行由上述三例单列。

`runtime-matrix-06` 在原 `ee73a730…` 执行结果上追加真实云OCR和独立assistant复核后为 **26 PASS、4 FAIL / 30（86.6667%）**；固定gold、四项失败、复核前25/30及所有本地OCR失败均保留。此结果保留执行/检查版本差异，没有在当前源码重跑30项。固定264条候选的召回215/219（98.1735%）、误介入0/32绑定更早的`88a5999e…`；独立身份/记忆50组场景及1项哈希校验共51项通过。以上分母与证据范围分别记录，全部L5、目标Linux服务器、七天试运行和生产切换均为 **NOT_RUN**。

本轮65项Python依赖0项已知漏洞，327个可发表文件凭据扫描0发现；wheel147条、sdist328条均无私有运行资料。累计policy仍50元；最终有效预算用量上界 **21.023746元**，含2笔未知预留，属于“用户报告的历史4.83元+后续保守估算”，不是实际扣费。原记录保留，最新完整控制台实扣尚未回传，见[费用说明](docs/development/LOCAL_VALIDATION_PROGRESS.md#费用与仍待完成的门槛)。

已验证代码head`7a268…`的[CI run43632514186](https://github.com/huchenzi0609-creator/GroupAgent/actions/runs/43632514186)四job（双Python core/render/isolated-worker）全部成功；这是该head的历史证明，最新提交CI以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准。[PR #8](https://github.com/huchenzi0609-creator/GroupAgent/pull/8)仍为draft，base仍未合并[PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1)，本轮不构成上线批准。

配置样例默认关闭群处理。`check-config --production` 只检查配置字段门槛，不能替代真实通道、模型质量、隔离、预算、备份、交付或发布审批。

## 文档

- [Spec v0.1](docs/SPEC_V0.1.zh-CN.md)
- [技术契约与状态机](docs/spec/TECHNICAL_CONTRACTS_V0.1.md)
- [48项验收与发布门槛](docs/spec/ACCEPTANCE_V0.1.md)
- [开发任务与依赖](docs/spec/IMPLEMENTATION_PLAN_V0.1.md)
- [工程纪律](docs/ENGINEERING.md)
- [实现与验证状态](docs/development/IMPLEMENTATION_STATUS.md)
- [本地运行手册](docs/development/LOCAL_RUNBOOK.md)
- [分层验收矩阵](docs/development/LOCAL_ACCEPTANCE_MATRIX.md)
- [Harness 实验](docs/development/HARNESS_POC.md)、[微信契约实验](docs/development/WECHAT_ADAPTER_POC.md)、[worker 构建与容量](deploy/worker/README.md)
- [规格变更记录](docs/CHANGELOG.md)、[开发前报告](docs/PRE_SPEC_REPORT.zh-CN.md)、[来源登记](docs/SOURCES.md)

私有远程为 [huchenzi0609-creator/GroupAgent](https://github.com/huchenzi0609-creator/GroupAgent)。分支保护受当前仓库套餐限制，尚不能声称受到平台强制保护；现行控制为 PR 审阅与人工合并纪律。根流程统一提交和发布，运行数据、密钥、真实群资料和构建缓存均不得入库。
