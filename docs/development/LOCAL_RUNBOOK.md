> 历史运行/验收记录（2026-11-14—15）。当前标准内核、无限累计预算和发布步骤见 [2026-11-17运行手册](DURABLE_CORE_RUNBOOK.md)；下面的Harness与50元命令不再是当前配置。

# 本地运行与发布准备（进行中）

**CI字体兼容小修：** 完整30/30实模验收绑定c411源码。最终提交另含`5c9c4640ac097c4a4c94b7a9c7a26708eb6ddd5a4ee982c0cb134b9b8880029e`的负数折线图标签避让修复；Linux DejaVu字体已复现原FAIL并验证修复PASS，正数验收样例及恒定/零值PNG字节不变。仅图表相关回归（13项及文档绘图选集172项）和必要CI重新验证，未重跑付费30项，旧CI失败记录保留。

更新：2026-11-15。适用于 `feat/local-readiness` 的隔离开发环境。当前默认通道为持久本地消息通道；本轮不连接微信、生产服务器或旧数据库，不安装systemd服务。

当前c411/122py（freeze18）的core18为2134 PASS/52 SKIP/31 DESELECTED/158.30秒，worker19为110 PASS/零skip/284.07秒；mypy122/Ruff/format285、images18同源通过，新增第52项core skip已在worker覆盖。focus22三任务均completed，Chart02全11硬checks/像素绑定通过、双WEB本轮sidecar review PASS；原raw1 PASS/2 NEEDS_REVIEW保留，未作统一3/3汇总。full22已在同一c411源码、images18和固定checkerfa下完成30/30 PASS；必要语义/视觉复核均绑定本轮产物，最终零付费汇总未增加调用。Owner audit282332已实际应用用户累计11.65基准，原50/50 policy与revision1不变，unknown5/held0.562140继续保留；最终有效上界17.266165含后续suffix保守估算与原unknown预留，非控制台实扣。 详见[修复报告](FOUR_FAILURE_REPAIR.md)；不以焦点sidecar替代全量正式汇总。

下列25b/09镜像命令及测量保留为此前基线：core11=1727/51/28/106.98秒、worker09=97/258.17秒、120py同源与三例实际执行/三性能。新候选须使用修复报告绑定的镜像与证据，不把旧结果移植；schema001—012未变。最新提交CI见[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)。

`demo` 使用合成模型且不计费；`serve` 使用真实模型，即使通道为local也会计费。所有实际验收调用必须继续使用既有 `local-acceptance-20261114` 账本的累计50元上限，不能通过新run-id、群ID、数据库、代理或重启重置额度。

## 依赖与离线冒烟

需要Python3.11或3.13、uv0.12.18、PostgreSQL16+。`scripts/dev_postgres.py`只管理自己创建并标记的开发集群，监听私有Unix socket，不开放TCP。

```sh
UV_CACHE_DIR="$PWD/.cache/uv" uv --no-config sync --locked
.venv/bin/groupagent check-config docs/spec/config.example.json
.venv/bin/python scripts/dev_postgres.py start
.venv/bin/groupagent migrate --dsn-file runtime/postgres/dsn.txt
.venv/bin/groupagent demo --dsn-file runtime/postgres/dsn.txt --format md
```

core回归不需要供应商密钥或微信：

```sh
export GROUPAGENT_TEST_DSN="$(.venv/bin/python scripts/dev_postgres.py dsn)"
.venv/bin/pytest --strict-markers --strict-config -m "not render and not sandbox and not live" -q
.venv/bin/ruff check src tests scripts
.venv/bin/ruff format --check src tests scripts
.venv/bin/mypy --no-incremental src
uv --no-config build --no-build-isolation
```

不要并行运行多个pytest进程修改同一测试数据库的迁移/恢复状态。core中的skip表示依赖缺失或该类验证未执行，不应计入通过分母。

此前core11日志`runtime/ci/final-core-11.log/.xml`仍绑定25b，09/10 fixture FAIL、core07及06结果均留在旧版本归档；它们不代替最新repair-core-02或repair-workers-03。完整绑定见[四项失败修复报告](FOUR_FAILURE_REPAIR.md)。

## 宿主渲染与真实worker

Ubuntu24.04的独立宿主渲染环境安装LibreOffice Writer/Impress、Poppler、fonts-wqy-zenhei；确认 `soffice`、`pdftotext`、`pdftoppm` 可发现，再运行：

```sh
export GROUPAGENT_RENDER_TESTS=1
export GROUPAGENT_FONT_DIRS=/usr/share/fonts
export GROUPAGENT_PDF_FONT=/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc
export GROUPAGENT_OFFICE_FONT='WenQuanYi Zen Hei'
.venv/bin/pytest --strict-markers --strict-config -m render -q
```

这些字体路径只适用于安装相应软件包的Linux环境。macOS使用可信的本机中文字体，不复制商业字体入库；可通过 `GROUPAGENT_PDF_FONT`、`GROUPAGENT_FONT_DIRS`、`GROUPAGENT_OFFICE_FONT` 指定路径/字体族。合成中文OCR输入使用 `GROUPAGENT_TEST_CJK_FONT` 或 `GROUPAGENT_PDF_FONT`；没有中文字体不能以缺字图片验收OCR。

worker构建步骤见 [deploy/worker/README.md](../../deploy/worker/README.md)。完整30项历史受测环境为c411/images18；以下命令使用CI字体小修后的images19，源码一致性见`frozen-image-source-proof-repair-19.json`。此前`frozen-image-source-proof-repair-18.json`核对两镜像122py同源、uid65532；worker19完整选集实际110 PASS、零skip、284.07秒，含新增真实worker用例、覆盖新增第52项core skip。先确认iid与proof一致；SDK/runtime依赖层复用，不称native独立重现。其他skip不算能力通过，旧候选仅为[历史证据](FOUR_FAILURE_REPAIR.md)。

```sh
export GROUPAGENT_WORKER_IMAGE="$(cat runtime/build/worker-repair-19.iid)"
export GROUPAGENT_HARNESS_IMAGE="$(cat runtime/build/harness-repair-19.iid)"
# 只指定独立开发Docker服务的Unix socket；默认本机Docker可不设置这一项。
export GROUPAGENT_DOCKER_HOST=unix:///REPLACE_WITH_DEVELOPMENT_DOCKER_SOCKET
.venv/bin/pytest --strict-markers --strict-config tests/sandbox tests/execution/test_harness_real.py tests/artifacts/test_isolated_matrix.py tests/artifacts/test_chart_labels.py tests/media/test_sandbox_media.py tests/media/test_browser_worker.py tests/runtime/test_inputs.py -q
```

先替换socket占位符再执行。该命令实际运行Docker/SDK/文件/OCR/浏览器；模型、HTTP与通道夹具仍属合成。测试不读取供应商密钥，不属于真实模型或微信验收。当前CI为render和isolated-worker设置了非空且零skip检查，避免“必需链路全部跳过也绿灯”。

2026-11-14经公开Registry元数据核对，固定base digest是OCI多平台index，包含Linux amd64和arm64；SDK及两架构runtime wheel的3个锁定hash也与公开PyPI发布记录一致。该元数据检查本身未运行Docker；远程`a00b4132…`的托管isolated-worker已另行success，仍不等于目标服务器的完整服务与容量通过。

## 本地持续服务

`config.local.json`以 [配置样例](../spec/config.example.json) 为基础，且被Git忽略。样例默认关闭群处理，不能直接启动服务。至少明确以下字段：

| 字段 | 本地要求 |
| --- | --- |
| `group` | 启用明确的合成群、owner和时区；合成成员ID与提交消息一致 |
| `runtime.channel` / `allow_live_send` | `local` / `false` |
| `runtime.local_member_names` | 合成成员ID到群昵称的映射，不使用真实群名单 |
| `runtime.worker_image` / `harness_image` | 与当前验收源码对应的不可变ID；默认 `engine=harness` 需要两个镜像 |
| `runtime.docker_host` | 可选独立Docker Unix socket；不指向生产主机 |
| `spend` | 明确daily/total上限、币种及已注册的价格快照；本轮复用既有50元账本 |
| `model_routes` | 本轮实际text/vision为 `deepseek-flash`，ASR为 `tools/asr`，TTS为 `tools/tts`；不配置插画服务 |
| `runtime.pg_bin_dir` | 与PG服务端主要版本匹配的客户端目录，用于备份/保留清理；缺依赖不能宣称备份已工作 |

`.env.local`是当前用户私有的0600普通文件；只包含已授权的 `DEEPSEEK_API_KEY`，本地通道不需要微信token。不要用 `source .env.local` 将其扩散到Shell/子进程，也不要将内容写入配置、模型参数、日志或构建上下文。服务启动时只读取允许的秘密项，重启不会调整既有金额上限；配置与账本不同会停止启动。

```sh
.venv/bin/groupagent check-config config.local.json
.venv/bin/groupagent serve --config config.local.json --dsn-file runtime/postgres/dsn.txt --root runtime/app --secrets .env.local
```

`check-config`不访问外部服务，也不会验证secret、镜像可用性或供应商能力。服务在启动时迁移所选开发数据库；上线前的迁移和回滚仍须独立演练。

另一个终端提交明确的合成消息或附件，成员ID先与配置核对：

```sh
.venv/bin/groupagent local-submit --config config.local.json --root runtime/app --member synthetic-owner --text '请用Python实际计算1到100的平方和'
.venv/bin/groupagent local-submit --config config.local.json --root runtime/app --member synthetic-owner --text '请总结这份材料' --file synthetic-report.pdf
.venv/bin/groupagent status --config config.local.json --dsn-file runtime/postgres/dsn.txt --root runtime/app
```

`local-submit`只写本地spool，拒绝真实通道配置；一次接受一个 `--file`，引用用 `--reply-to`。附件必须是合成/授权测试文件。输出文件与消息回执写入 `runtime/app/channel/outgoing`；accepted表示本地通道接受，不能证明手机收到、打开或播放。`status`显示工作/投递队列、未决告警、活跃进程租约和费用；它不是外部监控或真实通道连通性证明。

明确自包含的新文件/数值计算请求可走快速受理，保留完整Goal及可确定的格式、页数、逐字标题；不确定、引用/历史修改、人类分工、控制或日程仍走原路径。快受理前可没有Model费用，完成仍需实际工具与产物验证；“已受理”不等于已执行。原权限、quiet、source、snapshot、预算和幂等门控不变。

Ctrl+C/SIGTERM停止服务并保留状态。用同一配置、数据库及root重启会核对checkpoint、租约和效果账本。unknown发送或工具效果保留原幂等键；不能证明安全时暂停。不得删除状态、改幂等键或手工标completed来解除阻塞。

## 真实API验收

以下命令会产生真实模型费用，必须由已授权的根验收流程串行组织，并继续使用同一持久50元累计账本：

```sh
.venv/bin/python scripts/live_interaction_eval.py --run-id unique-language-run --concurrency 2
.venv/bin/python scripts/live_runtime_matrix.py --run-id unique-task-run --worker-image sha256:REPLACE_WITH_TESTED_ID --harness-image sha256:REPLACE_WITH_TESTED_ID --docker-host unix:///REPLACE_WITH_DEVELOPMENT_DOCKER_SOCKET
```

镜像和socket占位符必须替换。每轮保留独立run-id及失败证据；不得删账本或换群预算。语音素材TTS、转写和输出核对也计入同一额度。

同一矩阵中断后，先用原run-id、镜像和socket加 `--resume --recheck-only` 核对已完成工作：

```sh
.venv/bin/python scripts/live_runtime_matrix.py --run-id EXISTING_RUN_ID --worker-image sha256:ORIGINAL_WORKER_ID --harness-image sha256:ORIGINAL_HARNESS_ID --docker-host unix:///ORIGINAL_DEVELOPMENT_DOCKER_SOCKET --resume --recheck-only
```

该模式核对原manifest/fixture/配置/镜像，归档旧结果后复查原产物，不开启新任务或付费语音核对；缺少既有语音语义证据时不能补成PASS。去掉 `--recheck-only` 的 `--resume` 只继续尚未开始的用例，仍有可能计费；已开始/失败/暂停工作不隐式重跑，未决工作/事件须先审查。不同源码的复查会记录混合来源，不能当成一次最终冻结源码的全新30项通过。

固定输入为 `tests/fixtures/interaction_cases.json` 和 `runtime_acceptance_cases.json`，预期不进入受测模型上下文。最终分子/分母、失败和费用在 [验收矩阵](LOCAL_ACCEPTANCE_MATRIX.md) 记录。语义解析、工具回执、实际文件、投递接受须分别检查；模型自称完成不能作为证据。

此前已完成的固定候选评测为`interaction-release-06`：精确匹配246/264；实际门控离线复核请求召回215/219（98.1735%）、误介入0/32；禁止写候选和业务写入均为0。源码SHA256 `88a5999ed5454631b4cf93d4c06a68d6aeebb5b0d9ac53d5b9f0b30c4eb369de`运行前后未变。候选P95 2.419秒不能当作daemon持久接收→outbox性能；反复使用同一合成固定集不代表未见样本估计。

`runtime-matrix-06`复核汇总`summary-after-ocr-review.json`为26 PASS、4 FAIL / 30（86.6667%），该历史轮仍FAIL。Chart02的原产物追加无prompt/gold的云OCR及独立assistant复核；此前25/30、本地OCR失败、四项最终FAIL和固定gold均保留，见[进展](LOCAL_VALIDATION_PROGRESS.md)。原执行SUT本地commit `f83a9173a0dea2aa02afa3230c88eb3d406cc276`，宿主与06镜像120个Python源码SHA256均为`ee73a7302b58a261f14bf854b466aa59928ce762a000f2fd41b35417e6c3d44d`；核对记录为`runtime/build/frozen-image-source-proof-06.json`。后续计费源码`798e6379…`参与检查环境，汇总保留mixed_product_sources，不代表用新源码重跑30项。安装版本Pillow13.0.0、SDK/runtime0.2.2rc2；Harness复用既有不可变依赖层，没有全新安装/native复现证据。历史04为18 PASS、11 FAIL、1 NOT_RUN，05为21 PASS、9 FAIL，不拼接。

快路对原30的selection命中0（当时源码6f），不是重跑。真实代表执行03已3/3 PASS：Markdown、独立解码3页/标题PPT、实际Python ToolReceipt输出391及本地接受，未held且绑定25b/09镜像。02计算无Tool被Engine拒绝的FAIL、01 helper默认None比较错误及原生取消仍保留；Toolkit显式required_tools之后的03成功不覆盖历史。

此前25b的`perf-live-final`前后同源：短答/接单各30/30正确、0失败，P95保守上界6.200338417/2.044179791秒PASS；短答实模，接单`real_app_deterministic_admission`。快路DB证明全文Goal、格式/页数、Work/ack唯一、原群/来源和验证要求，不强造预Model费用；fallback保留实际账单证据。held执行lane不跑重型工作，实际执行由03三例单列，受理后仍原生取消。

此前25b的`perf-cancel-final`同源：30/30正确拒绝旧租约、P95上界1.175423541秒PASS；固定provider与真实PG/gateway，无Model费用。三项阈值20/5/2秒及commit-bracket定义未改；旧接单01—04和取消01/02的FAIL、各版本历史成功均完整保留，不补入最终分母。这里只覆盖该本地条件，非重型吞吐/微信时延；所有付费调用仍归同一50元账本。

费用证据分列：旧用户报告截至2026-11-14 23:00前4.83 CNY、前缀2203笔gross估算39.054016及schema1审计111620保留为历史。最新用户明确累计11.65 CNY，Root已通过[BudgetLedger.reconcile_billing](../../src/groupagent/billing/ledger.py)可信Owner API实际追加schema2 audit282332/prev111620，0 provider；新显式opt-in累计模式只替换settled前缀估算，unknown/预留全额继续held，默认旧mode含unknown仍拒绝。需as_of、SQL期望count/gross、币种、evidence_ref/SHA与上一审计ID；幂等/前缀/hash异常失败关闭。as_of取Root接收答复的2026-11-15 06:21:31 UTC（用户未给精确查询时刻、期间paid/active已空），不是connector验票。入口不对模型/自然语言开放，审计仅应用级追加、无DB强制不可变承诺。

此前25b轮的`runtime/billing/final-verification-report.json`记录policy daily/total均50、revision1未改；2808笔settled的gross55.079718 CNY为保守估算，2笔unknown预留0.168044保留。该轮上界21.023746 CNY不是当前修复轮最终值或实扣；原price/reservation/审计不覆写、unknown不伪结算，旧`actual`仍按保守估算解释。

上一段为25b轮结束/07前历史快照。新Owner基准实际应用后上界12.212140=用户报告累计11.65+原unknown held0.562140，3974 settled/5 unknown及三金融表hash不变；旧46.821060上界与80.482936 gross估算不删。原50/50 policy/revision1未变，不退款、不增额度、不逐笔伪settle。最终有效上界17.266165另含新suffix保守估算，非新实扣；full22正式30/30 PASS，已完成唯一一次最终零付费汇总，证据见[报告](FOUR_FAILURE_REPAIR.md)。

## 备份、恢复与回退

`backup`、`backup-verify`、`restore`使用受管理备份目录和外置删除日志。`--pg-bin`是包含匹配版本 `pg_dump`/`pg_restore` 的目录。以下操作仅针对已授权的独立本地数据库：

```sh
.venv/bin/groupagent backup --config config.local.json --dsn-file runtime/postgres/dsn.txt --root runtime/app --pg-bin /REPLACE_WITH_POSTGRESQL_BIN
.venv/bin/groupagent backup-verify --config config.local.json --dsn-file runtime/postgres/dsn.txt --root runtime/app --pg-bin /REPLACE_WITH_POSTGRESQL_BIN --backup-id REVIEWED_BACKUP_ID
.venv/bin/groupagent restore --config config.local.json --dsn-file runtime/postgres/dsn.txt --root runtime/app --pg-bin /REPLACE_WITH_POSTGRESQL_BIN --backup-id REVIEWED_BACKUP_ID --target-dsn-file runtime/postgres/empty-target-dsn.txt
```

先替换路径/备份ID，并由维护者准备新的空目标数据库及私有DSN文件。恢复拒绝非空目标，不覆盖运行库；恢复后的任务、投递和调度保持隔离，正常 `serve` / `status` 可能返回 `restore_pending`。原库不可用时，离线恢复由 `BackupManager.restore(..., offline=True)` 受控API提供；当前CLI仍要求源DSN且不传该标志，不能把该命令当成无源库灾备入口。

当前没有 `restore-release` 或 `reconcile` CLI命令。可信维护者须通过 [RestorationService](../../src/groupagent/ops/restore.py) 的 `status`、`reconcile_delivery`、`release` API，按 [ops恢复契约](../../src/groupagent/ops/README.md) 检查删除日志、每群授权、未决投递、费用和维护证据，再决定解除隔离；参照 `tests/ops/test_backup.py` 和 `test_task_recovery.py` 的合成演练。不能仅凭旧程序可启动恢复发送，也不能猜测unknown已经失败。

回退必须指向记录过的应用/镜像/配置与兼容schema，必要时先恢复已验证备份到新空库；没有自动向下迁移命令。当前旧生产切换和回退未执行。

旧SQLite仅在另行授权后迁移：先用 `legacy-plan --source ... --legacy-group ...` 审核，再用 `legacy-apply --source ... --plan-id ... --source-sha256 ...` 绑定原件hash。两者也要求 `--config`，数据库通过 `--dsn-file` 指定；这里只核对命令，不读取旧库。旧任务只成为TeamTask，旧提醒不自动激活，不复制聊天/旧密钥。

## 发布前外部门槛

`deploy/groupagent.service`是尚未安装的systemd模板。控制服务用户加入Docker组是受信任主机编排权限，不能当作不可信worker权限；worker没有宿主挂载、出站网络或凭据。模板中的用户、目录、PG客户端、镜像、启动/停止和备份必须在目标环境实际演练。

[PR #8](https://github.com/huchenzi0609-creator/GroupAgent/pull/8)仍draft，base未合并[PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1)。已验证代码remote`7a268eba0e50b33ba4f108f348da3c9c64bf368e`与本地946ce37同tree292292ed…，[CI43632514186](https://github.com/huchenzi0609-creator/GroupAgent/actions/runs/43632514186)四job全部success；该历史结果不延伸到未来head，最新状态以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准。本轮不安装服务或部署生产。

当前实测仅为macOS arm64控制进程和Linux arm64专用VM（2 CPU、3 GiB）。全部L5、目标Linux amd64服务器与完整服务并发/最大媒体/保留期容量、真实测试群手机文件/语音、回声、登录失效恢复、至少七天shadow、生产角色/预算/保留策略、真实迁移和单发送者切换均为NOT_RUN。本地通过不替代这些门槛；本轮不部署。

855dd094阶段CI已绿作为历史保留；最新提交状态以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，发布记录另绑定实际head与检查结果，不沿用旧绿。
