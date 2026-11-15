# 实现与验证状态（进行中）

**CI字体兼容小修：** 完整30/30实模验收绑定c411源码。最终提交另含`5c9c4640ac097c4a4c94b7a9c7a26708eb6ddd5a4ee982c0cb134b9b8880029e`的负数折线图标签避让修复；Linux DejaVu字体已复现原FAIL并验证修复PASS，正数验收样例及恒定/零值PNG字节不变。仅图表相关回归（13项及文档绘图选集172项）和必要CI重新验证，未重跑付费30项，旧CI失败记录保留。

更新：2026-11-15。分支：`feat/local-readiness`。本页是本地集成状态，不是 v1 完成或上线批准。当前线上机器人未变；本轮实际微信群资料、生产访问和旧数据迁移均为零。

范围按用户最新要求：排除独立插画服务及视频理解、抽帧、音轨分析和生成；保留图片理解/OCR、程序绘图、四种文档、语音、搜索、长期记忆与有限调度。

**最新入口：[四项失败修复报告](FOUR_FAILURE_REPAIR.md)。** 当前c411/122py（freeze18）的core18为2134 PASS/52 SKIP/31 DESELECTED/158.30秒，worker19为110 PASS/零skip/284.07秒；mypy122/Ruff/format285、images18同源通过，新增第52项core skip已在worker覆盖。focus22三任务均completed，Chart02全11硬checks/像素绑定通过、双WEB本轮sidecar review PASS；原raw1 PASS/2 NEEDS_REVIEW保留，未作统一3/3汇总。full22已在同一c411源码、images18和固定checkerfa下完成30/30 PASS；必要语义/视觉复核均绑定本轮产物，最终零付费汇总未增加调用。Owner audit282332已实际应用用户累计11.65基准，原50/50 policy与revision1不变，unknown5/held0.562140继续保留；最终有效上界17.266165含后续suffix保守估算与原unknown预留，非控制台实扣。

## 此前基线实现与范围

以下“已接通”指源码和相应测试路径存在。只有记录了环境、版本及实际结果的验证才是通过证据；模块存在不能使整项 A 用例自动通过。

| 模块 | 已接通的实现 | 当前证据及限制 |
| --- | --- | --- |
| 工程与配置 | Python3.11+、uv锁、严格配置、CLI、schema、私有资料排除 | 本表25b基线的core11/worker09/120py同源及历史CI已验证；当前c411与简短归档见[修复报告](FOUR_FAILURE_REPAIR.md)，不移用旧计数 |
| 持久状态与身份 | PostgreSQL 16 迁移001—012；事件去重、游标CAS、角色/群范围、成员启动同步、群昵称与人工姓名例外、来源权限 | 真实 PG 合成数据回归；实际通道名单及身份完整性仍需 L5；应用范围控制不等于数据库 RLS |
| 对话协调 | 无前缀参与、静默/批处理、引用与主题、工作接续、分工/执行分离、自然时间、聚焦澄清；明确自包含新Work快速受理 | 快受理保留全文Goal/可确定格式页数和原事务/_gate/来源复验，复杂请求仍走Model。此前25b同源run05的30接单P95保守2.0965秒达标，不作为当前ca7b测量；264候选仍绑定原评测源码 |
| 工作执行 | 真实 Harness SDK、每步工具策略、checkpoint、租约/fence、取消、等待输入、有限修复、重启恢复、重型并发槽 | 实际容器/SDK和本地合成通道证据；所有 unknown 效果先核对，不能把重启当重新授权或预算重置 |
| 文档与绘图 | MD/DOCX/PDF/PPTX读取、生成、声明范围编辑、模板/图表、不可变版本与冲突检查；代码绘图；隔离解析/渲染 | 每格式10个合成样本有真实容器验证；30任务复核后26 PASS、4 FAIL，失败保留。PDF可明确重建；不承诺任意模板、动画或嵌入对象无损编辑 |
| 媒体 | 图片格式/大小/哈希、OCR/扫描PDF、来源绑定、语音转写、关键歧义澄清、按需短语音及回执 | 真实供应商30张合成图片、20条TTS→ASR语义样本通过；实际群传输、手机播放及噪声录音未覆盖 |
| 联网 | 搜索/Fetch来源记录、URL/DNS范围、provider预算；隔离浏览器展示受控页面，禁用活动内容和额外出站 | 合成HTTP失败/注入回归和真实隔离浏览器证据；真实研究任务以最终任务矩阵为准；不绕过登录/验证码 |
| 记忆 | 主题/成员/任务检索、token阈值压缩、原子summary/checkpoint、事实纠错、撤回/删除传播、任务字段级来源 | 真实PG回归覆盖重启、跨群/私有主题、并发与删除竞态；独立50组身份/记忆场景加1项哈希校验共51项通过；不宣称无损记忆或真实群身份验收完成 |
| 调度 | at/every/daily/weekly、时区/DST、默认30天有效期、修改/暂停/恢复/取消、latest-only补跑、配额及有限事件触发 | 旧T06矩阵创建/修改通过；旧触发通知送达但required_text未连续匹配，原SCHEDULE03 FAIL保留。当前ca7b结果见修复报告；真实时钟及微信未覆盖，任意cron/RRULE未开放 |
| 费用与运维 | 原子金额预留/结算、每日/累计限额、价格快照；可信Owner累计账单对账；告警/health、备份/恢复、删除日志、清理、合成迁移 | 对账保留旧估算/全部预留与50元policy，61项真实PG billing通过；审计为应用级追加、非DB强制不可变。恢复解除隔离无CLI；目标服务器、真实切换和外部告警未验收 |
| 消息通道 | 默认持久本地消息通道；AgentWechat2.2.1契约适配、入库后ACK、文件/图片/语音发送与逐部分回执 | 本地spool与HTTP契约测试通过不等于真实微信；实际账号、共享账号回声、附件/语音手机端、失效重连全部待L5 |

数据库迁移一经应用即冻结校验和，后续变化新增迁移。控制服务持有数据库/供应商凭据并编排Docker；不可信程序在无网络、无宿主挂载、无Docker socket的worker中运行。这是已测试的边界，不是对控制服务或所有系统组件的绝对安全保证。

## 此前25b及更早验证归档

原始证据保存在 Git 忽略的 `runtime/`，发布文档只记录合成数据摘要、哈希与路径。详见 [持续验证记录](LOCAL_VALIDATION_PROGRESS.md) 和 [A01—A48矩阵](LOCAL_ACCEPTANCE_MATRIX.md)。

| 证据 | 当前结果 | 解释 |
| --- | --- | --- |
| 此前25b源码 | `25b1be619b9e6a55ad328ada603aaed7accfef63a1b4856b965f52377d66296a`，源码commit`974edd71a559b64a6c10854b609d3fae6abd9047` | 120个Python文件；后续commit946ce37仅改测试。schema001—012保持、无013；本行不是当前候选 |
| 此前core11 | 1727 PASS、51 SKIP、28 DESELECTED，106.98秒；`runtime/ci/final-core-11.log` / `.xml` | 绑定25b；core09/10各1项fixture错误FAIL日志保留，仅修测试后确定性通过，产品源码未改；skip不算能力通过 |
| 此前09镜像与worker | 全120py同25b；worker09为97 PASS、零skip，258.17秒，`runtime/ci/final-workers-09.log/.xml` | 实际SDK/Docker及产物/隔离选集，含合成模型/HTTP/通道。依赖层复用，非native独立重现 |
| 此前25b同源三性能 | 短答6.200338417秒、接单2.044179791秒、取消1.175423541秒，均30/30正确、0失败，P95保守上界全部PASS；前后25b不变 | `perf-live-final`短答实模、接单真实规则快路且held；`perf-cancel-final`固定provider/真实PG-gateway，旧租约工具均拒绝。非执行吞吐或微信时延 |
| 故障位置归档 | 12个不同注入位置、12个既有JUnit PASS；`runtime/ci/fault-injection-evidence-final.json`，绑定25b/946ce37 | 10项来自Core11的本地PG/文件/夹具，2项来自Workers09真实容器/native SDK；未新增测试执行或通过数，逐项边界见[矩阵短表](LOCAL_ACCEPTANCE_MATRIX.md#故障注入证据) |
| 历史core07 | 1667 PASS、51 SKIP、28 DESELECTED，130.95秒；`runtime/ci/final-core-07.log` / `.xml` | 绑定`6fcb2b39…`，与此前`25b1be61…`分列；skip不是能力通过 |
| 06版冻结core回归 | 1524 PASS、51 SKIP、28 DESELECTED，113.60秒；`runtime/ci/final-core-06.log` / `.xml` | 真实PG；排除render/sandbox/live，skip不算通过。绑定执行源码`ee73a730…`，不自动继承到后续计费源码 |
| 06版源码/镜像一致性 | 宿主、worker、Harness均为`ee73a7302b58a261f14bf854b466aa59928ce762a000f2fd41b35417e6c3d44d` | 120个Python源码文件及依赖锁内容核对一致；`runtime/build/frozen-image-source-proof-06.json`。Harness复用已锁定的SDK/runtime依赖层；同源证明不代替任务验收 |
| 06版真实worker选集 | 97 PASS、零skip，271.17秒；`runtime/ci/final-workers-06.log` / `.xml` | 实际Docker/SDK、隔离与文档/媒体链路，绑定`ee73a730…`；其中合成模型/HTTP/通道不算真实供应商或微信 |
| 后续计费basis变更 | 源码SHA256 `798e6379919b0c68c87c65d724987c6755b73926a8109e9bd6fc525d034559a8`；billing 61 PASS、零skip，3.14秒 | 仅新增可信Owner对账和统一reserve/begin口径，Ruff/format/mypy通过；`runtime/ci/billing-reconciliation.xml`。06任务及镜像没有在此源码重跑 |
| 独立身份/记忆集 | 50组场景加1项固定清单哈希校验，51 PASS | [测试与固定清单](../../tests/memory/test_identity_acceptance.py)；真实PG合成数据，属于L1/L2 |
| 历史Pillow13镜像隔离回归 | 86/86 PASS，227.29秒 | 真实SDK/Docker/文件/OCR/浏览器及混合输入回归；其中模型、HTTP和通道夹具仍属合成，不移植为最终镜像结果 |
| 历史文档镜像回归 | 43/43 PASS，177.96秒；`runtime/build/document-final-regression.txt` | 四格式实际容器、普通模板与遮挡检查；不代替最终镜像的完整SDK/应用/30任务 |
| 真实媒体API | 30/30图片、20/20语音语义样本通过 | 合成图像与真实TTS→ASR；不等于真实录音、微信群收发或人类评分完成 |
| 历史真实本地冒烟 | 计算、Markdown、中文Word 3/3通过 | 较早源码的真实DeepSeek/Harness/worker与本地回执；不替代当前快受理后的代表执行 |
| 此前快受理真实执行03 | 3/3 PASS；`runtime/live/fast-admission-execution-03/`，Source前后25b不变 | 实际Harness/09镜像执行、未held；MD、独立解码核对3页/标题PPT、Python实际ToolReceipt输出391并取得本地接受回执。与原固定30分列 |
| 代表执行历史失败 | 01 helper默认None比较错误，无产品执行；02 MD/PPT PASS、compute因无ToolReceipt FAIL | 02由Engine正确拒绝纯口算；随后objective显式传达required_tools，03真实重验已过。原错误、原生停止及清理回执不改写 |
| 固定30的快路选择覆盖 | `runtime/ci/fast-admission-frozen30-selection.json`：命中0/30 | 纯selection检查绑定`6fcb2b39…`，说明原30任务未被这条窄快路截获；不是重跑、通过率或当前源码任务执行证明 |
| 最新参与策略固定集 | `interaction-release-06`：246/264精确匹配；请求召回215/219（98.1735%）；误介入0/32；禁止写候选0、实际业务写入0；候选P95 2.419秒 | 召回/误介入由实际`service._gate`离线复核，其余13条单列为observe 11、reply 2。固定集反复回归不是未见样本估计；候选延迟不是daemon接收→outbox性能 |
| 30任务固定矩阵 | `runtime-matrix-06`复核汇总26 PASS、4 FAIL / 30（86.6667%）；`summary-after-ocr-review.json`，旧T06整轮status为FAIL | Chart02追加无prompt/gold的云OCR和独立assistant复核；原25/30与本地OCR失败保留。执行源`ee73a730…`、检查阶段含`798e6379…`，明确mixed_product_sources；四项失败及unknown保留，不冒充新源码重跑 |
| 性能历史保留 | 最终25b三性能各有独立30样本；旧01—04接单FAIL、取消01/02 FAIL原样保留 | 05接单2.0965秒仍是同源较早支持证据；历史成功也分列，不拼接或删慢样本 |
| 此前依赖/凭据/包内容 | Python65项0 known vulns；Gitleaks327个可发布文件0leaks；wheel147条/sdist328条私有项均0 | `runtime/final-python-audit-09.json`、`runtime/ci/final-gitleaks-09.log`、`runtime/package-content-check-final-09.json`；限此快照，不扩大到当前候选或系统依赖/L5 |

此前25b冻结版本与发布记录（最新修复候选见[报告](FOUR_FAILURE_REPAIR.md)，最新CI见[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)）：

| 字段 | 当前状态 |
| --- | --- |
| 应用commit / tree、schema / 锁文件摘要 | 源码提交974edd7、后续测试提交`946ce37fb3b1e9350384f17a320dd46eb3ea09f3`及tree`292292ed4928b25386599010bb1dcc06881bff4c`；src25b未变。schema001—012原样，无013。锁SHA256`789451f1e6e282422bc662c04a3e5ed8d8a3057835fb85a376e1759479e8dc59` |
| 此前09版worker / Harness | worker `sha256:350ac6898387c449c988800076c66eda5d3d5b656928843b71e6822dba0daddf`；Harness `sha256:377a2cf136a42e7a625e991326c076f42096cc1be1fecb278df99cc7d0f86bf1` | 两者120py均匹配25b，09 worker97 PASS/258.17秒，不作为当前候选worker结果。旧06的97/271.17秒仍只归ee73 |
| 06版worker / Harness不可变ID及源码对应关系 | worker `sha256:104a97b81c55395ef2d09d26dbe309fa9126032fbb0f0224d391942e4a1c0f43`；Harness `sha256:c72eb6887db3983c01c763d7b29606793163279eb5a9617293f984a0c08f6ec8`；均与宿主`ee73a730…`同源，Pillow13.0.0、SDK/runtime安装版本0.2.2rc2 |
| 30任务run-id、fixture hash、通过分子/30及失败列表 | `runtime-matrix-06`复核后26/30；固定fixture SHA256 `10f8ffbb473b596b409cd7318a39483082f4a5ef9e727808055ea38a4d58dffd`未变；FAIL为DOCX02（provider_unavailable）、PDF03（token预算）、WEB01（严格citation/access记录）、SCHEDULE03（连续required_text未匹配）。逐项边界见[进展](LOCAL_VALIDATION_PROGRESS.md) |
| 参与策略Spec召回率、误介入率及各自分子/分母 | release06：215/219（98.1735%）、0/32；证据为该run的`gated-participation.json`，边界见上表 |
| core / worker / build / secret / dependency结果 | 此前25b core11及worker09通过，build成功；65依赖、327文件扫描和wheel147/sdist328私有0；7a268代码CI四job成功 | 各结果绑定其记录版本；后续提交CI以[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准 |
| 已验证代码CI与最新检查 | remote`7a268eba0e50b33ba4f108f348da3c9c64bf368e`与本地946ce37同tree`292292ed4928b25386599010bb1dcc06881bff4c`；[run43632514186](https://github.com/huchenzi0609-creator/GroupAgent/actions/runs/43632514186)双core/render/worker四job全success | 7a268的历史成功不代表后续head；最新状态见[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)。PR仍draft/base未合并[PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1) |
| 此前25b轮最终账本 | 有效用量上界21.023746 CNY；gross settled55.079718为估算，2808笔settled；unknown2笔0.168044 | `runtime/billing/final-verification-report.json`是07前基线；07止后上界22.516922、unknown3/held0.302090，新Owner basis与修复轮快照见[报告](FOUR_FAILURE_REPAIR.md)。policy50/revision1未变，均非新实扣 |

06版Harness为对既有不可变依赖镜像叠加源码，未宣称全新下载或native独立重现。历史构建快照：worker `sha256:080ff780b4b83ddf08aa0cb2bea2712dc19f4d2bee6c3a7a1843199d82780a53`、Harness `sha256:e26981edd317c6fc78b51d43ba0ae6081ee2e044eab4d8bde5dbcebcda50a364`；43项仅属该历史worker。`interaction-release-06`候选评测仍绑定`88a5999ed5454631b4cf93d4c06a68d6aeebb5b0d9ac53d5b9f0b30c4eb369de`，与原执行ee73的`runtime-matrix-06`及此前25b分别记录。

旧用户报告4.83 CNY及schema1审计111620属于历史，25b轮21.023746等原快照保留。最新用户累计报告11.65 CNY已由Owner schema2审计282332（prev111620）实际应用，基准有效上界12.212140=11.65+原unknown held0.562140；原3974 settled/5 unknown及policy/prices/reservations全量hash未变。as_of采用Root收到答复的2026-11-15 06:21:31 UTC，非用户精确查询时刻、非connector独立验票。之后新调用仍加保守suffix，原50/50 policy/revision1未变，非退款/新额度或逐笔假结算，最新快照见[修复报告](FOUR_FAILURE_REPAIR.md)。

所有真实DeepSeek实验仍共用 `local-acceptance-20261114` 的持久累计50元上限，不因run-id、代理、重启或重试另开额度。本轮允许本地开发验收，不包含生产接入或部署授权。

## 实际运行和CI边界

本地控制环境为 macOS arm64、Python3.13、独立PostgreSQL16；隔离容器位于专用Colima VM（Linux arm64、2 CPU、3 GiB，无host mounts）。[worker证据](../../deploy/worker/README.md) 中的峰值来自小型合成负载，不涵盖完整服务、微信、最大文档、备份及保留文件。不能从本地结果推断原有约2 GiB Linux实例容量足够。

CI core 使用Ubuntu24.04、Python3.11/3.13和独立PG16；render安装真实LibreOffice/Poppler/中文字体；isolated-worker按锁文件构建worker与Harness镜像并实际执行。render与isolated-worker的必需测试若全部跳过或任何跳过，会失败。CI不持有供应商或微信凭据，不自动执行付费API或L5。CI配置存在不等于当前提交已通过。

2026-11-14的静态/公开元数据复核确认：固定Python base是包含Linux amd64/arm64的OCI index（amd64子manifest `sha256:f3dac913ab53b78c64f26d5518fcd7043d5a1a46522d6a493b598c3ed12f62a6`）；[SDK](https://pypi.org/pypi/deepseek-harness-sdk/0.2.2rc2/json) 与 [runtime](https://pypi.org/pypi/deepseek-harness-runtime-bin/0.2.2rc2/json) 的三个wheel hash与锁文件一致。uv离线导出与worker requirements一致，render9项/worker94项严格收集无误；两段零skip检查已对临时pytest生成的JUnit验证能接受通过、拒绝skip/空结果。此批没有执行产品、容器或付费测试，证据在 `runtime/ci/static-audit/`；不计为托管CI或目标amd64运行通过。

历史foundation提交的托管CI已验证过较小功能集，当前扩展提交须重新通过。2026-11-14平台设置检查显示当前私有仓库套餐不能强制执行branch protection；未更改可见性或购买套餐。现行PR/人工合并纪律须如实记录，不能声称main已有技术强制保护。

## 剩余发布门槛

以下保留25b阶段门槛；旧失败与阶段证据保留。当前c411 core18/worker19/镜像同源及Owner新basis已验证，focus22取得必要checks/reviews但raw1 PASS/2 NEEDS_REVIEW未统一改写；新完整full22已正式30/30 PASS，同一源码且无旧结果拼接。最新提交CI以[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，历史CI不移作新head。

| 门槛 | 当前状态与剩余证据 |
| --- | --- |
| G0 | 本地Harness、隔离、文件和恢复有证据；真实微信实验尚未执行，整体未通过 |
| G1 | 此前25b的core11/worker09/镜像同源/build/所列扫描及7a268代码CI已过；最新提交以[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，旧失败及版本保留 |
| G2 | 多个真实模型→本地通道闭环已通过；Spec要求的实际群完整交付未执行 |
| G3 | 原30执行ee73复核26/30及四FAIL保留；当前25b三性能各30/30达标、真实代表执行03为3/3；这不等于A01—A48或L5全通过，全部L5 NOT_RUN |
| G4 | 真实测试群至少七天shadow/有界执行、一次恢复和规则变更：NOT_RUN |
| G5 | 生产角色/预算/保留策略确认、目标构建、备份回滚、真实迁移和单发送者切换：NOT_RUN |

当前不部署、不安装systemd模板、不连接真实账号，也不使用旧数据库验证。完成本地文档和测试后，仍按 [验收规格](../spec/ACCEPTANCE_V0.1.md) 收集外部门槛证据。
