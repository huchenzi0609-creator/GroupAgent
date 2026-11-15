# 本地集成验证记录（进行中）

**CI字体兼容小修：** 完整30/30实模验收绑定c411源码。最终提交另含`5c9c4640ac097c4a4c94b7a9c7a26708eb6ddd5a4ee982c0cb134b9b8880029e`的负数折线图标签避让修复；Linux DejaVu字体已复现原FAIL并验证修复PASS，正数验收样例及恒定/零值PNG字节不变。仅图表相关回归（13项及文档绘图选集172项）和必要CI重新验证，未重跑付费30项，旧CI失败记录保留。

更新：2026-11-15。当前工作分支 `feat/local-readiness`；本记录不是上线批准。

用户已排除独立插画服务与视频处理。保留图片理解、OCR、程序绘图、四种文档、语音、搜索、自然语言任务与有限调度。旧生产机器人、实际微信群、旧数据库和服务器均未用于本轮测试。

**最新修复见[四项失败修复报告](FOUR_FAILURE_REPAIR.md)。** 当前c411/122py（freeze18）的core18为2134 PASS/52 SKIP/31 DESELECTED/158.30秒，worker19为110 PASS/零skip/284.07秒；mypy122/Ruff/format285、images18同源通过，新增第52项core skip已在worker覆盖。focus22三任务均completed，Chart02全11硬checks/像素绑定通过、双WEB本轮sidecar review PASS；原raw1 PASS/2 NEEDS_REVIEW保留，未作统一3/3汇总。full22已在同一c411源码、images18和固定checkerfa下完成30/30 PASS；必要语义/视觉复核均绑定本轮产物，最终零付费汇总未增加调用。Owner audit282332已实际应用用户累计11.65基准，原50/50 policy与revision1不变，unknown5/held0.562140继续保留；最终有效上界17.266165含后续suffix保守估算与原unknown预留，非控制台实扣。

## 此前25b及更早证据归档

| 范围 | 实际结果与边界 |
| --- | --- |
| 此前25b冻结源码 | src SHA256`25b1be619b9e6a55ad328ada603aaed7accfef63a1b4856b965f52377d66296a`；源码974edd7/测试946ce37，schema001—012保持。此处归档不替代当前候选；最新提交CI见[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)。 |
| 此前core11 | 1727 PASS、51 SKIP、28 DESELECTED，106.98秒；`runtime/ci/final-core-11.xml/log`，绑定25b。core09/10各1项fixture错误FAIL原日志保留，仅修测试后通过；不计skip为能力通过。 |
| 此前09镜像与worker | worker350ac689…、Harness377a2cf1…全120py与25b一致；完整ID见[实现状态](IMPLEMENTATION_STATUS.md)，`frozen-image-source-proof-09.json`。worker09为97 PASS/零skip/258.17秒，`runtime/ci/final-workers-09.log/.xml`。不移作当前候选结果。 |
| 此前快受理执行03 | 3/3 PASS；`runtime/live/fast-admission-execution-03/`。真实Harness/09镜像执行、未held；MD、3页/标题PPT、Python391均有本地接受，绑定25b。与原30和新08分列。 |
| 最终同源短答/接单 | `perf-live-final`各30/30、0失败，P95保守上界短答6.200338417秒PASS、接单2.044179791秒PASS；前后25b不变。短答真实Model，长任务为真实规则快路、无预Model账单且held执行lane，执行另以03三例证明。 |
| 最终同源取消 | `perf-cancel-final`：30/30全正确、0失败，旧租约工具调用均被拒绝；P95保守上界1.175423541秒PASS，前后25b不变。固定provider、真实PG/gateway，无Model费用，不冒称真实Model取消。 |
| 历史core07 | 1667 PASS、51 SKIP、28 DESELECTED，130.95秒，`runtime/ci/final-core-07.log` / `.xml`；绑定`6fcb2b39…`。当前25b以core11单列，不能移用旧计数。 |
| 06版冻结core回归 | 1524 PASS、51 SKIP、28 DESELECTED，113.60秒；`runtime/ci/final-core-06.log` / `.xml`。真实PostgreSQL，排除render/sandbox/live；skip不算通过，后续源码变更不自动继承此结果。 |
| 06版源码与镜像 | 冻结SUT本地commit `f83a9173a0dea2aa02afa3230c88eb3d406cc276`；宿主、worker和Harness均为`ee73a7302b58a261f14bf854b466aa59928ce762a000f2fd41b35417e6c3d44d`，120个Python源码文件及依赖锁内容一致，记录在`runtime/build/frozen-image-source-proof-06.json`。Harness复用已锁定SDK/runtime依赖层；没有本轮全新安装或独立重现native binary的证据。同源核对不代替任务评分。 |
| 06版真实worker回归 | 97 PASS、零skip，271.17秒；`runtime/ci/final-workers-06.log` / `.xml`。真实Docker/SDK与文件/媒体/隔离链路，执行源码`ee73a730…`；其中合成模型/HTTP/通道仍非微信或真实供应商任务。 |
| 后续计费口径校准 | 源码`798e6379919b0c68c87c65d724987c6755b73926a8109e9bd6fc525d034559a8`；billing真实PG 61 PASS、零skip，3.14秒，Ruff/format/mypy通过。只新增可信Owner累计账单对账，保留原policy/price/reservation；`runtime/ci/billing-reconciliation.xml`。06执行证据不转记为新源码重跑。 |
| 独立身份/记忆固定集 | 50组真实PG合成场景与1项清单哈希校验，共51 PASS；[固定清单及测试](../../tests/memory/test_identity_acceptance.py)。不等于实际微信名单/群身份验收。 |
| 历史镜像隔离回归 | 86/86 PASS，227.29 秒：真实 Harness SDK、Docker 边界、四格式样本、OCR/浏览器及输入链路；其中模型/HTTP夹具不能算真实供应商理解，也不移植为最终镜像结果。 |
| 真实媒体 API | 30/30 合成图片、20/20 合成语音语义样例通过；语音使用真实 TTS→ASR，不把音调当语言。未覆盖实际群传输及真实噪声录音。 |
| 真实闭环冒烟 | Python计算、Markdown交付、中文Word交付 3/3，通过真实 DeepSeek、Harness、隔离进程和本地消息通道。Word两页另做视觉抽查，未发现缺字、空白页、溢出或遮挡。 |
| 最新自然语言固定集 | `interaction-release-06`：264条独立Agent预标注样例，246 PASS、18 FAIL；请求召回215/219（98.1735%），误介入0/32，其余13条observe 11/reply 2；禁止写候选0、实际业务写入0。候选P95 2.419秒，不是daemon接收至outbox性能。 |
| 真实恢复与隐私 | PostgreSQL dump/空库恢复、恢复隔离、外置fsync删除日志、原始资料到期、已验证文档独立保留、版本级物理清理、任务字段来源及删除权限均有合成数据回归。实际旧库尚未迁移。 |
| 此前依赖、凭据与打包 | Python65项0 known vulns、327文件0leaks、wheel147/sdist328私有0，原09快照保留。不是当前候选扫描或打包结果，也不涵盖系统包/真实通道。 |

release06的`manifest.json`记录源码SHA256为`88a5999ed5454631b4cf93d4c06a68d6aeebb5b0d9ac53d5b9f0b30c4eb369de`，运行前后未变。`result.json`保留原始候选结果，`gated-participation.json`按真实`service._gate`离线计算固定219/32分母；使用默认natural模式、无持久群/静默覆盖且独立窗口主动计数为0。这些结果不代表未见样本泛化、人类最终标注、实际业务成功率或规定性能SLO。

历史对比单独保留：首轮206/264，repair02与repair03均232/264；`interaction-final-04`为247/264、候选P95 2.24秒；`interaction-release-05`为242/264、21 FAIL与1 ERROR。以上均不作为当前参与结果，不与release06拼接，也不修改gold抬分。

Pillow变更依据：[官方13.0.0发布说明](https://pillow.readthedocs.io/en/stable/releasenotes/13.0.0.html)。这不代表所有系统组件永远无漏洞。

## 综合任务验收中的失败也保留

30项任务在 `tests/fixtures/runtime_acceptance_cases.json` 预先冻结，涵盖文档、计算、图表、图片、搜索、语音、调度及版本编辑。只以实际字节、工具回执、数据库和投递状态评分。

- `runtime-matrix-01`：前四项产物已由应用完成，本地交付后验收器因2 MiB超出进程参数的2,000,000字节上限而报错。已停止付费运行，未把缺失校验算通过。修复验收器后用既有产物及合成文件做8项真实容器解码检查，全部通过，没有重新付费生成这些检查输入。
- `runtime-matrix-02`：前六项Word/PPTX完成，五项程序检查通过，一项内容结论待独立审阅；后续暴露真实上下文膨胀问题，未完成的任务如实为失败/未执行。内部来源指纹和运行细节现已移出公开模型投影，同时补全文件目录的来源依赖，并对序列化大小、事件与任务来源数量做界限检查。修复后需重新执行完整矩阵。
- `runtime-matrix-03`：最新已保存的零成本复核汇总为20/30 PASS、10 FAIL；原始结果和复核版本均保留。WEB02复核为独立assistant审阅，不冒充human adjudication。该轮存在混合源码，不能用它替代最终冻结运行。
- `runtime-matrix-04`：最终保留汇总`summary-final-preserved.json`为18 PASS、11 FAIL、1 NOT_RUN，已包含独立审阅。文档暂停及图片OCR失败保留在该轮记录中，不因05版修复或重跑而改写。
- `runtime-matrix-05`：最终保留汇总`summary-preserved.json`为21 PASS、9 FAIL，固定分母30；该轮源码`737027aa…`与05版镜像的证据保留。失败不会因06版运行而改写。
- `runtime-matrix-06`：`summary-after-ocr-review.json`为26 PASS、4 FAIL / 30（86.6667%），整轮status保留FAIL。原执行源码为`ee73a730…`；后续检查环境含计费源码`798e6379…`，汇总明确标记`mixed_product_sources=true`并保存checker/runner revision，不冒充单一最终构建或未知样本盲测。复核前25/30的`summary-before-ocr`与本地OCR失败全部保留。

Chart02的原产物追加一次真实云OCR，接口未获得prompt/gold，提取W1—W4实际数据；readback和bill=0.0015的回执已settled。独立assistant复核后该项通过，汇总本身不产生付费调用或业务写入；固定gold hash仍为`10f8ffbb473b596b409cd7318a39483082f4a5ef9e727808055ea38a4d58dffd`。这不是人类审阅证明。其余四项失败保持原判定：

| 用例 | 保留的FAIL原因 |
| --- | --- |
| RA-DOCX-02 | `provider_unavailable`导致执行暂停；0.108410元估算预留保持unknown，部分产物不计成功 |
| RA-PDF-03 | 工作token预算耗尽；未因后续金额对账改写既有失败 |
| RA-WEB-01 | 严格citation/access记录项未满足；不降低证据要求 |
| RA-SCHEDULE-03 | 实际正确通知“检查一下合成实验清单”，未匹配gold要求的连续required_text；保留FAIL，不改预期字符串 |

完整原始证据只保存在Git忽略的 `runtime/` 下。不会以覆盖失败轨迹、清空费用或更改预期结果的方式获得通过记录。

## 快受理与性能的版本边界

明确自包含的新Work可以直接受理，Goal保留完整原文，可确定的格式、页数、逐字标题继续进入原验证；其它请求仍走原Model。上下文、权限/quiet门控、来源快照、最终事务、预算与工具回执检查都保留。性能修复只做上下文读去重、缺失本store文件的否决预检，以及该topic-membership查询的`prepare=False`，未调低阈值或改计时定义。

| run | 接单30项结果 | 源码与范围 |
| --- | --- | --- |
| perf-live-01 | 功能正确；P95上界16.51728625秒，FAIL | `798e6379…`，受理前等待Model完整候选，固定样本含付费重试 |
| perf-live-02 | 功能正确；P95上界5.69251825秒，FAIL | `6fcb2b39…`，真实快受理；该轮短答30/30、P95上界15.0473秒PASS |
| perf-live-03 | 功能正确；P95上界5.601984541秒，FAIL | `fbb9ab51…`，上下文缓存后；失败记录原样保留 |
| perf-live-04 | 功能正确；P95上界5.489573125秒，FAIL | `a579d9a7…`，当前store缺失文件预检后；失败记录保留 |
| perf-live-05 | 30/30正确；P95上界2.096471917秒，PASS | `25b1be61…`，前后同源；只证明该轮明确新工作接单，不覆盖执行吞吐或微信传输 |
| perf-live-final | 短答30/30、接单30/30，均0失败；P95上界6.200338417/2.044179791秒，均PASS | 前后同25b；短答实模、接单真实规则快路且held执行lane，完整实际执行以03三例分列 |

各轮使用真实`app.run`和commit-bracket保守上界，短答/接单/取消阈值始终20/5/2秒。当前25b最终三项分别30/30、0失败，P95上界6.200338417/2.044179791/1.175423541秒均PASS；取消使用固定provider和真实PG/gateway。01—04接单与取消01/02的FAIL保留，不删慢样本、不把历史版本拼入最终样本。

`fast-admission-frozen30-selection.json`对原固定30条的纯选择命中0，绑定6f；不是新的30任务执行。首轮执行01的helper默认None比较错误未进入产品执行，Root原生停止清理；执行02的MD/PPT通过、compute因无python_compute回执被Engine拒绝纯口算，FAIL保留。Toolkit随后在objective显式传达required_tools，当前执行03以真实Harness/09镜像取得3/3 PASS。旧helper错误、停止及控制回执不被03覆盖。

一次额外授权的实际合成group全路径profile只创建一个Work，随后同Owner原生引用停止；Work已cancelled，两投递回执均经原Dispatcher/LocalChannel accepted，其它Work/Actor与费用未变。证据`runtime/ci/admission-fullpath-profile-01/`；这是诊断轨迹，不混入30项SLO分母。未应用的索引实验完整保存在`runtime/ci/topic-index-draft-01/`，001—012保持冻结。

## 费用与仍待完成的门槛

本节21.023746及其明细属于此前25b轮/07前历史口径，原数据保留。当前已实际应用用户新累计11.65 CNY的Owner schema2审计282332；基准上界12.212140包含保留的5 unknown/held0.562140，随后suffix仍保守估算。原50 policy/revision1未变，新完整full22正式30/30 PASS、最终有效上界17.266165，见[修复报告](FOUR_FAILURE_REPAIR.md)。

用户报告：截至2026-11-14 23:00（Asia/Shanghai）前，DeepSeek控制台显示测试实际总扣费为 **4.83元**；这是用户报告的控制台金额（`user_reported_deepseek_console`），不含该时点之后的调用。记录保存在 `runtime/billing/console-report-20261114.json`。

同一历史前缀的 **39.054016元** 是计量用量与保守价格表计算的估算，账本字段`actual`仍表示这种估算。Root通过可信Owner API，以截至2026-11-14 22:59:59（Asia/Shanghai）的2203笔terminal记录核对count/gross后，追加`user_reported_provider_console`审计111620，将有效预算口径中的此前缀替换为报告的4.83元；原reservation、gross、价格和全部预留不改。daily/total policy均50.000000、revision1未变，未新建账本或额度。

历史应用快照（2026-11-15 00:36:38，Asia/Shanghai）为gross settled48.907864、unknown held0.108410，有效累计上界14.792258 CNY，证据`runtime/billing/applied-console-reconciliation.json`；它不含后续OCR/性能调用，现由下面最终快照补齐。该口径依赖Owner报告准确完整，不是新的供应商实扣；同一50元预算不因对账退款、重置或另开额度，历史预留合计也不作收费额。

此前25b轮最终快照`runtime/billing/final-verification-report.json`（2026-11-15 03:42:30，Asia/Shanghai）记录2808 settled、gross55.079718保守估算、unknown2/held0.168044、上界21.023746 CNY；仅属旧4.83报告口径，不覆盖最新用户累计11.65。新报告证据为`runtime/billing/console-report-20261115-1165.json`，实际Owner应用为`runtime/billing/console-reconciliation-20261115-1165.json`：schema2 audit282332/prev111620、基准上界12.212140、3974 settled/5 unknown不变；原policy/prices/reservations全量hash一致，旧46.821060上界及gross80.482936估算保留，非退款/重置额度/伪settle。

对账只供受信本地Owner API，不暴露给模型/自然语言/群通道。旧默认含unknown仍拒绝；新显式opt-in累计模式仅替换settled前缀估算，所有unknown/预留继续held，即使用户累计可能已含其真实费用也保守覆盖。报告as_of取Root接收“当前”答复的2026-11-15 06:21:31 UTC，用户未给精确查询时刻、期间paid/active已空；不是独立connector验票。审计仅应用级追加，prefix/hash异常失败关闭，不承诺DB强制不可变；旧V1审计及报告保留。

历史取消03（798e）保守P95 **1.1299235秒**PASS、并行负载01/02的2.228/3.320秒FAIL均保留；最终取消采用同25b的独立30样本和1.175423541秒，不用历史样本补当前分母。最终费用上界已按上述快照记录，不等同新实扣。

[PR #8](https://github.com/huchenzi0609-creator/GroupAgent/pull/8)仍draft，base为未合并[PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1)。已验证代码remote`7a268eba0e50b33ba4f108f348da3c9c64bf368e`与本地946ce37同tree292292ed…，[CI43632514186](https://github.com/huchenzi0609-creator/GroupAgent/actions/runs/43632514186)双core/render/isolated-worker四job全success。core11、worker09、09同源、执行03、最终三性能与扫描/账本各有绑定版本；最新提交CI以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，历史成功不预判未来head。

全部L5、真实微信文件/语音/回声/登录恢复、目标Linux amd64服务器与容量、至少7天试运行均为NOT_RUN；本地通过不能替代这些门槛，也不构成全功能完成或上线批准。

855dd094绿仅属修复前历史。当前c411 focus22取得必要checks/reviews、原raw1 PASS/2 NEEDS_REVIEW保留；full22正式30/30 PASS，最终有效上界17.266165；账本及发布证据见[修复报告](FOUR_FAILURE_REPAIR.md)，最新CI见[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)。
