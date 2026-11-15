# 本地分层验收矩阵（进行中）

更新：2026-11-15。目标规格：[Spec v0.1](../SPEC_V0.1.zh-CN.md) 与 [A01—A48](../spec/ACCEPTANCE_V0.1.md)。本轮仅使用合成数据、隔离开发PG/容器及本地消息通道，未访问真实群、旧库或生产。独立插画服务和所有视频处理均排除；A14仍须验证视频不会被分析。

本表是证据映射，不是48项全部通过的声明。各层分别评分，不得把pytest总数、模型候选分数或文件样本数直接当A场景分母。所有L5均为NOT_RUN；即使本地子项通过，也不能使需要真实通道的整项验收变为PASS。

**最新修复入口：[FOUR_FAILURE_REPAIR.md](FOUR_FAILURE_REPAIR.md)。** 当前c411/122py（freeze18）的core18为2134 PASS/52 SKIP/31 DESELECTED/158.30秒，worker19为110 PASS/零skip/284.07秒；mypy122/Ruff/format285、images18同源通过，新增第52项core skip已在worker覆盖。focus22三任务均completed，Chart02全11硬checks/像素绑定通过、双WEB本轮sidecar review PASS；原raw1 PASS/2 NEEDS_REVIEW保留，未作统一3/3汇总。full22已在同一c411源码、images18和固定checkerfa下完成30/30 PASS；必要语义/视觉复核均绑定本轮产物，最终零付费汇总未增加调用。Owner audit282332已实际应用用户累计11.65基准，原50/50 policy与revision1不变，unknown5/held0.562140继续保留；最终有效上界17.266165含后续suffix保守估算与原unknown预留，非控制台实扣。

## 证据层与已执行批次

| 层 | 含义与本轮边界 |
| --- | --- |
| L1 | 纯逻辑/状态/参数/权限规则；下表链接到代表性单元或属性测试 |
| L2 | 真实PG、显式合成HTTP/模型/通道的契约与集成；真实PG不使微信HTTP夹具成为真实微信 |
| L3 | 固定264窗口、真实模型的候选理解；没有施行业务写入，不能证明真正执行或正确内容 |
| L4 | 真实SDK/隔离worker/媒体API/文件字节和受控故障；本地接受回执不等于手机收到 |
| L5 | 实际测试账号/群、手机显示下载播放、回声、断线重连；本轮全部NOT_RUN |

| 代号 | 定位与现有结果 | 可以支持的结论 |
| --- | --- | --- |
| R | 此前core11：1727 PASS、51 SKIP、28 DESELECTED，106.98秒，源码25b；`runtime/ci/final-core-11.xml/log` | 09/10各一项fixture错误FAIL保留，仅修测试后通过。历史core07=1667绑定6f、06=1524绑定ee；不移作当前候选结果 |
| ID | [独立身份/记忆固定清单](../../tests/memory/test_identity_acceptance.py)：50组场景加1项哈希校验，51 PASS；清单hash `503c4042ad0ce25d4c0cf34cce7927ed4c73150bb929809769e3fabc9b688a6b` | 真实PG合成身份/记忆与负责人边界，属于L1/L2；不替代原生微信成员字段和真实群测试 |
| W | 06版真实worker选集97 PASS、零skip，271.17秒；`runtime/ci/final-workers-06.log` / `.xml`，执行源码`ee73a730…` | 真Docker/SDK、文档、OCR/浏览器/输入与隔离边界；选集含合成模型/HTTP/通道，不能称97次真实供应商任务或微信验收 |
| W9 | 此前09 worker97 PASS、零skip，258.17秒；全120py匹配25b，worker350ac689…、Harness377a2cf1… | 旧日志`runtime/ci/final-workers-09.log/.xml`和完整ID保留；不是d74a的repair-workers-03 |
| W0 | 历史Pillow13镜像86/86混合回归，227.29秒；[CI isolated-worker选集](../../.github/workflows/ci.yml) | 历史证据保留，不移植为后续计费源码的结果 |
| W2 | 历史worker `080ff780…` 的43/43实际容器文档回归，177.96秒；`runtime/build/document-final-regression.txt` | 4×10代表样本及普通模板/遮挡检查；不把历史镜像结果移植为最终镜像整套通过 |
| M | 真实媒体API：30/30合成图片、20/20真实TTS→ASR语义样本；`runtime/live/`私有证据 | 合成图片关键问答和合成语音意图。无真实群传输、噪声录音或人类标注复核结果 |
| S | 历史真实本地冒烟：Python计算、MD交付、中文DOCX交付3/3 | 较早版本真模型/Harness/worker/本地回执，不代替当前快受理后的代表执行 |
| E | 此前执行03真实Harness/09镜像3/3 PASS：MD、独立3页/标题PPT、Python实际ToolReceipt输出391及本地接受 | 未held、源25b；旧helper/compute失败保留。此3既不是原30分母，也不是新08执行 |
| I | 最新`runtime/live/interaction-release-06/`；[固定集](../../tests/fixtures/interaction_cases.json)，hash `5922f43451acfea2dfab66069bf5cbb26a5ab0d1aec10c763ca7c0a009c53730` | 246/264精确匹配；实际门控离线复核召回215/219（98.1735%）、误介入0/32；禁止写候选0、业务写入0、候选P95 2.419秒。下表列类别关联，不暗示该类别全部PASS |
| T | [固定30任务](../../tests/fixtures/runtime_acceptance_cases.json)，hash `10f8ffbb473b596b409cd7318a39483082f4a5ef9e727808055ea38a4d58dffd`未变；`runtime/live/runtime-matrix-06/summary-after-ocr-review.json` | 26 PASS、4 FAIL / 30（86.6667%），旧T06整轮status为FAIL；Chart02补无prompt/gold的云OCR/独立assistant复核。原25/30和所有本地OCR失败保留；四项FAIL及unknown见[进展](LOCAL_VALIDATION_PROGRESS.md) |
| P | 前后同25b：最终短答/接单/取消各30/30、0失败，P95保守6.200338417/2.044179791/1.175423541秒均PASS | `perf-live-final`短答实模、长任务真实规则快路且held；`perf-cancel-final`固定provider/真实PG-gateway拒绝旧租约。旧FAIL保留，非执行吞吐/微信时延 |
| B | 后续计费源码`798e6379…`，`runtime/ci/billing-reconciliation.xml`，61 PASS、零skip | 可信Owner累计对账，reserve/begin统一容量；原估算/unknown/50元policy不改；审计仅应用级追加，不是DB强制不变或connector验票 |
| F | `runtime/ci/fault-injection-evidence-final.json`：12个不同故障位置、12个既有JUnit PASS，源码25b/测试提交946ce37 | Core11的10项含真实PG/本地文件及明确夹具；Workers09的2项为真实本地容器。逐项边界见[故障短表](#故障注入证据)，不计参数变体或新增测试运行 |

I的源码SHA256为`88a5999ed5454631b4cf93d4c06a68d6aeebb5b0d9ac53d5b9f0b30c4eb369de`，运行前后未变。召回与误介入分别使用固定219/32分母，余下13条为observe 11、reply 2，不混入这两个分母。门控复核使用默认natural模式、无持久群/静默覆盖、独立窗口主动计数为0；固定合成标签不是人类最终裁定或未见样本。候选P95不计作daemon性能。旧`interaction-final-04`只作为[历史对比](LOCAL_VALIDATION_PROGRESS.md)，不再代表当前结果。

T06原执行SUT本地commit为`f83a9173a0dea2aa02afa3230c88eb3d406cc276`，宿主与06镜像源码SHA256均为`ee73a7302b58a261f14bf854b466aa59928ce762a000f2fd41b35417e6c3d44d`；120个Python源码文件和依赖锁一致，证据`runtime/build/frozen-image-source-proof-06.json`。后续检查环境含计费源码`798e6379919b0c68c87c65d724987c6755b73926a8109e9bd6fc525d034559a8`，汇总明确保留`mixed_product_sources=true`和checker/runner revision；26/30不是此新源码重跑、单一最终构建或未知样本盲测。Harness复用已锁定SDK/runtime依赖层，没有本轮全新安装/native复现证据。历史04的18 PASS、11 FAIL、1 NOT_RUN与05的21 PASS、9 FAIL独立保留，不拼接。

本表25b证据对应974edd7/946ce37；当前c411 focus22三任务completed/必要checks与sidecar review通过，原raw1 PASS/2 NEEDS_REVIEW保留，未统一3/3；新完整full22正式30/30 PASS，见[修复报告](FOUR_FAILURE_REPAIR.md)。旧case不拼入新run；schema001—012、gold/PASS阈值未改，checkerfa0122从新run起点固定，旧checker/修订保全。最新CI见[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)。

下表L1/L2链接说明可运行的测试入口及其子行为，不凭源码存在判PASS。T只支持所列本地固定任务的结果；没有对应完整场景证据的子项仍NOT_RUN。整行最终状态均为IN_PROGRESS，全部L5为NOT_RUN。

## A01—A17：参与、身份与媒体

| 场景 | L1/L2代表证据与范围 | L3候选证据 | L4实际证据及缺口 | L5 |
| --- | --- | --- | --- | --- |
| A01 无前缀交办 | L2：[provider](../../tests/conversation/test_provider.py)、[app](../../tests/runtime/test_app.py) 无前缀/去重工作 | I：bot_work | S三项；T26/30，四项FAIL保留，不泛化到任意交办 | NOT_RUN |
| A02 不抢别人任务 | L1/L2：[participation](../../tests/conversation/test_participation.py) 明确叫另一成员不调模型；owner分工单独识别 | I：human_target_member、owner_assignment | NOT_RUN完整多人讨论执行场景 | NOT_RUN |
| A03 普通闲聊静默 | L1/L2：[participation](../../tests/conversation/test_participation.py)、[quiet](../../tests/runtime/test_application_controls.py) 静默与主动通知 | I：ordinary_discussion、tentative_or_joking | NOT_RUN长期真实节奏；不是以候选分数代替误介入率 | NOT_RUN |
| A04 多主题与指代 | L2：[memory](../../tests/memory/test_memory.py) 原生引用/私有主题；[context](../../tests/runtime/test_context_privacy.py) 受限目录与来源 | I：multi_topic_reference、ambiguity | NOT_RUN两人同时修改两份真实交付物的完整场景 | NOT_RUN |
| A05 控制优先 | L2：[controls](../../tests/runtime/test_application_controls.py)、[supervisor](../../tests/execution/test_supervisor.py) 慢模型期间取消/安全检查点 | I：work_control | 当前取消final同25b，30/30正确拒旧租约、P95上界1.175423541秒；固定provider/真实PG-gateway，不是实际微信或真实Model取消 | NOT_RUN |
| A06 自身回声 | L2：[coordinator](../../tests/test_coordinator.py)、[inputs](../../tests/runtime/test_inputs.py) 接受回执与同账号真人区别、语音回声不下载 | NOT_RUN真实回声回放 | 本地/夹具回声已测；NOT_RUN实际平台回传 | NOT_RUN |
| A07 同名及改名 | L2：[members](../../tests/memory/test_members.py) 同名/改名不转移任务所有权；ID固定50组含身份边界 | I：memory_and_identity | NOT_RUN原生群名单/改名链路；ID的51项通过只计L1/L2 | NOT_RUN |
| A08 名单与例外 | L2：[members](../../tests/memory/test_members.py) 启动一次、失败不清空、群昵称优先、人工例外 | I：memory_and_identity的有限候选 | NOT_RUN实际名单字段完整性 | NOT_RUN |
| A09 角色不能自封 | L2：[authority](../../tests/test_persistence_authority.py)、[team tools](../../tests/runtime/test_team_tools.py) 可信actor/逐步权限 | I：authority_boundaries、untrusted_instruction | W无凭据worker；NOT_RUN所有恶意文档→真实模型→写入组合 | NOT_RUN |
| A10 群隔离 | L2：[memory](../../tests/memory/test_memory.py)、[artifact store](../../tests/artifacts/test_store.py)、[gateway](../../tests/execution/test_gateway.py) 组/工作范围拒绝 | I：authority_boundaries | W执行环境无群外宿主文件；实际多群通道NOT_RUN | NOT_RUN |
| A11 图片理解 | L2：[media contract](../../tests/media/test_service_contract.py)、[inputs](../../tests/runtime/test_inputs.py) 对应图片字节/作者/引用 | I：media_and_video仅路由 | M30图片；W真实解码/OCR；T图片任务3/3，不覆盖任意真实群图片 | NOT_RUN |
| A12 图片不可用 | L1/L2：[validation](../../tests/media/test_validation.py)、[inputs](../../tests/runtime/test_inputs.py) 损坏/失效/待下载/撤回不造内容 | I：media_and_video的有限候选 | W字节拒绝；实际通道失效下载NOT_RUN | NOT_RUN |
| A13 引用与转发媒体 | L2：[media contract](../../tests/media/test_service_contract.py)、[channel](../../tests/channels/test_agentwechat.py) 来源/次序与不可用句柄 | I：multi_topic_reference、media_and_video | W输入复制/来源链；实际转发资源可用性NOT_RUN | NOT_RUN |
| A14 视频不分析 | L1/L2：[normalization](../../tests/channels/test_content_normalization.py)、[inputs](../../tests/runtime/test_inputs.py)、[triggers](../../tests/scheduling/test_event_triggers.py) 元数据/伪装文件拒绝 | I：media_and_video | NOT_RUN实际平台视频消息；字节拒绝证据在L1/L2 | NOT_RUN |
| A15 语音转写 | L2：[ASR provenance](../../tests/memory/test_audio_provenance.py)、[inputs](../../tests/runtime/test_inputs.py) 原作者/持久缓存/无重复付费 | I：media_and_video仅已转写语义 | M20；W音频归一化+mock ASR；T固定ASR任务1/1 | NOT_RUN |
| A16 语音关键歧义 | L1/L2：[validation](../../tests/media/test_validation.py)、[team tools](../../tests/runtime/test_team_tools.py) 姓名/日期不确定进入waiting_input且零任务写入 | I：ambiguity、media_and_video | NOT_RUN真实低质量录音→澄清完整样本 | NOT_RUN |
| A17 按需语音输出 | L2：[media contract](../../tests/media/test_service_contract.py)、[media send](../../tests/channels/test_media_send.py) 仅显式请求、WAV和回执 | I：media_and_video仅意图 | M的TTS→ASR；T两项TTS交付2/2，本地接受非手机播放 | NOT_RUN |

## A18—A35：产物、任务与可靠性

| 场景 | L1/L2代表证据与范围 | L3候选证据 | L4实际证据及缺口 | L5 |
| --- | --- | --- | --- | --- |
| A18 Markdown修改 | L1/L2：[documents](../../tests/artifacts/test_documents.py)、[tool contracts](../../tests/artifacts/test_tool_contracts.py) 定点编辑、版本和可修正参数错误 | I：accepted_work_update的有限候选 | W每格式10样本；S生成交付；T MD3/3、版本编辑2/2，仅固定声明范围 | NOT_RUN |
| A19 Word模板与编辑 | L1/L2：[extended documents](../../tests/artifacts/test_extended_documents.py) 中文/图/表、模板和定点保留 | NOT_RUN完整模板语义回放 | W实际渲染/普通模板；S中文Word；T DOCX2/3，02 provider_unavailable FAIL | NOT_RUN |
| A20 PDF文本与扫描 | L1/L2：[documents](../../tests/artifacts/test_documents.py)、[extended](../../tests/artifacts/test_extended_documents.py) 页操作/明确重建/原件 | I：media_and_video仅入口 | W扫描PDF OCR/文本/渲染；T PDF2/3，03 token预算FAIL | NOT_RUN |
| A21 PPTX交付 | L1/L2：[extended](../../tests/artifacts/test_extended_documents.py) 10页/图表/模板结构 | I：bot_work仅入口 | W 10页实际渲染及布局检查；T PPTX3/3，不承诺任意模板保真 | NOT_RUN |
| A22 文件并行冲突 | L1/L2：[store](../../tests/artifacts/test_store.py) 并发更新唯一赢家、expected_version与旧指针保留 | NOT_RUN模型并发编辑回放 | W不可变字节；NOT_RUN两工作端到端冲突场景 | NOT_RUN |
| A23 格式与上限拒绝 | L1/L2：[documents](../../tests/artifacts/test_documents.py)、[validation](../../tests/media/test_validation.py)、[inputs](../../tests/runtime/test_inputs.py) 宏/加密/损坏/集合上限 | I：media_and_video的有限候选 | W实际解码/OCR/工作区限制；所有最大准入负载容量NOT_RUN | NOT_RUN |
| A24 不再200字截断 | L1/L2：[coordinator](../../tests/test_coordinator.py)、[dispatch](../../tests/runtime/test_dispatch.py) 长回复完整分段/重启按部分核对 | I：bot_work仅目标 | S真实文件交付；实际平台长消息显示NOT_RUN | NOT_RUN |
| A25 搜索证据 | L2：[research](../../tests/research/test_research.py)、[html profile](../../tests/research/test_html_profile.py) 来源/时间/摘要与正文区分 | I：bot_work仅入口 | W受控离线浏览器；T研究1/2，WEB01严格citation/access记录FAIL | NOT_RUN |
| A26 检索失败 | L2：[research](../../tests/research/test_research.py)、[browser](../../tests/research/test_browser.py) 空/断网/超时/挑战/截断可见 | NOT_RUN真实模型检索失败回放 | W挑战页明确blocked不绕过；真实供应商异常组合NOT_RUN | NOT_RUN |
| A27 人类任务与Agent分离 | L2：[app](../../tests/runtime/test_app.py)、[team tools](../../tests/runtime/test_team_tools.py) TeamTask/WorkOrder/完成权限独立 | I：self_claim、bot_work、own_task_update | S实际WorkOrder交付；T26/30不等于人类TeamTask完成或本场景全覆盖 | NOT_RUN |
| A28 无机械二次确认 | L2：[app](../../tests/runtime/test_app.py)、[provider](../../tests/conversation/test_provider.py) 合法自领一次提交、重复幂等 | I：self_claim、own_task_update | S直接交办；真实模型→TeamTask落库完整量化未汇总 | NOT_RUN |
| A29 缺参与越权 | L2：[provider](../../tests/conversation/test_provider.py)、[team tools](../../tests/runtime/test_team_tools.py) 缺时间/同名/他人任务与角色核对 | I：ambiguity、deadline_edges、authority_boundaries | W gateway范围拒绝；未将I的零禁止候选计作实际写入测试 | NOT_RUN |
| A30 owner分配 | L2：[task provenance](../../tests/memory/test_task_provenance.py)、[team tools](../../tests/runtime/test_team_tools.py) 操作者与负责人分开 | I：owner_assignment | NOT_RUN真实模型分配→完整跨重启结果评分 | NOT_RUN |
| A31 中途改方向 | L2：[supervisor](../../tests/execution/test_supervisor.py)、[work revisions](../../tests/ops/test_work_revisions.py) goal revision/fence/累计预算/验收条件 | I：accepted_work_update | W Harness追加上下文；完整应用改目标交付NOT_RUN | NOT_RUN |
| A32 排队及响应 | L2：[supervisor](../../tests/execution/test_supervisor.py)、[controls](../../tests/runtime/test_application_controls.py) 跨进程重型槽、控制独立 | I候选P95不计本项 | P当前25b最终短答/接单各30/30、P95上界6.2003/2.0442秒PASS；held lane不覆盖重型执行吞吐与完整容量 | NOT_RUN |
| A33 崩溃与租约 | L2：[supervisor](../../tests/execution/test_supervisor.py)、[state](../../tests/execution/test_engine.py) 过期租约/安全checkpoint；F含10项Core11故障位置 | NOT_RUN真实Model故障回放 | F共12位置/12 PASS；FI10真实worker取消、FI11真实native SDK断管道（模型为夹具），其余环境逐项列明；不扩大为通用无重复副作用保证 | NOT_RUN |
| A34 发送结果不明 | L2：[dispatch](../../tests/runtime/test_dispatch.py)、[outbox races](../../tests/test_persistence_outbox_races.py) 原键核对、逐部分恢复和隔离 | NOT_RUN真实平台结果不明 | S本地接受回执；实际通道超时后回执核对NOT_RUN | NOT_RUN |
| A35 多附件部分成功 | L2：[dispatch](../../tests/runtime/test_dispatch.py) 保留已接受部分、实际字节/次序、仅处理未决部分 | NOT_RUN多附件语义回放 | 本地PG/通道组合有证据；真实平台部分失败NOT_RUN | NOT_RUN |

## A36—A48：调度、记忆与运维

| 场景 | L1/L2代表证据与范围 | L3候选证据 | L4实际证据及缺口 | L5 |
| --- | --- | --- | --- | --- |
| A36 自然创建提醒 | L1/L2：[time/provider](../../tests/conversation/test_provider.py)、[scheduler](../../tests/scheduling/test_postgres_scheduler.py) 基于来源时间/时区、一次提交 | I：schedule_create、deadline_edges | 旧T06创建1/1、原触发FAIL保留，非真实时钟等待；当前候选见[修复报告](FOUR_FAILURE_REPAIR.md) | NOT_RUN |
| A37 周期修改与取消 | L2：[scheduler](../../tests/scheduling/test_postgres_scheduler.py)、[schedule tools](../../tests/scheduling/test_tools.py) revision/取消与历史不改写 | I：schedule_change、schedule_control | T规则修改1/1；完整服务重启后周期控制NOT_RUN | NOT_RUN |
| A38 重启与补跑 | L1/L2：[recurrence](../../tests/scheduling/test_recurrence.py)、[scheduler](../../tests/scheduling/test_postgres_scheduler.py) DST/最新一次/24小时/并发重启不重复 | NOT_RUN自然语言停机回放 | 旧T06通知required_text不连续匹配，原0/1 FAIL保留；当前候选见[修复报告](FOUR_FAILURE_REPAIR.md)，真实停机跨时刻NOT_RUN | NOT_RUN |
| A39 循环与配额 | L2：[triggers](../../tests/scheduling/test_event_triggers.py)、[tools](../../tests/scheduling/test_tools.py) 源权限/配额/派生递归拒绝 | I：schedule_create/control的有限候选 | NOT_RUN真实模型持续派生压力场景 | NOT_RUN |
| A40 自动有限跟进 | L2：[triggers](../../tests/scheduling/test_event_triggers.py)、[tools](../../tests/scheduling/test_tools.py) 期限、取消、来源撤权、真实完成事实 | I：schedule_create的有限候选 | NOT_RUN真实模型职责→后续事件→交付完整场景 | NOT_RUN |
| A41 压缩与重启记忆 | L2：[compaction](../../tests/memory/test_compaction.py)、[snapshots](../../tests/memory/test_snapshots.py) token阈值、原子游标、失败不推进 | I：memory_and_identity仅可见上下文 | NOT_RUN真实模型长对话压缩→服务重启→早期追问完整质量样本 | NOT_RUN |
| A42 更正与保留 | L2：[memory](../../tests/memory/test_memory.py)、[privacy cascade](../../tests/ops/test_privacy_cascade.py)、[task provenance](../../tests/memory/test_task_provenance.py) 失效/保留最小证据/删除传播 | I：memory_and_identity、own_task_update | 真实PG/物理文件清理有证据；真实通道删除缓存NOT_RUN | NOT_RUN |
| A43 超预算与429 | L2：[ledger](../../tests/billing/test_ledger.py)、[对账](../../tests/billing/test_reconciliation.py)、[begin revalidation](../../tests/billing/test_begin_revalidation.py)、[media](../../tests/media/test_service_contract.py) 并发/unknown/对账/429不重置 | I不承担财务写入证明 | B及Root审计111620；50元policy与全部预留保留，账单报告/保守估算/有效上界分列；429组合仍为L2 | NOT_RUN |
| A44 恶意资料 | L1/L2：[gateway](../../tests/execution/test_gateway.py)、[research](../../tests/research/test_research.py)、[team tools](../../tests/runtime/test_team_tools.py) 引用/文件不授予写权限 | I：untrusted_instruction、authority_boundaries | W真实无宿主文件/凭据/网络；跨所有格式的真实模型攻击集未全跑 | NOT_RUN |
| A45 工具与秘密隔离 | L1/L2：[gateway](../../tests/execution/test_gateway.py)、[worker](../../tests/sandbox/test_worker.py) 伪路径/链接/日志/范围拒绝 | I：untrusted_instruction的有限候选 | W真实uid/cap/noexec/断网/无挂载/无socket/无秘密与SDK上传关闭 | NOT_RUN |
| A46 账号失效 | L2：[dispatch](../../tests/runtime/test_dispatch.py)、[channel](../../tests/channels/test_agentwechat.py) auth失效退避/保留队列/告警状态 | NOT_RUN实际失效对话回放 | NOT_RUN实际登录失效、重新认证及维护者外部告警 | NOT_RUN |
| A47 迁移与单发送者 | L2：[legacy](../../tests/ops/test_legacy.py)、[backup](../../tests/ops/test_backup.py)、[controls](../../tests/runtime/test_application_controls.py) 合成迁移、空库恢复/隔离、应用单租约 | NOT_RUN旧数据回放 | 真实PG备份恢复为合成数据；真实旧库迁移/双端切换/回滚NOT_RUN | NOT_RUN |
| A48 发布可追溯 | L2：[artifact registry](../../tests/runtime/test_artifact_tools.py)、[publication](../../tests/test_export_publication.py)、[backup](../../tests/ops/test_backup.py) 来源/哈希/回执与快照 | I有fixture/结果/模型轨迹 | 本地源码/镜像/配置与运行证据分别绑定；7a268历史CI已验证，最新状态见[PR checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)；生产发布未执行 | NOT_RUN |

## 故障注入证据

F从已有Core11/Workers09的JUnit PASS中，每个不同注入位置取1个未参数化用例，共12项，达到≥10位置门槛；没有为本汇总新增测试、API或Docker运行。绑定源码25b、测试提交946ce37及09镜像，JSON保存完整JUnit/log哈希、精确testcase与测试源码哈希；原JUnit未导出逐Work trace ID，不补造该字段。

| 位置与测试入口 | 注入与主要已验证不变量 | 执行边界 | 既有结果 |
| --- | --- | --- | --- |
| [FI01 受理事务提交前](../../tests/test_persistence.py#L143) | 写入后抛异常；事件/Work/outbox/cursor及审计整体回滚 | 真实本地PG，合成事件 | Core11 PASS |
| [FI02 Scheduler建Work后](../../tests/scheduling/test_postgres_scheduler.py#L137) | 插入后异常；发生记录/Work/event/cursor回滚，重扫仅1次 | 真实本地PG，合成定时工作 | Core11 PASS |
| [FI03 tool_requested持久化点](../../tests/execution/test_engine.py#L292) | event sink失败；Engine FAILED，目标文件未创建 | 脚本模型夹具、真实临时目录 | Core11 PASS |
| [FI04 Gateway handler超时](../../tests/execution/test_gateway.py#L183) | 超时后handler退出，副作用仍标UNKNOWN | 本地async handler/网关夹具，非外网 | Core11 PASS |
| [FI05 本地接受后派送回写前](../../tests/runtime/test_dispatch.py#L62) | 接受后异常；UNKNOWN按原键逐块恢复，无重复spool | 真实PG与LocalChannel文件spool，非微信 | Core11 PASS |
| [FI06 ASR响应后原子发布前](../../tests/runtime/test_inputs.py#L265) | 无半成decision；首笔结算保留，恢复后仅1个decision；允许有账可查的第2次调用 | 真实PG；ASR HTTP/worker/价格夹具，非实际付费 | Core11 PASS |
| [FI07 UNKNOWN预留后重启](../../tests/billing/test_ledger.py) | 未知费用继续占额、不可当未启动释放；有usage证据后结算 | 真实PG账本，测试价格/合成usage | Core11 PASS |
| [FI08 通道ACK返回503](../../tests/channels/test_intake_integration.py) | 沿durable cursor恢复；epoch变更不静默重置 | 真实PG，HTTP daemon夹具，非微信网络 | Core11 PASS |
| [FI09 journal后DB提交失败](../../tests/ops/test_deletion_sink.py) | DB回滚而外部forgotten墓碑保留 | 真实PG与本地文件journal，合成隐私源 | Core11 PASS |
| [FI10 worker运行中取消](../../tests/sandbox/test_worker.py) | 有界返回cancelled，late.txt未导出 | 真实本地Docker worker，合成脚本/无网络 | Workers09 PASS |
| [FI11 native Harness IPC断开](../../tests/execution/test_harness_real.py) | 暂停、夹具模型等待被取消，checkpoint非安全恢复点 | 真实Docker/native SDK，ScriptedFakeModel | Workers09 PASS |
| [FI12 摘要提交/游标事务点](../../tests/memory/test_compaction.py) | summary提交异常；游标不前移，原文仍可检索 | 真实PG，确定性摘要夹具 | Core11 PASS |

各项只支持其列明不变量：FI06允许两次有账调用，FI04保留UNKNOWN；FI09不单独证明断电恢复，FI10不扩大为独立PID枚举，FI11不代表真实Model网络故障。该表不声称真实微信、供应商实扣、所有故障下均无重复副作用或生产故障验收通过。

## 量化门槛与未完成项

| Spec要求 | 当前可报告结果 | 最终状态 |
| --- | --- | --- |
| ≥200窗口；明确Agent/预授权开放请求召回≥95% | I：实际门控复核215/219=98.1735%；原始候选精确匹配另为246/264 | L3固定集已有证据；不代表端到端完成或未见样本 |
| 不应介入误介入≤5%；错误写归属单列 | I：误介入0/32；禁止写候选0，实际业务写入0；未以整体精确匹配率反推 | L3固定集已有证据；未执行写入不能证明实际写归属全部正确 |
| ≥30端到端任务，无人工补救成功≥85% | T06复核后26/30=86.6667%，gold未改、原产物未人工修复；追加OCR/assistant复核及混合执行/检查版本全部披露 | 旧T06达到固定集数值门槛；该历史轮仍FAIL，非单一最终构建或L5通过 |
| 四格式每种≥10代表样本，100%结构/关键内容/渲染通过 | W2历史4×10结构/所声明断言/渲染及模板遮挡证据，W的06选集97 PASS；不是任意模板保真或全部Office特性 | 容器代表样本已有受限证据；T DOCX/PDF各2/3，不把业务失败写成全通过 |
| ≥30图、≥20语音，关键问题/意图≥90% | M30/30、20/20；合成输入与TTS→ASR，不是实际群录音或人类评分 | 已有受限证据，L5 NOT_RUN |
| ≥50身份/记忆；负责人串人阻断 | ID：独立50组真实PG合成场景，加固定清单哈希校验共51 PASS | L1/L2已有固定集证据；L5 NOT_RUN |
| 接单P95≤5s、短答≤20s、取消落库/禁新工具≤2s | P同25b三项各30/30、0失败，P95上界6.200338417/2.044179791/1.175423541s全部PASS；旧FAIL保留 | 当前本地条件达标，长任务held lane且取消固定provider；不等于完整负载/微信SLO |
| ≥10不同位置故障注入，无可重现重复高影响副作用 | F：12个不同注入位置/12个既有JUnit PASS，按各项不变量计数；10项Core11、2项真实本地容器Workers09，不重复计参数变体 | 本地位置数门槛已满足；不扩大为通用无重复副作用或外部/生产验收通过，边界见短表 |
| L5与G4七天 | 实际群、手机、原生登录/重连/回声、目标Linux amd64和七天shadow均未执行 | NOT_RUN |

当前09镜像同25b的证明与历史06分列，完整commit/tree、镜像、锁、schema和轨迹见[实现状态](IMPLEMENTATION_STATUS.md)。25b历史有效预算用量上界21.023746 CNY，gross settled55.079718为估算，unknown2笔0.168044保留；daily/total仍50、revision1，该阶段当时user-reported实扣4.83；最新累计报告11.65及Owner对账见修复报告。该上界不作账单、退款或新额度，见[费用说明](LOCAL_VALIDATION_PROGRESS.md#费用与仍待完成的门槛)。

当前扫描65项Python依赖0 known vulns、327可发表文件0leaks、wheel147/sdist328条私有0，不扩大到系统依赖或目标服务器。历史代码head7a268…的[CI43632514186](https://github.com/huchenzi0609-creator/GroupAgent/actions/runs/43632514186)四job全success；最新提交CI以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，不预判后续head。[PR #8](https://github.com/huchenzi0609-creator/GroupAgent/pull/8)仍draft/base未合并[PR #1](https://github.com/huchenzi0609-creator/GroupAgent/pull/1)。

原固定矩阵四项FAIL完整保留。所有L5、目标Linux容量、至少七天shadow、真实迁移与单发送者切换均NOT_RUN，G0—G5不能据本表宣称全通过。
