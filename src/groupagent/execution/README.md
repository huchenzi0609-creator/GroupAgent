# 执行、Harness 与恢复

`ScopedToolEngine` 与 `HarnessEngine` 实现同一 EnginePort。前者直接驱动可注入 ModelPort；后者运行固定版本的真实 DeepSeek Harness SDK。`ScriptedFakeModel` 仅供明确标记的离线测试。两者的结束文本均为 `CANDIDATE`，不是业务完成；父 `WorkSupervisor` 必须验证当前目标和来源，并等待该 goal revision 的全部交付 accepted 后才能完成 WorkOrder。

共同边界包括允许工具集合、单轮及总轮数、输入／输出 token、工具调用数、活动时间、每次模型调用的金额 reserve/settle、取消、输入追加、结构化事件与 checkpoint。`ToolGateway.registered_names` 是只读集合；schema 标准化不授予工具权限。权限、群、work、工作区、输入版本与当前 lease 仍在 invoke 与每个实际副作用前复核。

`bind_persistence(event_sink,checkpoint_sink)` 必须在 start 前绑定。`flush_checkpoint(run_id)` 产生新的 progress sequence，并与其他保存序列化。checkpoint 保存累计 round/token/tool/time、候选文本、失败码、可选 backend state 和 `safe_to_resume`；未知工具副作用不能自动重放。模型 HTTP 429、`provider_unavailable` 与模型响应解析／usage／arguments 无效的 `invalid_model_response` 共用有界额外重试次数（默认 2，0 表示禁用），遵循有上限的退避及 Retry-After；每次单独 reserve/settle、计入轮数和保守 token，不因重试重新获得预算。响应已解析后的 `invalid_model_reply`、工具及策略错误不进入该重试。

供应商故障仅在原活动 Core 循环／Harness relay 中重新生成尚未交给网关的纯模型响应：前次模型 task 必须已停止，无未知工具效果，不重放历史工具。每次未知费用继续保留最坏预留，`uncertain_model_usage` 保存历史账务不确定性；新的成功响应建立确定执行边界后 `model_recovery_pending=false`，不会退还旧未知费用。重试耗尽、取消、未停止的模型 task、模型超时及其他错误仍暂停／停止；旧 unsafe checkpoint 不会自动恢复，须另行核对。轮数、token、活动时间、工具及金额预算仍在每次调用前复核。

## 真实 Harness 适配

SDK/runtime 使用发布版 `0.2.2rc2`，包哈希、源码 commit 对应和许可证记录见 `deploy/worker/harness-manifest.json`。原研究 commit `39163dd30f703a79079e2d2446545cff3c453738` 的根版本与 Python 生成版本并不等于一个可安装的同名发布包；不能据此声称测试了不存在的 wheel。实际 SDK `api.py/client.py` 与该 commit 字节匹配，发布 tag 指向 `ddac96760b6edf8fa0fe17cbdcf0b512f4c12b2c`。

`harness_worker.py` 使用公开的 `DeepSeekHarness`、start_session、session.run、session_prompt、resume_session 和 close。真实 SDK 模型线路是 DeepSeek Messages API；`harness_wire.py` 转换有限的 text/tool 消息和 SSE，拒绝无法验证的 schema/协议扩展。参考的主文档为 [Anthropic API compatibility](https://api-docs.deepseek.com/guides/anthropic_api/) 与 [Create message](https://api-docs.deepseek.com/zh-cn/api/create-message/)，2026-11-14 核对。

SDK 在无外部网络的容器内，仅访问本容器 loopback relay。provider key 留在宿主 ModelPort/预算端，容器只有无外部权限的占位字符串。所有 MCP 工具请求经宿主统一网关，还必须匹配宿主已计费模型响应中的提议。禁用原生 bash/pwsh/文件系统工具、MCP resource、内置模型重试、压缩、图片 offload、自动标题和 DeepSeek session log 上传。未注册工具和额外 `dsh_*` contribution 会被拒绝。

实际验证曾发现 native addon 在 noexec tmpfs 加载失败、64 MiB 缓存不足、SDK 使用 Messages API 而非 Chat Completions；实现依据失败证据修正。仅 Harness 的私有 `/tmp` 允许 executable mapping 供固定 native runtime 加载，`/work` 仍 noexec；compute/media tmpfs 不改变。SDK session 文件有边界、gzip/base64 保存于业务 checkpoint，可恢复安全原生会话；这不等于恢复任意运行中的进程。追加输入在 SDK 原生轮次边界处理，同时宿主下一模型调用注入已接受的更新，防止旧目标工具提议越过边界。

本地实际 SDK 7 项与 sandbox suite 11 项（7 项启动真实 Docker，4 项配置／预拒绝单元测试）于 2026-11-14 同一轮通过；模型回复来自显式 ScriptedFakeModel，不能称为真实供应商语义验收或真实微信群验收。固定 SDK 镜像 `sha256:23e34bc9067286559776fa44a5f8b94a176ec3694754bfae2612a44b092f88b1`。原生二进制未独立可重复构建，PyPI provenance 未取得；发布者 wheel 哈希不替代这两项证明。

## 父运行层接入

`runtime/executor.py` 的 `WorkSupervisor` 接收 store/state、可信 control actor 与 actor resolver、ExecutionFactory、CandidateValidator 和工作区。`tick` 拉取后台工作并检查投递，`run_once` 方便受控测试，`close` 取消活动执行。每群一个跨进程 heavy-worker lease；当前产品只配置一个授权群，不声称全局多租户容量隔离。工作 lease 心跳涵盖准备、模型和验证，等待投递时释放 heavy slot，但继续维护有效工作 lease。

安全 checkpoint 可恢复；不安全 checkpoint 暂停核对。新 goal revision 继承累计预算，旧 candidate 不能直接完成新目标。崩溃后安全 candidate 可复验后重建交付 bundle，不重复调用模型。验证失败可在同一累计预算下修复两次；用尽后持久失败，显式 retry 也不清空累计消耗。unknown/dispatching 投递先核对，再恢复 finalization；accepted 的部分不能重复发送。

`tests/execution/test_supervisor.py` 首轮 14 项真实 PostgreSQL 回归通过，覆盖租约、取消、目标版本、累计预算、验证修复／超时、来源撤销、候选恢复和交付核对；后续新增范围见下文。运行期过程和模型 checkpoint 的事件／保存由同一 state 接口持久化；这不替代真实 channel 送达验证。

## 等待附件与请求资料

准备阶段的 `_runtime_preparation` 保存在同一 Work.scope，包含尝试次数、下次重试和等待期限、阶段及累计活动时间下界。factory 调用前先持久预留时间；完成后按实际时间结算，崩溃无法确认时保留最坏预留并暂停核对。默认单次准备上限 60 秒，首次 `attachment_pending` 释放 heavy slot 并转 WAITING_EXTERNAL，5 秒后最多再尝试一次；等待总期限 300 秒。再次 pending 或期限到达转 WAITING_INPUT，要求新的可用资料。expired/unavailable 直接等待输入，auth_required 暂停；不存在无限自动重试或重启清空计数。等待期间不计活动时间，准备耗时、既有 model/token/tool/time 消耗均继承到安全 checkpoint。取消等待会阻止后续请求；仅重新排队相同已耗尽的输入不能重置重试窗口。

`register_request_input_tool(gateway)` 提供真实模型可调用的明确工具。问题最多 2000 字，缺少的字段最多 8 个。只有网关可信注册的 control_kind 能产生顶层 ToolReceipt.control，网页／普通工具 output 内的同名 JSON 是数据；不能制造控制回执。问题优先于同批其他工具效果。Core 停止该批剩余执行；Harness 拒绝后续模型 relay 请求，以真实 SDK 原生会话结束结果保存 session，不伪造一个模型最终回答，也不额外调用供应商。

EngineCheckpoint.input_request 和 needs_input 事件持久问题；Supervisor 在 WAITING_INPUT 原子保存 `_runtime_input_request={question,required_inputs,goal_revision,attempt_id}`。父通知层仅在工作处于 WAITING_INPUT 且问题 goal_revision 匹配时展示，发送仍走 outbox。经已有 revise_work + input_received 路径恢复同一 Work，预算不重置；问题不是完成候选。新增真实 PG、合成 channel/decoder 和安全控制测试已通过；Supervisor 当前 25 项真实 PG 测试。Harness 新增的真实 request_input／原生恢复用例也通过，含同批过早文件写入被阻止，模型仍明确使用 ScriptedFakeModel。

参数纠错仍受原有 round/token/tool/time/金额预算限制。网关 schema 失败是 NOT_STARTED，只提供已知字段／有界数组索引、静态错误类型和 reviewed `ToolData.validation_hints`；未知字段名、输入值、ValidationError ctx 和原始 ValueError 文本不进入回执。计算工具在启动容器前检查输入路径、输出父目录、新文件名和参数格式，已证明无执行的错误为 NO_EFFECT 并给出 cwd=/work、相对路径修复提示。执行后的未知状态或部分导出不会因为同样叫 invalid_path 就转换为 NO_EFFECT。

`unresolved_tools` 只保留 UNKNOWN 效果。NOT_STARTED／NO_EFFECT 的失败回执不被改成成功，但允许其他成功路径形成候选；required_tools 仍必须有成功回执，文件仍须独立验证和 accepted 交付。安全旧 checkpoint 中仅因历史无效果失败留下的名称会按真实 UNKNOWN receipts 重算，消耗不重置。SDK 本地拒绝、未到达 host gateway 的提议记为 NOT_STARTED 并计入工具次数，不能被伪称调用成功。

真人任务工具由 `runtime.team_tools.register_team_tools` 注册。写权限、可修改的 task IDs、完成认可和当前授权消息均由控制层 scope 给出，不能来自文件／OCR 或模型参数。`may_request_input` 是显式注册的可信前置条件能力；其控制回执 success=False、NO_EFFECT，不计作写入成功，也不会缓存成永久过期的问题。Core/Harness 会等待输入，父 validator 仍检查真实任务效果。成功写入的内部 QUOTE 空 text、无 platform_message_id，授权来源与已消费的数据来源分别记录；没有伪造人类原话。

## 清理前定向停止

`await supervisor.quiesce(work_ids)` 仅对已在 PostgreSQL 暂停／取消或终止的目标工作执行，成功返回 None；`DomainError('work_busy', retryable=True)` 表示仍不能删除。它不调用全服务 close，也不取消其他工作。调用者必须把持久停止／隐私撤销保持到删除完成，不能在清理过程中重新授权同一工作。

方法取消并有界等候目标准备、模型、工具和验证任务。模型／工具不响应取消时保留忙碌状态；不仅检查一个 cancel 标志。Docker 使用 hashed group/work labels 查找同一目标的容器，停止后再次向配置的本机 daemon 查询，确认没有残留后才写 work_quiesced。正常结束写 execution_stopped 握手；外部 controller 的未确认 attempt 或 dispatching/unknown 投递仍要求核对，租约过期不是进程已经停止的证明。这不承诺终止远端供应商已接收的任务、退回未知费用或撤回已发送消息。

新增 6 项 PG 回归验证定向停止、准备／验证 join、不合作模型 busy、其他工作继续、未知发送保留和外部 controller 握手门槛；Gateway 另有不合作 handler 负例。1 项 PG+真实 Docker 用例先读取正在运行的 guest 标记，再停止目标，验证容器消失及 late 输出未导出。Supervisor 现有 31 项非 Docker PG 用例及 1 项真实 Docker 用例。输入预处理 pseudo-work 的容器可用 `DockerSandbox.quiesce_work(group_id,pseudo_work_id)` 检查，但它不替代 InputProcessor 宿主任务的停止握手。
