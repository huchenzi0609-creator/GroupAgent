# Execution foundation and Harness adoption gate

状态：本地实现与离线契约验证，2026-11-13。此文不声明 Harness、真实模型、OS 隔离或生产验收通过。视频处理不在范围内。

## 已实现的边界

`groupagent.execution.ScopedToolEngine` 实现项目自己的 `EnginePort`，不是 DeepSeek Harness 包装器。它能调用可替换的 `ModelPort`，执行工具循环、接收追加输入、取消正在等待的模型／工具、输出带顺序号的类型化事件，并导出可序列化 checkpoint。

`start(ExecutionRequest)` 返回 attempt ID；`wait()` 和 `inspect()` 返回 `RunSnapshot`；`events(after_sequence=...)` 可重读本进程事件。`append_input(..., expected_revision, input_id=...)` 校验操作者与目标修订，过时模型结果不会再发起工具。`cancel()` 只允许请求者或本群 owner。默认同时运行一个工作；控制层负责持久排队与全局租约。

默认上限为 24 次模型调用、累计 200000 token、60 次工具调用、1200 秒活动时间。每次模型调用先按 UTF-8 字节数加协议余量保守估算输入，预留输出上限；这是本地估算，不宣称是供应商精确 tokenizer。有效响应按使用量结算；超出预留停止执行。超时／取消后计费结果不明的请求保留预留量，checkpoint 禁止自动重放。重试由控制层决定，适配器不隐式重试。价格和群每日费用仍由控制层配置与结算，本模块未接入真实计费。

工具批次超过剩余次数时不启动该批次。响应被截断、达到轮数／时间限制、输出未知或必要工具失败都不会产生成功候选。允许预算内修正无副作用的工具错误；未知副作用先暂停，等待核对。

结果只可能是 `candidate`、`waiting_input`、`paused`、`failed` 或 `cancelled`，从不把模型文字转换成业务 `completed`。控制层仍须校验目标 revision、必需产物、内容和渲染证据，以及平台接受回执。

## 工具网关与接入接口

`ToolGateway.register(ToolDefinition(...))` 接受受审阅的异步处理器 `async handler(args, context)`。输入、输出为 Pydantic 模型；输入须禁止额外字段。定义同时声明版本、角色、群／工作／资源范围、超时、输入输出及产物大小、费用计量和幂等策略。`ToolData` 是可直接继承的基础模型。`ToolContext` 的 actor、group、work、workspace 和 idempotency_key 来自控制层，模型不能填入或替换。

网关只执行已注册且在本工作允许列表中的工具。生产应注入 `policy_provider`，每次调用查询当前角色、允许列表与工作是否仍有效；默认 `static_policy` 仅适用于离线基础实验。网关拒绝跨群／跨工作上下文、重叠工作目录，以及含 `..`、绝对路径、符号链接的路径参数。内置 UTF-8 文件工具使用 POSIX 目录描述符与 `O_NOFOLLOW`，读操作拒绝非普通文件和硬链接；写操作只原子发布新草稿，不能覆盖已有文件。工具声明的输出路径也重新校验。

这些是对受审阅工具的能力约束，**不是 OS 沙箱**。Python 处理器本身仍运行在宿主权限下，不能安装不可信处理器。没有默认 Shell、Python 执行、浏览器、外网下载或 Docker 工具。需要运行任意代码的能力必须在另行验证的隔离 worker 中实现。

可注入 `event_sink` 与 `checkpoint_sink`，事件／检查点写入失败会阻止后续动作。未注入时只保存在内存，`checkpoint_saved.payload.durable` 明确为 `false`。持久恢复由控制层提供校验过的 checkpoint；`resume(..., attempt_id=...)` 创建新 attempt，继承已消耗预算和工具幂等账本。运行中模型／工具或未知副作用的 checkpoint 拒绝自动恢复。恢复不会声称进程、浏览器或未登记外部效果仍然存在。

## DeepSeek HTTP 适配器

`groupagent.execution.deepseek.DeepSeekHTTPModel` 单独实现非 thinking Chat Completions 工具调用。按当前官方接口发送 `messages`、`tools`、`max_tokens`、`thinking.type=disabled`，读取函数调用及 prompt/completion token；缺少或矛盾的 usage、非法工具 JSON 和超大响应均失败关闭。默认路由为 `deepseek-flash`，模型名由调用方显式配置。参考：[Tool Calls](https://api-docs.deepseek.com/guides/tool_calls/)、[Chat Completions](https://api-docs.deepseek.com/api/create-chat-completion/)（访问：2026-11-13）。

凭证由调用方显式传入，构造过程不读取环境密钥。默认仅允许 `https://api.deepseek.com`，拒绝重定向，不隐式使用环境代理。没有真实密钥、付费调用或供应商连通性测试；`httpx.MockTransport` 仅验证请求／响应协议、限额与错误处理。它不是 Harness 接入、图像／语音能力或生产模型评测。

## 固定 Harness 上游的核验

研究 commit：`39163dd30f703a79079e2d2446545cff3c453738`；所有下列源均在 2026-11-13 阅读。根包版本是 `0.2.2-rc.3`，源码工作区要求 Node >=24.9.0；这不等同于已经发布且选定的稳定运行时。[package.json](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/package.json)

| 已核验事实 | 对接入的影响 | 固定来源 |
| --- | --- | --- |
| Python 包 `deepseek-harness-sdk` 依赖同版本 `deepseek-harness-runtime-bin`；公开入口包括 `DeepSeekHarness(...).run()` 和 `resume_session()` | 必须证明安装包与研究 commit 的对应关系，不能猜测包版本；恢复不还原外部进程 | [Python SDK](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/python/sdk/README.md) |
| `sdk-minimal` 默认包含持久 Shell，采用全访问权限；工作目录不限制可访问路径；日志贡献默认开启，文档提供关闭配置项 | 在隔离环境与白名单插件配置完成前不能加载真实群内容；关闭额外日志还须抓取出站协议验证 | [Python 指南](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/docs/user/guide/python-sdk.md) |
| SDK 线协议无 cancel 或 session-close 方法；停止活动靠关闭整个 runtime 进程 | 不可虚构 `session.cancel()`；需验证每工作独立 runtime、终止宽限和未知副作用恢复 | [SDK 协议](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/packages/sdk/protocol/README.md) |
| 上游明确属于实验预览，尚未安全审计，也不能作为唯一安全控制 | 宿主最小权限、凭证隔离和网络／文件隔离必须单独验收 | [SAFETY.md](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/SAFETY.md) |

本实现没有伪造 Harness adapter，也没有执行 Harness 默认全权限 profile。

## 可重复环境探针与当前阻塞

从项目目录运行（只用标准库也可以直接执行）：

```sh
.venv/bin/python src/groupagent/execution/harness_probe.py
```

2026-11-13 15:28:02 UTC 的实际结果：Python 3.13.15、Darwin arm64；项目虚拟环境找不到 SDK 与 runtime 两个 distribution，也找不到 `deepseek_harness` 模块；PATH 中存在 `dsh` 和 `node`，不存在 Docker CLI。发现 `dsh` 不证明该二进制来自固定 commit，探针没有启动它。退出码为 **2**，表示接入门槛尚未通过，`credentials_read=false`、`runtime_started=false`、`model_requests=0`。

补充只读核验：`/opt/homebrew/bin/dsh` 链接到 `../lib/node_modules/@deepseek-ai/dsh/lib/bin.js`；其 `package.json` 声明 `@deepseek-ai/dsh` **0.1.5-rc.1**。使用清空继承环境后的 `dsh --version` 也返回 **0.1.5-rc.1**。这是已安装的旧 CLI，不是“机器上完全没有 Harness”；它不能替代研究基线 `0.2.2-rc.3` 的 SDK/runtime 验证。版本检查没有运行 Agent、工具或 profile 工作；探针现会只读报告发现的 CLI 路径和 npm 版本。

必须继续通过以下门槛才能实现并启用 Harness 适配器：

1. 固定 SDK/runtime 构建产物、哈希、许可和 commit 对应关系，在独立环境完成启动。
2. 验证只加载允许的工具与插件；默认全权限工具不可绕过统一网关。
3. 使用合成会话和模拟模型接收端，证明日志贡献关闭、出站字段与预算记账正确。
4. 验证运行中追加、每工作独立终止、超时升级终止、重启恢复，以及不明副作用不重试。
5. 在目标 worker 环境实测不能读取通道凭证、其他群文件、SSH 目录或 Docker socket，并验证网络和资源上限。

Docker 当前缺失是本机隔离实验的阻塞之一；本轮没有用不受隔离的宿主 Shell 代替该验收。上述结果属于可复核的未通过结论，不代表 Harness 技术上无法满足要求。

## 本地验证

```sh
.venv/bin/python -m pytest tests/execution -q
.venv/bin/python -m ruff check src/groupagent/execution src/groupagent/tools tests/execution
.venv/bin/python -m mypy src/groupagent/execution src/groupagent/tools
```

测试使用合成身份、临时工作目录、显式 `ScriptedFakeModel` 与 HTTP 模拟传输。涵盖工具允许列表、跨群／路径拒绝、策略撤销、幂等、并发控制、预算预检查、取消、过时结果、检查点恢复、事件顺序、假完成阻断与 HTTP 错误卫生。真实 Harness、真实模型质量、OS worker 隔离与真实群交付仍待验收。

2026-11-13 实际检查结果：**54 tests passed**；Ruff lint 与格式检查通过（17 个文件）；mypy 通过（12 个源文件）。这些是离线测试结果。pytest-asyncio 提示项目尚未显式设置异步 fixture 的默认 loop scope；本目录没有异步 fixture，测试使用当前项目配置。
