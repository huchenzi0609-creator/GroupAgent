# 证据与来源登记

核对日期：2026年11月13日，Asia/Shanghai。优先采用官方产品文档、项目维护者文档及固定版本代码。下列公开产品行为不等于可复制的内部实现；开源项目文档声明不等于已在本项目实测。

## 1 参考产品

| 来源 | 支持的事实 | 使用边界 |
| --- | --- | --- |
| [OpenAI Meet dots](https://learn.chatgpt.com/docs/dots) | 持续在线、云端计算机、交谈间继续工作、记忆与权限边界 | 产品设计参考，不推断可嵌入微信群的Dots内部API |
| [Dots Tasks and memory](https://learn.chatgpt.com/docs/dots/tasks-and-memory) | 任务与对话、记忆和持续职责的公开产品说明 | 不能推出我们的自建系统已具备同等效果 |
| [Grok Message and collaborate](https://docs.x.ai/grok-bot/chat-and-collaboration) | 自然消息、附件、过程交互、群中Bot响应与接续工作 | 文档中多Bot群与多人微信群不是同一个权限场景 |
| [Grok Initiative](https://docs.x.ai/grok-bot/initiative) | 主动提示、提出帮助、风险动作的边界 | 不把主动性解释为无授权执行全部操作 |
| [How Grok Bot works](https://docs.x.ai/grok-bot/how-grok-bot-works) | 记忆、工具/插件、计算机、文件及交付的协作过程 | 只参考公开行为，不声称了解私有架构 |

## 2 DeepSeek Harness固定研究基线

仓库：[deepseek-ai/deepseek-harness](https://github.com/deepseek-ai/deepseek-harness)。本轮读取的master提交为 **39163dd30f703a79079e2d2446545cff3c453738**，提交时间2026-11-13 07:37:50 UTC，根包版本0.2.2-rc.3。不能把开发分支版本描述成已经选定的稳定生产版本。

| 固定来源 | 核对内容 |
| --- | --- |
| [README](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/README.md) | 插件架构、开发预览与兼容性变更声明 |
| [package.json](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/package.json) | 根包0.2.2-rc.3、源码Node要求≥24.9.0、构建与检查入口 |
| [Python SDK指南](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/docs/user/guide/python-sdk.md) | Python≥3.10、匹配运行时wheel、标准/最小配置差异、镜像输入、权限和日志贡献默认行为 |
| [SDK server README](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/packages/sdk/server/README.md) | stdio JSON-RPC、会话/事件、恢复、审批回调和SDK附件边界 |
| [Schedule子系统](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/docs/subsystems/schedule.md) | 持久规则、时区、目标Session、投递记录；入队与完成不同；跨存储崩溃可重复投递 |
| [Safety](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/SAFETY.md) | 实验状态、未经过安全审计、不可单独依赖其沙箱作为唯一隔离措施 |
| [MIT License](https://github.com/deepseek-ai/deepseek-harness/blob/39163dd30f703a79079e2d2446545cff3c453738/LICENSE) | 主仓库许可与保留版权/许可说明要求；不替代第三方组件许可审查 |

本轮没有安装、执行或压测Harness，也没有审计全部源代码。选型结论建立在上述接口、配置与机制文档的审阅上；具体集成和隔离效果必须通过S0实验。

## 3 Superagent参考原型

仓库：[deepseek-ai/dsh-superagent-prototype](https://github.com/deepseek-ai/dsh-superagent-prototype)，研究提交 **8c32a9dc5d72c033816c6a8110b37f40cf94fba4**；根包版本0.1.0。

- [README](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/8c32a9dc5d72c033816c6a8110b37f40cf94fba4/README.md)：明确属于参考研究原型，并非官方产品；主要面向单一GitHub仓库，可选飞书。
- [Domain](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/8c32a9dc5d72c033816c6a8110b37f40cf94fba4/docs/001-domain.md)：Source、Trigger、Thread、Eval、Mandate、Ticket、Assignment与Subscription。
- [Execution](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/8c32a9dc5d72c033816c6a8110b37f40cf94fba4/docs/003-execution.md)：背景工作、并发、失败和重启；工作目录及包装器不是安全边界。
- [Architecture](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/8c32a9dc5d72c033816c6a8110b37f40cf94fba4/docs/007-architecture.md)：领域规则、来源适配与执行器的分离。
- [package.json](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/8c32a9dc5d72c033816c6a8110b37f40cf94fba4/package.json)：旧版Harness依赖基线；不能假设与当前master直接兼容。

报告借鉴领域划分，没有复制其业务实现、未把GitHub工作流当作微信群成品。

## 4 模型与通道

| 来源 | 核对内容与边界 |
| --- | --- |
| [DeepSeek Vision](https://api-docs.deepseek.com/guides/vision/) | deepseek-flash当前接受图片；不能因此推出语音、视频或图像生成能力 |
| [DeepSeek Files API](https://api-docs.deepseek.com/guides/files_api/) | 当前文档围绕图像上传和引用；不能当作Word/PPT/PDF通用编辑API |
| [DeepSeek Search & Fetch](https://api-docs.deepseek.com/guides/search/) | 独立搜索与页面读取接口；真实地域可达性、质量和费用仍需验证 |
| [agent-wechat消息类型](https://thisnick.github.io/agent-wechat/how-it-works/message-types/) | 图片/文件等类型，转发附件、引用媒体和撤回的已声明边界 |
| [agent-wechat附件收发](https://thisnick.github.io/agent-wechat/use/media/) | 能力上限查询、媒体job与回执、超时后核对；不是部署v2.2.1全部能力的验收记录 |

接入项目是第三方客户端，当前文档可能领先于既有部署版本。任何升级必须先固定版本并进行真实测试群验证；本报告不承诺微信平台的长期兼容性或账号行为。

## 5 文档工具与备选框架

- [python-docx](https://python-docx.readthedocs.io/en/latest/)：创建和更新.docx。
- [python-pptx](https://python-pptx.readthedocs.io/en/latest/)：创建、读取及更新.pptx，并说明仍有不支持的特性。
- [LibreOffice参数说明](https://help.libreoffice.org/latest/en-US/text/shared/guide/start_parameters.html)：命令行与headless处理基础；不构成文件保真保证。
- [LangGraph Persistence](https://docs.langchain.com/oss/python/langgraph/persistence)：线程checkpoint与跨线程store的区分，用于备选框架评估。

以上均为工具候选，未在本轮安装或运行新文档流水线。版本锁定、字体、转换结果和资源消耗需单独实验。

## 6 旧原型本地审计

位置：相邻的Project_WechatLeader目录。仅检查源代码和非秘密配置文件，没有读取config.local.json、真实数据库内容、登录凭证或真实附件。

2026-11-13执行：python3 -B -m unittest discover -s tests -q。结果：36项测试通过；这是旧能力的离线基线，不是新版能力测试。

| 文件与位置 | 观察 |
| --- | --- |
| leader.py MAX_REPLY/brief/extract_command | 200字符硬限与显式唤醒 |
| natural.py interpret | 有限动作分类，没有通用工具执行循环 |
| bridge.py normalize_text/messages/tick | 纯文本入口、固定起点分页、同步执行及全局投递不确定保护 |
| memory.py Memory.context | 最近16条＋摘要，无按主题/语义检索 |
| leader.py pending_reminders/daily_checkin | 固定到期和每日19点规则 |
| compose.yaml | client与bot共享data与网络命名空间，适合旧版但不能直接给通用执行worker沿用 |

哈希清单见 [legacy-source-manifest.json](legacy-source-manifest.json)。它用于标识此次审阅基线，不包含旧代码副本或私密运行数据。

本轮未重新连接生产服务器；2核2GB、系统及容器部署等环境事实沿用此前对话中的部署验证，报告已注明这一时效边界。
