# 实现与验证状态

更新：2026-11-14。分支：`feat/control-plane-foundation`。本页记录开发证据；不是 v1 完成或上线声明。现有群机器人、服务器和群数据均未修改。视频处理保持排除。

## 当前交付

| 模块 | 本批实现与证据 | 明确未完成的部分 |
| --- | --- | --- |
| 配置与工程 | 严格类型配置、uv 锁、Python 包/CLI、JSON Schema 导出、PR CI 定义 | 配置齐全不能证明真实运行能力；分支保护和 CI 结果须另查实际平台 |
| 数据与身份边界 | PostgreSQL 16 迁移001—003；不可变事件与来源去重、游标CAS、可信actor与群策略版本、角色权限 | 完整成员档案同步落库、历史数据迁移、RLS和不可信数据库角色尚未实现 |
| 对话协调 | 模型只提出结构化候选；独立持久接收与决定；无前缀入口、工作新建/改写/取消、回声按接受回执去重、只读问答需显式策略 | 自然参与质量、主题关联、静默/冷却/分段、丰富上下文与模型评测尚未验收 |
| 工作执行 | 工作单与成员任务分开；目标revision、租约fence、取消、检查点、工具允许列表、每步策略核对、轮数/token/工具/时间限制 | 自主子任务分解的生产策略、持久执行守护进程、群费用账本和真实Harness适配尚未完成 |
| 文档产物 | .md/.docx/.pdf/.pptx真实生成、读取、定点编辑或重建；不可变版本、哈希、冲突检查、结构/内容/渲染/布局验证 | 扫描PDF OCR、任意模板高保真、复杂图表/批注/动画、异常文档OS隔离尚未验收；PDF编辑目前为重建 |
| 研究工具 | DeepSeek搜索/Fetch适配、来源与截断标识、URL/DNS范围检查、超时和固定供应商端点 | 未调用真实模型/搜索服务；交互式浏览器尚未实现 |
| 通道 | 固定AgentWechat2.2.1契约：范围化事件、群内名、附件句柄、文本/文件发送、幂等回执；真实PG验证入库后ACK与重启恢复 | MockTransport不是实际微信；真实图片/语音/文件传输与手机打开、昵称完整性、共享账号回声均需独立验收 |
| 调度 | at/every/daily/weekly、时区/DST、30天默认有效期、配额、取消、有限补跑、合并、occurrence和WorkOrder原子提交 | 自然语言时间解析、后台循环、cron/RRULE及有限事件触发尚未实现 |
| 长期记忆与多模态 | 原始事件、成员任务和工作单已持久化，视频在模型前排除 | 自动压缩/主题检索/事实纠错、图片理解、ASR/TTS、图像生成尚未实现；不能把已有数据库等同完整记忆功能 |
| 运维 | 迁移校验、操作审计、可复现本地实验、版本化证据、未知投递暂停核对 | 全量费用、监控、备份恢复演练、保留/删除传播、生产单发送者切换、七天试运行尚未完成 |

数据库迁移一经测试应用即冻结校验和，后续变更新增迁移。所有数据库账号仅用于控制层；当前 Python 工具范围检查不等于 OS 沙箱，不能向不可信工具开放宿主执行权限。

## 已执行的验证

2026-11-14 本机 macOS arm64，Python 3.13.15、PostgreSQL 16.15：

- 整库 **366 passed**，无skip；其中包括真实 PostgreSQL、9项实际渲染测试、中文三格式、两次独立演示以及完整渲染后交付结案。
- Ruff lint、格式检查通过；mypy 对40个源文件通过。
- wheel 与 sdist 构建通过；wheel包含迁移001/002/003，sdist不含 `.cache`、`.venv`、`runtime`、`.git` 或 `vendor`。
- 配置示例可解析；生产检查仍阻断缺少群配置、金额/价格快照、保留策略、模型与运行时的配置。
- 修复独立复查发现的：重定向泄漏风险、非成功模型结束原因、空白配置、更新验收条件丢失、伪造actor、数据库脚本符号/硬链接、遗漏的CI渲染链路、只读角色无法接收合法答复。

范围：数据库与文档工具是真实运行；模型、搜索HTTP和微信HTTP使用合成fixture。没有访问付费API、微信群、旧凭据或生产数据库。HTTP契约测试不证明供应商连通性、模型质量或实际收发成功。布局自动检查不等于所有模板的人类视觉审查。

私有仓库已创建；GitHub CI定义包含 Ubuntu 24.04 的 Python3.11/3.13 core job 和独立PostgreSQL的render job。托管运行的最终状态在PR及后续验证记录中更新，不能由本地测试推断。

2026-11-14通过已登录的仓库设置页面核验：ruleset及经典branch protection两种入口均提示，当前私有仓库的规则不会强制执行，需迁入符合套餐条件的组织。未调整可见性、购买套餐或创建假装生效的保护；当前采用PR审阅与人工合并纪律，**不能声称main已受技术强制保护**。这是G1仍未满足的条件之一。

## 可重复执行

按照根README启动独立开发数据库。测试只使用 `GROUPAGENT_TEST_DSN` 指定的数据库，不能指向生产库。服务监听当前用户私有Unix socket，不开启TCP；脚本拒绝未登记的已有cluster及路径重定向。

安装 LibreOffice、Poppler、CJK 字体后：

```sh
export GROUPAGENT_TEST_DSN="$(uv run --no-sync python scripts/dev_postgres.py dsn)"
export GROUPAGENT_RENDER_TESTS=1
# 按当前机器配置可信字体；下面为Ubuntu 24.04安装fonts-wqy-zenhei后的路径。
export GROUPAGENT_FONT_DIRS=/usr/share/fonts
export GROUPAGENT_PDF_FONT=/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc
export GROUPAGENT_OFFICE_FONT='WenQuanYi Zen Hei'
uv run --no-sync pytest -q
```

`soffice`、`pdftotext`、`pdftoppm` 须可发现。若 `pdftotext` 不在PATH，可设置 `GROUPAGENT_PDFTOTEXT` 为绝对路径。本机测试使用系统的 Arial Unicode 字体，不复制进仓库；Ubuntu的字体嵌入行为须由托管render job验证。

生成实际中文DOCX并保留渲染证据：

```sh
uv run --no-sync groupagent demo --dsn-file runtime/postgres/dsn.txt \
  --format docx --language zh --font-dir /usr/share/fonts \
  --office-font-family 'WenQuanYi Zen Hei'
```

PDF另外传 `--pdf-font`，所有输出仍是开发演示。当前没有 `serve` / 生产发送守护命令，避免把未验收组件自动接上真实群。

## 下一步与发布门槛

原[实施计划](../spec/IMPLEMENTATION_PLAN_V0.1.md)和[验收清单](../spec/ACCEPTANCE_V0.1.md)继续有效，本批没有缩减其范围。

1. **G0尚未通过**：固定Harness SDK/runtime构建；在隔离worker中证明工具、凭证、出站、取消/恢复边界。当前本机有旧dsh0.1.5-rc.1，项目SDK/runtime与Docker不存在；详见Harness实验记录。不能在宿主用全权限profile代替。
2. 完成独立测试通道POC：冻结真实AgentWechat运行版本、可信身份、媒体输入与回声语义，再做手机端文件打开验证。现有生产账号不用于这批自动回归。
3. **G1尚未通过**：完善费用预留/结算、秘密隔离、持续扫描、监控与平台审查控制；当前token预算不能代替日金额账本。
4. 在供应商和预算配置明确后完成自然参与/图片研究/DOCX+PDF交付的G2真实闭环；补全长期记忆、语音、图像生成、自然语言调度及相应验收。
5. 完成G3全功能验收、G4至少七天有界试运行和G5备份回滚/生产配置确认后，才安排旧系统迁移与单发送者切换。

这是一项大工程的可审阅实现增量。待验证项按实际阻塞管理，不把mock通过写成完整产品可用，也不要求成员为普通已授权低风险动作反复确认。
