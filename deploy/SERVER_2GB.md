# Pot 的 2GB 共存部署档

日期：2026年11月15日。状态：配置与部署方案；新版尚未在目标服务器部署，不能据此认定整机容量验收通过。

## 当前边界

主流程通过本机 SSH alias `pot-server` 只读盘点目标主机：Alibaba Linux 3、x86_64、2 核、约 1870 MiB 内存，采样时 `MemAvailable` 约 1020 MiB。这是瞬时余量，不能作为持续可用容量。脱敏证据保存在本地忽略目录 `runtime/pot-server/`；不在仓库保存主机 IP、账号凭据、密钥、登录会话或真实群数据。

保留现有 PM2 小游戏、nginx（80/443/8080）、Node（3001/3002）以及旧微信 client／bot；旧 client 约 211 MiB、bot 约 28 MiB 的 RSS 仅供盘点。本轮不停止、删除或改配这些服务。`MemAvailable` 已反映它们的占用，准入时不得再减一次游戏或旧服务 RSS。

目标主机尚无新版要求的 Python ≥3.11、uv 或 psql，现有 Docker 可用。不要替换 Alibaba Linux 的系统 Python；后续需要独立准备应用运行时、PostgreSQL 16+、匹配的 PG 客户端与锁定依赖。当前本地 worker 镜像为 Linux arm64（约 696 MB），不能直接当作该 x86_64 服务器的运行镜像；需要相同源码的 Linux amd64 构建与验证。

用户已明确授权保留专用、持久 SSH 维护连接；本机入口为 `ssh pot-server`。这是维护授权，不是部署或单发送者切换完成的证据。私钥、known_hosts、SSH 配置和服务器授权公钥均留在各自受控位置，不复制入仓库；连接不向 worker 开放。

## 配置与资源准入

采用 [2GB 配置样例](../docs/spec/config.server-2gb.example.json)。它仍是 `development`／`local`，关闭群处理和真实发送，包含未配置的模型与费用门槛；不能直接用于生产。[标准样例](../docs/spec/config.example.json)保留原执行器和输入上限，只有显式选择 `resources.profile=server_2gb` 才启用此档。

| 配置 | 2GB 档默认值 |
| --- | --- |
| 执行器／重型并发 | `scoped`／1；显式 `harness` 或并发不为 1 时拒绝配置 |
| worker 内存／CPU | 512 MiB／0.75 CPU；CPU shares 128，OOM score adj 500 |
| worker 进程／工作区／临时空间 | 192 PIDs／128 MiB／64 MiB |
| 宿主余量／传输预留 | 128 MiB／128 MiB |
| 资源复查／保留清理间隔 | 5 秒／180 秒 |
| 图片 | 每张 8 MiB、12 MP，每工作最多 4 张 |
| Office／PDF | 每文件 16 MiB |
| 输入集合／工作区 | 最多 8 件、总计 32 MiB／128 MiB |
| 单个输出文件 | 保留 25 MiB |

字段名中的 `mb` 按 MiB 使用。`/work` 和 `/tmp` 是受限 tmpfs，其实际占用计入 worker 内存；512 MiB 不是可在其上额外叠加的进程预算。0.75 CPU 是配额，shares 是争用时的相对权重，OOM 调整用于让 worker 优先承担压力，不构成其他服务绝不会受影响的保证。PIDs 上限 192 用于容纳已有 Chromium 夹具测得的 129 PIDs；上限不会预先分配相应内存。

默认启动门槛为 **768 MiB = 512 worker + 128 传输 + 128 宿主余量**。传输预留考虑 25 MiB 输出在 base64／JSON 协议处理时的多份内存副本，主进程瞬时需求可约达 100 MiB；它与 worker cgroup 上限分别核算。

控制进程内共享一个 `ResourceGovernor`，读取 Linux `/proc/meminfo` 的 `MemAvailable`，并取可见 cgroup v1／v2 当前层及祖先剩余额度的最小值。低于门槛、信息不可读或已有 worker 时，工作留在持久队列，按默认 5 秒复查；资源等待不取消工作、不扣模型重试次数，也不重发已有副作用。聊天、控制、状态与投递继续走独立路径。每次只启动一个短时隔离 worker，退出并确认清理后才释放通道；不保留 Harness 与浏览器并驻。

优先让 controller 直接在 Linux 宿主的独立应用运行时中运行，以读取宿主资源。若 controller 自身在严格限内存的容器里，即使 Docker daemon 把 child worker 放在 sibling cgroup，准入仍会取 controller cgroup 的较小余量，可能保守拒绝启动；当前设计不绕过这条限制。

## 配置身份与准备顺序

两个样例统一 `runtime.bot_name=Pot`、`runtime.bot_badge=【Pot】`；`bot_aliases` 保留“机器人、机器人组长、组长、小伴”。名称与 badge 必须一致，旧配置迁移时同时检查这两项。每个文本分块只加一次 badge，回环判断使用可信发送回执；共享账号中本人手动发言仍属于本人，不凭 badge 或账号就当成机器人回声。

后续部署按以下顺序形成可审阅结果：

1. 保存旧服务、端口、内存／cgroup 与磁盘基线；在独立构建环境产出固定源码和依赖对应的 Linux amd64 worker，记录 digest，不在这台共存主机上并行构建重型镜像。
2. 准备独立 Python 运行时、应用用户、私有 PostgreSQL／DSN、备份和应用数据目录；使用 [systemd 模板](groupagent.service)前核对用户、路径、权限和兼容性。模板尚未安装，也未给 controller 设置经过验证的整机内存上限。
3. 从 2GB 样例建立私有配置，核对不可变 `runtime.worker_image`、`scoped`、单重型并发、群权限、路由、费用、保留与验证版本。无需配置 `harness_image`。先只做离线校验：

   ```sh
   .venv/bin/groupagent check-config docs/spec/config.server-2gb.example.json
   .venv/bin/pytest tests/test_resource_config.py -q
   ```

   这些命令在已准备好的开发环境执行，不连接服务器、数据库或模型；通过不证明镜像可用或整机可运行。

4. 分别保存本地真实 worker 的 512 MiB 实验、目标服务器资源／容器共存 smoke、新版完整服务目标主机验收。前两者均不能替代第三项。目标验收需覆盖浏览器、Office／OCR、输入上限、产物传输、资源排队／取消、重启恢复及既有游戏和旧 client 的可用性；不把历史 arm64／1 GiB worker 结果移植为本档通过。
5. 真实微信群、shadow、迁移、单发送者切换与回滚仍按[发布门槛](../docs/spec/ACCEPTANCE_V0.1.md)和[运行手册](../docs/development/LOCAL_RUNBOOK.md)执行。当前未启动新版，不改变现有生产发送者。

## 证据与限制

本档依据当前 [配置实现](../src/groupagent/config.py)、[准入实现](../src/groupagent/resources.py)及[配置测试](../tests/test_resource_config.py)，决策见 [ADR 0002](../docs/adr/0002-server-2gb-resource-admission.md)。历史 [worker 资源证据](worker/resource-evidence.json)来自 3 GiB arm64 开发 VM、小型合成负载、零供应商调用：Harness 与浏览器的独立 cgroup 峰值约 405.5／428.5 MiB，二者之和不是同时 RSS，且不含微信、PG 或 controller。

2026年11月15日首轮本地真实 worker 实验已完成 DOCX／PDF／PPTX 生成并渲染、离线 Chromium 共 4 项，4 PASS／36.34 秒。worker 实际限制为 512 MiB，最高 `memory_peak_bytes=357519360`（约 341 MiB），浏览器 `pids_peak=122`；原始计数留在 `runtime/pot-server/local-worker/*.json`，用例见[真实 worker 资源测试](../tests/sandbox/test_lowmem_profile.py)。这是 Linux arm64 worker 与合成小文件／网页的证据；macOS controller 的余量由固定夹具提供，不是整台 2GB 主机实测。后续图片／导入上限修复的最终镜像与源码复验由主流程另行绑定，不能把这轮结果视为目标 x86_64 整机通过。

目标 x86_64 服务器另完成真实 cgroup v1 准入与共存 smoke：基线可用 982 MiB 时允许 512 MiB worker 预算；增加受限的 400 MiB 合成邻居后可用降至 602 MiB，准入拒绝；移除合成负载后恢复至 1010 MiB，再次允许。`coexistence-summary.json` 记录前后游戏 PM2 PID／启动时间相同、旧 client／bot 容器 ID 和 running 状态相同、合成探针已移除、供应商调用数 0、新版未部署；详细 JSON 保存在忽略目录 `runtime/pot-server/`。该 smoke 没有运行新版完整服务、PG 或真实微信链路。最大媒体／复杂 Office、长期负载与目标 x86_64 完整应用容量仍待验收，不能以两类隔离实验替代上线结论。
