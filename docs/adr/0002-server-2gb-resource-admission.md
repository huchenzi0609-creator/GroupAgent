# ADR 0002 在 2GB 共存主机上按余量启动单个 worker

日期：2026年11月15日

状态：已选定开发方案；本地验证与目标主机完整验收分开，尚未部署新版。

## 问题

目标服务器为 Alibaba Linux 3 x86_64、2 核、约 1870 MiB 内存，主流程只读盘点时 `MemAvailable` 约 1020 MiB。已有游戏、nginx、Node 与旧微信 client／bot 必须保留。历史 arm64 开发 VM 上 Harness 与浏览器的独立 cgroup 峰值约 405.5／428.5 MiB，尚不包含 PG、controller 与微信；长期并驻后再运行重型工具不适合作为本机起点。

## 决策

新增显式 `resources.profile=server_2gb`，标准档保留原行为。2GB 档默认宿主 `scoped` 执行器与单重型工作，拒绝配置驻留 Harness 或重型并发不为 1；模型循环留在 controller，需要计算、浏览器、媒体或渲染时才启动短时隔离 worker。

共享一个 `ResourceGovernor` 串行放行实际容器：默认 worker 512 MiB，另预留传输 128 MiB 与宿主余量 128 MiB，准入门槛 768 MiB。读取宿主 `MemAvailable` 与可见 cgroup v1／v2 祖先剩余额度，取最小值；邻居服务已体现在余量中，不重复减其 RSS。资源不足或采样不可靠时持久排队，默认每 5 秒复查，不取消工作、不增加付费重试；聊天、控制与投递保留独立处理路径。

worker 默认 0.75 CPU、shares 128、OOM score adj 500、192 PIDs、128 MiB 工作区、64 MiB `/tmp`；输入集合和图片／Office 上限适当收紧，单文件输出保留 25 MiB。保留清理默认间隔改为 180 秒。详细值与部署边界见[2GB 部署说明](../../deploy/SERVER_2GB.md)及[配置样例](../spec/config.server-2gb.example.json)。

## 原因与代价

- 按完整 worker 上限核算，避免把历史小样本峰值当成容量保证；传输预留覆盖 25 MiB 输出经 base64／JSON 处理的多份副本。
- 现有服务无需停机让路；低余量时任务延迟，吞吐降低。该门槛是启动时采样，其他服务随后增长仍可能使 worker 触发 OOM。
- 512 MiB、192 PIDs 和 64 MiB 临时空间较紧；PIDs 上限容纳已有 Chromium 129 PIDs 的夹具峰值且不预分配内存，复杂浏览器、Office 与最大媒体仍需实际验证。
- controller 直接运行于 Linux 宿主是默认部署方式。若 controller 限在小 cgroup，即使 child 在 sibling cgroup，仍会保守拒绝；不为提升可用性绕过可见限制。
- worker 清理未确认时关闭后续准入，避免失联容器与新任务叠加；需维护者核对残留与恢复证据。

## 替代方案与重新评估

不通过停止游戏或旧微信服务释放容量；也不把仅限单工作理解为 Harness 与浏览器可以同时驻留。更大主机或独立执行节点是后续选择；持续资源排队、目标工作集无法在本档完成，或 controller／PG 本身余量不足时，重新评估拆分与容量，而非自动放宽并发或内存门槛。

依据为本地[资源测量](../../deploy/worker/resource-evidence.json)、当前[配置](../../src/groupagent/config.py)与[资源准入](../../src/groupagent/resources.py)源码，以及主流程 2026年11月15日 `pot-server` 只读盘点（脱敏日志留在忽略目录 `runtime/pot-server/`）。既有 arm64 镜像不能替代目标 amd64 构建；本 ADR 不声称新版 2GB 整机验收、真实微信切换或生产部署已经完成。
