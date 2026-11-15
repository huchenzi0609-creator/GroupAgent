# Pot 身份与 2GB 适配验证

日期：2026-11-15。分支：`feat/pot-2gb-server`，基于 `f002afe2be506e322fe91168f16eca862cfe03db`。本轮无付费模型调用，未部署新版或切换微信发送者。

## 已实现

- 机器人自称 Pot，识别大小写及中文相连的呼叫；保留旧称呼和 `/组长`。回复、进度与提醒的每个文本分块统一加一次 `【Pot】`。共享账号仍以可信发送回执区分机器人与本人，避免回环。
- 显式启用 `server_2gb` 档后，使用宿主 Scoped 执行循环；计算、浏览器、文档、媒体和输入预处理共用一个 worker 准入器。单 worker 限 512 MiB、0.75 CPU、192 PIDs，不并驻 Harness。
- 准入按实时可用内存及可见 cgroup v1/v2 祖先余量核算：512 MiB worker + 128 MiB 传输 + 128 MiB 余量。小游戏占用已经计入可用内存，不重复扣减。资源不足时持久排队，5 秒复查；不耗模型轮次、不重放已完成的工具，重启可续做。
- 附件数量、字节、像素限制贯穿下载后、缓存复用、服务和实际 worker 解码。输入 Office/PDF 为 16 MiB，可信生成产物独立保留 25 MiB 上限；协议总输出另留 1 MiB 元数据空间。
- 输入音频在未开始、没有本次付费 reservation 时可无损重试；已有付费记录时保留账务并阻止自动重放。执行结束释放 attempt 服务缓存，繁忙时推迟可选维护；产物清理不跨网络等待持有数据库锁。
- 启动先取得应用 lease，并持续心跳；核对和清理本组带标记的遗留 worker，包括没有 WorkOrder 的音频输入。清理不能确认或旧标记无法归属时关闭重任务准入，聊天保留；其他组、小游戏和旧微信服务不在清理范围内。

## 必要验证

| 验证 | 结果与范围 |
| --- | --- |
| 身份与回环 | 专项 pure 407 PASS、PG 集成 81 PASS；均使用合成群数据 |
| 持久资源调度 | Supervisor PG 40 PASS，包含跨重启、未执行工具续做、目标更新、准备/验证延期及缓存释放；实际 sandbox 用例另测 |
| 输入重试与裁限 | PG 51 PASS + 参数接线后 4 PASS；1 个既有 Docker 用例跳过，不计能力通过；最终新增 pure 18 PASS |
| 媒体/文档裁限 | 新增 pure 27 PASS，相关既有 pure 115 PASS；生成产物与输入大小分别验证 |
| 清理并发 | PG 64 PASS，包含 11 个新并发/漂移 case |
| 最终应用接线与资源选集 | 107 PASS，1 个测试因合成 external_group_id 缺少 `@chatroom` 失败；修正夹具后该 case 单独复验 PASS，产品代码未因该失败改动；1 个真实 sandbox case 排除另测 |
| 最终真实 worker | **5 PASS / 22.38 秒**：DOCX/PDF/PPTX 实际生成与渲染、离线 Chromium、14 MP 图片在 12 MP 门槛前被拒绝。512 MiB 上限下观测峰值约 **320.02 MiB**、124 PIDs；仅代表这些小型合成样本 |
| 重启遗留容器 | 17 个资源 pure case 及 1 个本地真实 idle 音频孤儿清理 PASS；不依赖原进程内存登记 |
| 静态检查 | 全部 Ruff/格式通过；mypy 覆盖 125 个源码文件通过 |

最终 worker 为 `sha256:80b04370b2f1d756c54f8bf89c867df6bbed151d344467f891147903f5cae2e0`，125 个 Python 文件与本地最终源码逐文件 SHA256 一致。它是在原锁定依赖镜像上叠加源码的 **Linux arm64** 开发构建，不是目标 amd64 发布镜像，也不是独立重新下载依赖的构建。证据保存在忽略目录 `runtime/pot-server/final-source-proof.json` 和 `final-worker/`。

## 目标服务器实测边界

通过用户授权的持久维护连接，在 Alibaba Linux 3 x86_64、2 核、1870 MiB 主机上运行实际资源准入源码。专用测试容器无网络、无业务目录挂载，不接入微信或模型。

基线可用约 982 MiB，允许 512+256 MiB 准入；受限测试容器临时占用 400 MiB 后，可用约 602 MiB，拒绝新重任务；清理后可用约 1010 MiB，恢复准入。同一准入器第二次 reservation 被拒绝，释放后恢复。测试容器已删除。小游戏/PM2 的 PID 和启动时间、旧微信容器 ID 与运行状态前后一致。证据为 `runtime/pot-server/{baseline,pressure,recovered}-probe.json` 及 `coexistence-summary.json`。

这证明目标 Linux 的余量采样、准入和短时共存行为，不证明新版整机已运行。目标机仍需独立 Python 运行时、PG、amd64 worker 以及完整服务共存验证；真实微信、shadow、数据迁移、单发送者切换与回滚门槛保留。复杂 Office、最大媒体或游戏突然增载仍可能触发 worker 上限，需排队、缩小任务或重新评估容量。部署参数与后续顺序见 [SERVER_2GB](../../deploy/SERVER_2GB.md)。
