# Pot test_2 当前部署方案

更新：2026年11月16日。主流程已备份、停止并删除旧 bot，且已从旧 compose 中移除；小游戏、nginx 和 `wechat-leader-client-1` 保留。client 已通过 saved login 恢复，主流程已核对 test_2 的唯一群 ID；真实群标识与登录资料不写入仓库。

目标机已开展 test_2 真实链路验证：用户已在手机核对 Markdown、DOCX、PDF、PPTX 四种格式，并核对一条真人语音的 ASR 结果。最新聊天修复的四条真实问题均给出有效答案、零占位回复；从消息时间至平台接受答复分别为4.580、6.983、5.561、5.243秒，服务入库至接受为2.834—5.124秒。样例覆盖身份、概念、无前缀连续追问及长期记录查询；这不是P95统计、手机收到时间或长期共存保证。

目标机 systemd 239 不识别 `NetworkNamespacePath`。当前采用 Docker controller 共享保留 client 的网络命名空间；宿主保留 Python 3.6.8，不替换系统 Python，也不再依赖向宿主传输完整 Python／venv。

| 组件 | 当前选定方式与边界 |
| --- | --- |
| controller | 与 worker 相同的最终 Linux amd64 镜像，显式使用可信入口运行 Python 3.13 应用；`--user=994:990`，对应 `pot-test2` |
| controller 网络 | `--network=container:wechat-leader-client-1`，复用 client 本地 API 与现有网络，不新增公网入口 |
| controller 挂载 | 受限 broker Unix socket、自有应用数据、PG Unix socket、只读配置／DSN、受控客户端工具及 tmpfs 凭据；不挂载 raw Docker socket、微信 profile 或 SSH 目录 |
| worker | 512 MiB，CPU默认请求0.75核；受限 broker 自适应调节配额，同一个 controller 的准入器串行启动 |
| PostgreSQL | 独立 Docker 16.16，socket-only、`--network=none`、192 MiB；当前启动脚本配置0.5 CPU，仅挂载自己的数据与 socket 目录 |
| Docker broker | root 控制、代码与策略由 root 所有；复用已有固定 Python 3.11 镜像的小容器，`--network=none`、64 MiB、0.15 CPU |

controller 内存硬上限1280 MiB、软限256 MiB，当前启动脚本CPU配额0.75；内存限额不预占对应 RAM。worker 自适应CPU已观测到约0.25—1.03核的配额调整，1.5核只是允许的上限，本轮未观测达到，不能当成实际使用量或吞吐保证。broker、PG与worker的限额分别核算；`server_2gb`保留`scoped`、单重型工作、128 MiB宿主余量和128 MiB传输余量，默认准入门槛768 MiB。准入取`MemAvailable`与controller可见cgroup headroom的较小值；游戏与client占用已体现在宿主余量中，不重复扣减RSS。

`pot-test2`（UID 994／GID 990）不加入 Docker 组。该 root 等效授权已被自动审批拒绝，当前方案使用受限 broker。**只有 broker 挂载 raw Docker socket**；controller 只连接受限 Unix socket。broker 严格限制固定镜像、群／作用域标签、容器名字、资源配额、无网络、只读根文件系统及允许的隔离字段，拒绝任意挂载或主机权限扩展。实际策略见 [pot_docker_proxy.py](pot_docker_proxy.py)，安装与启动脚本由主流程审核；这里不把代码存在当作运行验证通过。

client 重建或网络命名空间变化后，主流程须重新核对网络绑定并按实际服务脚本重建 controller；已有 controller 不会自动跟随新命名空间。PG 和 broker 继续使用各自的无网络容器与 Unix socket，不通过开放新 TCP 端口解决连接。

原 CI 基础镜像 pin 实际为 arm64；本轮目标机构建已改用主流程校验的官方 amd64 manifest（digest前缀`f3dac…`）。完整digest、运行镜像ID及各轮应用源码绑定以主流程构建证据为准；早期arm64测试不替代本次x86_64结果，已完成的群样例也不延伸为最新对话改动的测量结果。

凭据由 root 预启动 helper [prepare_pot_runtime_secrets.py](prepare_pot_runtime_secrets.py)准备。helper 兼容宿主 Python 3.6+，只读取显式指定的服务器现有 canonical 旧 bot `config.local.json` 的 `api_key` 和 client `native/api-token`，不复制本机 Secret，不改原文件，也不另建持久明文副本。唯一发布目标为 tmpfs 中的 `/run/pot-test2/secrets.env`：目录 root 所有、组 `pot-test2`、0750且组不可写，文件归 UID 994／GID 990、0600。文件仅包含 `DEEPSEEK_API_KEY` 与 `GROUPAGENT_CHANNEL_TOKEN`；helper 原子替换且不记录值。controller 只读挂载该 RAM 文件，停止服务后可由 root 移除。

本轮预算为原授权剩余额度的明确子分配，**不是新的 50 CNY**。原账本有效上界 17.266165 CNY，原付费入口已冻结；原 policy 50／50、revision 1 及既有 settled／unknown 记录保留，不伪结算。test_2 新组划拨 cap 20 CNY，合计有效上界不超过 **37.266165 CNY < 原 50 CNY**；绑定与冻结审计由主流程的可信 allocation manifest 记录。runner 不创建或重置预算，真实调用前仍由主流程核对费用与剩余额度。

按用户最新范围，语音生成／TTS暂缓：目标机与配置样例均为`speech_reply_on_request=false`，不要求TTS路由，不新增语音生成调用；文字、附件与ASR保持启用。普通聊天不生成提前占位回执；名字及唯一、来源完整的已知记录可直接回答，其他问题仍由模型判断。回声凭原生接受回执提前过滤，撤回仍走隐私处理；完整问题句可跳过两秒合并等待，角色、参与策略与执行权限不变。受理重任务时说明已知的计算、研究或文件工作。

更新入口为[apply-pot-source-update.sh](apply-pot-source-update.sh)：先保存镜像、非秘密配置与源码，再在已核对的固定基础镜像上离线叠加源码；只切换controller／broker，进行应用lease健康检查，失败自动恢复原镜像与源码。群策略revision单调递增，不重置预算或历史回执。此入口只接受维护者审核的公开源码包，不用于用户附件。

本目录中原生宿主 Python／venv、原生 PG、`pot-test2.service`、`pot-test2-postgres.service` 及 [prepare-pot-test2.sh](prepare-pot-test2.sh) 属于保留的可选旧方案，**不是当前目标机的首选安装步骤**。其中生成 `NetworkNamespacePath` 的 drop-in 不适用于当前 systemd 239；不得直接按旧模板安装。当前容器服务脚本、固定镜像、挂载权限、PG 认证及匹配版本的备份恢复入口由主流程另行生成并验证。
