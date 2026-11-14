# 本地 worker 构建与资源证据

应用运行只接受不可变镜像 ID/digest。`groupagent-worker:local-readiness` 是本地构建句柄，不能作为生产唯一标识。基础镜像固定为 `python@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72`；Python 依赖来自根 uv.lock 的 hash export，Harness 包另有 `harness-requirements.txt`。apt 安装的实际版本写入镜像内清单；apt 仓库未做日期快照，重新构建可能不同，因此应发布并测试同一最终镜像，而不是宣称位级可重复构建。

```sh
docker -H "$GROUPAGENT_DOCKER_HOST" build \
  --build-arg BASE_IMAGE=python@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72 \
  -f deploy/worker/Dockerfile -t groupagent-worker:local-readiness .
docker -H "$GROUPAGENT_DOCKER_HOST" image inspect groupagent-worker:local-readiness --format '{{.Id}}'
docker -H "$GROUPAGENT_DOCKER_HOST" build --build-arg WORKER_IMAGE="$GROUPAGENT_WORKER_IMAGE" \
  -f deploy/worker/Dockerfile.harness -t groupagent-harness:local-readiness .
```

构建上下文默认拒绝全部，仅允许公开源码／依赖锁／部署文件；不得加入 `.env*`、运行数据库、会话或真实群数据。运行 worker 不挂载本机目录。媒体镜像包含中文／英文字体、Tesseract、Poppler、LibreOffice、Chromium、Playwright、用于音频容器检查的 ffmpeg 和 arm64 Playwright Node 所需 libatomic1；安装 ffmpeg 不授予视频处理能力。

实际验收镜像记录（2026-11-14）：

以下是升级 Pillow 13 之前的历史镜像，只保留早期测试证据，不作为发布目标。父流程需按新 uv.lock 重新导出 requirements.txt，再重建 worker 与 Harness，并记录新 ID 和验证结果；SDK wheel hash 钉不变。

| 用途 | 不可变 ID | 验证 |
| --- | --- | --- |
| 边界与资源测量 worker | `sha256:65444c6be03356ddbcedc07c38cf1ce18c8066ae320449db12f25060afe29863` | sandbox 11 项（7 项真实容器）及并行资源采样 |
| Harness | `sha256:23e34bc9067286559776fa44a5f8b94a176ec3694754bfae2612a44b092f88b1` | 实际 SDK 7 项，含 MCP、追加、取消、原生会话恢复 |
| renderer 冻结后 worker | `sha256:82e85d4e5ee39387b30d71f08a21e30ccba7480daf3926fe0b7ed5a37f340d78` | artifacts 代理报告模板／遮挡 3 项、PPTX 图表／混合 3 项及输入 PG+Docker 16 项通过 |

最后一项不是对全部新源码的最终镜像发布验收；最终 tree 冻结后由父流程重新构建并保存它实际测试的 ID。早期镜像仍作为可复核证据保留，不覆盖旧数据。

## 实际资源测量

`measure_resources.py` 在专用 Colima VM（Linux arm64，2 CPU、3 GiB RAM、20 GiB 开发盘，无 host mounts）同时运行真实 Harness SDK 与离线 Chromium。模型为显式 ScriptedFakeModel，故意延迟 10 秒确保进程重叠；供应商调用数为 0。完整机器可读证据是 `resource-evidence.json`。

| 测试进程 | cgroup memory.peak | pids.peak | 私有 tmpfs 使用 |
| --- | ---: | ---: | ---: |
| Harness SDK | 405.5 MiB | 19 | /tmp 89.3 MiB |
| Chromium 媒体 worker | 428.5 MiB | 121 | /work 184 KiB，浏览器结束时已清理临时目录 |

个别峰值相加为 834.0 MiB，这是两个独立 cgroup 峰值之和，不是同一瞬间测得的 RSS；证据另保存确实并发的 docker stats 样本。工作负载是小型合成网页截图和 SDK 工具循环，不是最大尺寸 Office、200 页 OCR 或 40 MP 图片的压力测试。测量不含控制进程、PostgreSQL、真实微信客户端、操作系统、备份和保留文件。

配置起点：计算 worker 512 MiB；Harness 1 GiB、/tmp 256 MiB、pids 128；媒体 1 GiB、/work 256 MiB、/tmp 256 MiB。浏览器实际 pids.peak 121 接近测试上限 128，建议部署配置给 256 后再验收。两个重型 worker 的内存上限已合计 2 GiB；当前约 2 GiB 的旧线上机器未测试、未连接，不能据此承诺可部署。建议以至少 2 vCPU / 4 GiB 的独立环境作为完整应用预发布验收起点，并用最终并发／媒体上限／保留期测试确定容量；这不是性能保证。磁盘应依据镜像、业务文件和备份保留预算，20 GiB 仅是本地开发配置。

SDK 发布者 wheel 哈希、源码和许可证据见 `harness-manifest.json`。没有独立复现原生 runtime 构建或 PyPI attestation。Linux amd64、Python 3.11 宿主与实际云机的完整运行矩阵仍须使用目标构建重新验证；本轮真正运行的 Linux worker 是 arm64、Python 3.13。
