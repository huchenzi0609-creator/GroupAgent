# 隔离 worker 构建

更新：2026-11-17。控制进程采用 `langchain==1.5.0` + `langgraph==1.2.22`，无需另建 Harness 镜像。只有受限计算、文档、图片/ASR 和浏览器工具启动短时 Linux worker。旧原生 SDK 镜像和 2026-11-14 的 `resource-evidence.json` 仅保留历史测量语境，不作为当前容量证据。

Python 依赖由根 `uv.lock` 导出并校验制品哈希：

```sh
uv --no-config export --frozen --no-dev --no-emit-project --format requirements-txt --output-file deploy/worker/requirements.txt
docker build --platform linux/amd64 \
  --build-arg BASE_IMAGE=python@sha256:f3dac913ab53b78c64f26d5518fcd7043d5a1a46522d6a493b598c3ed12f62a6 \
  -f deploy/worker/Dockerfile --iidfile runtime/build/worker.iid .
export GROUPAGENT_WORKER_IMAGE="$(cat runtime/build/worker.iid)"
```

测试和实际运行使用最终不可变 IID/digest。基础镜像多架构索引与实际目标架构分别核对；apt 仓库未做日期快照，不宣称跨时位级可重复构建。部署和验收使用同一最终制品。离线依赖层也必须由同一锁文件与目标 Python/架构生成。

构建上下文只允许公开源码与依赖，不能复制密钥、群消息、数据库或本机登录数据。增量构建先在镜像中移除旧 `groupagent` 包，再复制完整新包，防止已删除旧执行器文件残留。实际群文件和数据库在镜像外保留。

镜像含中文/英文字体、LibreOffice、Poppler、OCR、Chromium、Playwright、ffmpeg。安装音视频容器工具不授予视频或TTS产品能力。worker 无宿主挂载、无网络、非root、只读根目录，受cgroup/进程数/私有tmpfs限制；控制进程独占接入凭证与数据库权限。

2GB 档重型工具单并发，默认512MiB上限；内存准入使用实时 `MemAvailable`、cgroup余量和预留，邻近游戏的占用已在系统余量中体现。所有子进程、输出发布和租约须确认结束后才释放资源。具体整机共存以目标机本轮证据为准。

```sh
.venv/bin/pytest tests/sandbox tests/execution/test_langgraph_core.py tests/runtime/test_durable_worker_pipeline.py -q
```

需要真实本地 Docker、不可变镜像、独立测试数据库和 `GROUPAGENT_DOCKER_HOST`。模型/通道夹具仍属合成；真实模型与微信测试另行记录，跳过不算能力通过。详见 [当前运行手册](../../docs/development/DURABLE_CORE_RUNBOOK.md)。
