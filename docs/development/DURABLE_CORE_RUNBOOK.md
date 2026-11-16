# Pot 标准执行内核：本地验收与发布

更新：2026-11-17。迁移分支 `refactor/pot-durable-agent-core`。架构依据为 [ADR 0006](../adr/0006-standard-durable-agent-core.md)。本轮结果另见验收记录；历史 Harness/50元验收命令不再作为当前运行说明。

## 构建与本地验证

固定 `langchain==1.5.0`、`langgraph==1.2.22`；其余制品由根 `uv.lock` 锁定。只构建一个隔离 worker 镜像，控制进程中的标准内核不需要常驻 Harness/Node/工作流服务器。

```sh
UV_CACHE_DIR="$PWD/.cache/uv" uv --no-config sync --locked
.venv/bin/python scripts/dev_postgres.py start --runtime-root runtime/postgres-kernel --port 55441
export GROUPAGENT_TEST_DSN="$(.venv/bin/python scripts/dev_postgres.py dsn --runtime-root runtime/postgres-kernel --port 55441)"
.venv/bin/pytest --strict-markers --strict-config -m "not render and not sandbox and not live" -q
.venv/bin/ruff check src tests scripts deploy
.venv/bin/ruff format --check src tests scripts deploy
.venv/bin/mypy --no-incremental src
```

独立进程的并行测试使用独立数据库，不能共享迁移/恢复测试的数据库。`skip` 不算通过。真实 worker 按 [构建说明](../../deploy/worker/README.md) 配置不可变镜像和本地 Docker socket，运行：

```sh
.venv/bin/pytest tests/sandbox tests/runtime/test_durable_worker_pipeline.py tests/execution/test_langgraph_core.py -q
```

完整故障场景包含：真实缺失输出→结构化失败→图检查点让出→全新 Supervisor/Toolkit/ModelPort→修复计算→生成含图 PDF→真实渲染与内容校验→本地文件回执。其模型为显式离线脚本，不能称作真实模型验收。

真实 DeepSeek 验收使用现有本地密钥和合成 LocalChannel，不连接微信：

```sh
.venv/bin/python scripts/live_durable_core_acceptance.py \
  --run-id UNIQUE_RUN_ID --dsn-file runtime/postgres-kernel/dsn.txt \
  --worker-image sha256:REPLACE_WITH_VERIFIED_IMAGE \
  --docker-host unix:///REPLACE_WITH_LOCAL_DEVELOPMENT_SOCKET
```

运行前替换镜像/socket 占位符。默认验证自然短答、实际 Python、含图 PDF、公开表情包 Word 和 PPT；可以 `--case` 指定故障修复后的必要复验。保留失败报告，不覆盖 run-id，不将多轮结果伪合并成一次冻结版本通过。Mac 上的固定内存样本只用于走通 `server_2gb` 档；真正的整机共存需在目标机单独测量。

费用和用量继续记账；当前用户取消金额、token、整个目标的累计步数预算。配置显式使用 `spend.production_requires_explicit_limit=false`、两个金额字段 `null`、四个累计 execution 上限 `null`。旧有限政策只作为兼容和隔离测试保留，不能被旧验收脚本重新写回实际群。

## 目标、暂停与恢复

- activation 让出、资源不足、可恢复工具/网络错误：持久排队和下一唤醒，不要求用户再说“继续”。
- 普通工具/草稿输出错误：同一个标准模型—工具图中返回观察、修正执行。
- 纯推理崩溃：先取得新工作租约和更高 fencing token，旧响应不能再通过网关；保留原费用未知状态后继续。
- 外部操作未知：保留原幂等键并核对，不能重置账本或盲目重发。
- 用户手动暂停/取消、撤销来源或权限：保留真实控制含义，不能被自动恢复抹去。
- “完成”：产物已验证、当前目标版本的必需交付已获通道接受；不表示成员已读。

## 数据保留与 test_2 发布

1. 完成本地回归、真实 worker、真实模型任务与恢复门槛，固定源码、依赖锁和镜像。
2. 只读记录目标机的游戏/微信/数据库/控制器资源及活跃进程；使用已有 Pot 专用持久 SSH。
3. 验证数据库与文件备份，停止 Pot 唯一发送者并确认旧隔离工作退出；保留小游戏运行。
4. 在受限构建中离线安装锁定的 x86_64 依赖并替换完整应用目录，避免 COPY 留下已删除旧文件。
5. 仅镜像/内核实现变化用 `groupagent.ops.release` 的受审计配置交接，保留授权 revision。它核验旧 digest、应用已停止以及所有非实现字段相同；群、权限、功能、费用或资源政策的变化必须走各自迁移。
6. 保留旧工作与检查点。安全旧 scoped 检查点迁入标准图；旧 UNKNOWN 草稿只有实际进程、原工具请求、发布历史、原生回执和路径证据共同证明后才能继续。未知外部写入仍核对。
7. 启动单一新发送者，在 `test_2` 验证自然问答、实际任务、文件接受和回声去重，核对游戏及资源基线。
8. 失败时停止候选，用候选 reader 隔离新执行状态再恢复旧可执行文件和镜像；保留原数据库与文件，不删状态解除阻塞。

执行器没有生产管理 Shell 或数据库凭证。操作系统服务维护、配置交接及备份只属于可信维护路径。
