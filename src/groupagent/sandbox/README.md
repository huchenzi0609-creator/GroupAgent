# 隔离进程接口

`DockerSandbox(image, docker_host=..., limits=..., allowed_programs=..., policy_guard=...)` 实现 `SandboxRunner.run(ProcessSpec, ToolContext)`。镜像必须是不可变 sha256 ID/digest；没有 Docker daemon 或镜像时失败关闭。没有宿主 Shell/Python 回退，也不把 Python 路径检查称为操作系统隔离。

`ProcessSpec` 指定 program、字面 argv、显式 input_paths/output_paths、stdin、timeout_seconds、max_output_bytes。宿主使用 scoped openat/禁止符号链接及硬链接的私有文件检查流式发送选定输入，不挂载任何宿主路径。容器 `/work` 和 `/tmp` 是限额 tmpfs，根文件系统只读；只导出声明的普通文件，校验字节长度与 SHA-256，以排他创建写回当前 work 工作区。失败、超时、取消和输出超额不导出文件；缺失声明输出也是失败。

容器无外部网络、无 Docker socket、无通道 profile、无数据库或供应商凭据，使用 CPU／memory／swap／PID 限额、no-new-privileges 与最小 supervisor capabilities。可信 supervisor 与不可信子进程身份不同，子进程 UID/GID 65532、零 capabilities；子进程不能读 supervisor 私有文件或向其发信号。POSIX rlimit 限制 CPU、文件大小、core 和打开文件数。超时／取消结束进程组及余留后代；宿主 relay 丢失时 stdin EOF watchdog 也结束子进程，避免 orphan worker 继续运行。

宿主新建容器带 managed 标记与 group/work 哈希标签，不把原成员／工作 ID 放进标签。移除后必须从 daemon 成功查询到容器不存在才确认清理；rm 返回或查询不确定会保留 busy，不能仅杀掉 docker CLI 就声称 guest 已停止。`DockerSandbox.quiesce_work` 在已配置的本机 daemon 中只处理匹配标签且名称符合受控模式的容器；不影响其他工作，也不调用外部 shell。

`python_compute` 通过 `python3 -I -c` 在容器内计算；`process_run` 只准许受审查程序的字面参数；`shell_command` 只解析单条允许命令，不运行 shell、不接受管道、重定向、环境赋值、变量展开或命令链。默认程序表为 ls/cat/wc/head/tail/sort，媒体 worker 的入口由可信配置单独提供。未注册或被 scoped gateway 拒绝的调用不会启动容器。

程序允许表约束工具入口，不声称是 Python 语言级隔离：Python 代码可以调用容器内已有程序，其后代仍受同一文件系统、网络、身份、cgroup 与取消边界约束。`noexec` 也不禁止解释器读取脚本文本。安全边界来自容器和可信协议，不依赖过滤几个 Python 关键词。

计算进程 cwd 是 `/work`。input_paths/output_paths 必须使用相对路径，例如 `chart.png`，不是 `/work/chart.png` 或 `/work/workspace/chart.png`；输入必须存在，输出必须是新文件，其宿主受控父目录须事先存在，推荐顶层文件名。工具在调用 runner 前验证这些条件；参数失败返回明确 NO_EFFECT，允许在原预算内修复。runner 已开始后的异常仍保守保留 UNKNOWN，不能按错误名称笼统推断“没执行”。

输出是有界结果数据，不能更改网关允许表或身份。运行配置、worker 请求及日志不包含 controller 凭据；这不声称能识别并清除用户任意输入中所有未知敏感字符串。固定 SDK 使用私有 executable `/tmp` 的例外见 execution README；普通计算／媒体 `/tmp` 均保持 noexec。

2026-11-14 sandbox suite 11 项通过，其中 7 项启动真实 Docker、4 项是配置／预拒绝／缺失 Docker／工具白名单单元测试，覆盖宿主文件／环境／网络不可见、进程身份及 capability、PID／内存／工作区／输出边界、超时取消、子进程清理、controller EOF 和导出检查。测试 VM 为专用 Colima Linux arm64 2 CPU / 3 GiB、无宿主 mounts；没有对现有线上服务器执行部署或压力测试。真实边界测试不是内核漏洞证明，宿主可信 controller 和 Docker daemon 均在可信计算基内。
