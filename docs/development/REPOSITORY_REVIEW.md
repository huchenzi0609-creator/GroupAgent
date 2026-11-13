# 私有仓库发布审查

审查时间：2026-11-14 00:24:43，Asia/Shanghai（2026-11-13 16:24:43 UTC）。

结论：**本次审查范围内未发现阻断项，可以将已审查的候选源码树发布到用户私有 GroupAgent 仓库。** 此结论针对源码仓库发布，不表示新版服务、真实微信账号、Harness、隔离 worker 或远程 CI 已通过上线验收。

## 范围与快照

- 本地分支：`feat/control-plane-foundation`。
- 历史锚点：`5b0ca458e863339bdf3ac35ae400d1e713ee660c`。
- 读取当前分支全部 4 个可达提交、26 个唯一历史 blob；本地其他分支也未增加可达提交。
- Git 索引包含 20 个 blob，均已包含在上述历史扫描范围，没有另行暂存但未扫描的版本。
- 当前工作树：100 个候选文件，共 1,085,126 字节；范围为已跟踪文件加未被忽略的未跟踪文件，不含本报告自身。
- 候选清单指纹：`e92e15092a26cdb0704efe0ee723e159bb5cbaf34c2438144b2edaef8856c8b3`。计算方式为按路径排序的 `(相对路径, 文件 SHA-256)` 列表，按 Python 默认 `json.dumps` 序列化后再取 SHA-256；排除本报告，避免递归指纹。
- 最终复扫已纳入 `README.md`、`docs/ENGINEERING.md`、`docs/development/IMPLEMENTATION_STATUS.md`、`src/groupagent/transport.py` 和 `tests/channels/test_intake_integration.py` 的新增或变更内容。

本报告只新增此文档，没有修改其他文件，没有提交或推送。若最终上传树包含后续新增／修改内容，应对变化部分补查；本报告不自动覆盖之后的代码。

## 实际方法

1. 用 `git ls-files`、`git ls-files --others --exclude-standard` 枚举候选文件，检查文件类型、符号链接、大小和二进制特征；没有沿符号链接读取外部文件。
2. 用 `git rev-list HEAD`、`git ls-tree -r -z`、`git cat-file blob` 检查完整可达历史中的文件内容；另查索引对象和提交消息。不是仅检查当前 diff。
3. 使用本地 Python 定制规则检查私钥头、常见供应商凭证格式、JWT 格式、带用户凭证的连接 URL、凭证字面量、可识别的微信身份格式、电子邮件、IP 字面量和高熵引号内 token。仅输出路径、行号和类别，没有输出疑似秘密值。
4. 对匹配项按代码位置、类型声明、测试用途、保留测试域名、软件版本语境及合成 fixture 标记逐项归类。另核对样例的合成数据声明和运行产物路径类型。
5. 用 `git check-ignore --stdin` 检查 13 类代表性敏感路径的忽略规则。
6. 用 `tomllib` 解析 `uv.lock` 与 `pyproject.toml`，检查分发来源、URL 凭证和哈希字段；用 `importlib.metadata` 读取已安装依赖的许可声明及许可文件存在性。对本机未安装的条件依赖补查固定版本的官方 PyPI 页面。

**没有运行专业完整扫描器。** 本环境未安装 Gitleaks、TruffleHog、detect-secrets、pip-audit、REUSE、Syft 或独立许可扫描器，也没有为本轮审查安装它们。`rg` 仅用于文件／关键词定位；上述定制规则检查不能被描述为专业秘密扫描、全面漏洞扫描或完整供应链审计。

## 数据与秘密检查结果

未发现真实 API 凭证、私钥、登录会话、真实群成员 ID、聊天导出、二维码内容或私密附件混入候选文件或可达历史。未发现候选符号链接、子模块、二进制运行产物或内置 vendor 目录。当前秘密格式规则的 12 个匹配均完成归类，没有未解决的疑似项。

| 路径／范围 | 匹配类别 | 核验结果 |
| --- | --- | --- |
| `.github/workflows/ci.yml` | 数据库口令与连接 URL | 明确标记的临时合成 CI fixture；没有真实服务凭证 |
| `tests/channels/conftest.py` | 模拟连接／认证字段 | 合成测试数据 |
| `tests/channels/test_agentwechat.py`、`tests/execution/test_deepseek.py` | 类似邮箱的 URL authority | 测试域名与拒绝凭证 URL 的输入样例 |
| `tests/research/test_research.py` | IP、连接 URL、认证相关字面量 | 网络边界测试 fixture，没有真实目标账户 |
| `src/groupagent/channels/models.py` | IPv4 字面量 | 本机开发回环地址 |
| `src/groupagent/coordinator.py`、`domain.py`、`execution/deepseek.py`、`persistence/postgres.py`、`research/deepseek.py` | 凭证字段名称附近的匹配 | Python 参数或字段类型声明，没有凭证值 |
| `docs/development/ARTIFACT_POC.md` | 类似 IPv4 的数字串 | 渲染软件版本号 |
| `docs/spec/examples/*.json`、`docs/spec/config.example.json` | 群、成员、工作等身份字段 | 文档明确声明的合成样例 |
| `docs/legacy-source-manifest.json` | 旧项目引用和文件哈希 | 仅本仓库内的既有代码清单记录；本轮没有访问旧项目、数据库或私密资料 |

历史提交保留 1 组正常 Git 作者身份元数据；本报告没有复写姓名或邮箱，未将其误判为群成员凭证。没有在提交消息中发现上述秘密格式匹配。

`.gitignore` 对环境配置、私钥扩展名、数据库、`runtime/`、`data/`、`uploads/`、`artifacts/`、`backups/`、`.venv/`、`.cache/` 和 `vendor/` 的代表性路径检查均通过；这些目录也没有已跟踪文件绕过忽略规则混入本次候选树。被忽略目录的内部内容不属于此次推送集合，本轮未读取其内容。强制添加此类文件会超出当前放行范围。

## 依赖锁与许可

`uv.lock` 包含 37 项：36 个公共 PyPI 依赖和本项目的 editable 引用。检查的 825 条分发记录均使用 HTTPS `files.pythonhosted.org` URL，均带 SHA-256 字段；没有带用户名／口令的下载 URL、非公共 registry、Git 依赖或本地第三方路径依赖。这里验证的是锁文件记录，不等于逐一下载全部平台分发物并重新计算其哈希。

本机已安装的 34 个第三方依赖读取了许可元数据；未安装的 `colorama` 与 `tzdata` 读取了固定版本官方页面。值得单独保留的许可分类如下：

| 依赖 | 观察到的许可声明 | 证据范围 |
| --- | --- | --- |
| `psycopg`、`psycopg-binary` 3.3.9 | LGPL-3.0-only | 已安装包元数据及许可文件；[官方 PyPI](https://pypi.org/project/psycopg/3.3.9/) |
| `certifi`、`hypothesis`、`pathspec` | MPL-2.0 | 已安装包元数据及许可文件 |
| `pytest-asyncio`、`sortedcontainers` | Apache-2.0 系列声明 | 已安装包元数据 |
| `packaging` | Apache-2.0 OR BSD-2-Clause | 已安装包元数据 |
| `Pillow` | MIT-CMU | 已安装包元数据及许可文件 |
| `typing-extensions` | PSF-2.0 | 已安装包元数据 |
| `colorama` 0.4.6 | BSD-3-Clause | [固定版本官方 PyPI](https://pypi.org/project/colorama/0.4.6/) |
| `tzdata` 2026.6 | Apache-2.0 | [固定版本官方 PyPI](https://pypi.org/project/tzdata/2026.6/) |
| 其余锁定依赖 | MIT／BSD 系列声明 | 已安装包元数据；包括 HTTP、模型契约、Office/PDF 等库 |

以上外部页面访问日期为 2026-11-14。当前候选树没有携带 `.venv`、wheel、系统库、字体文件或容器镜像，也未发现内置第三方源码目录或第三方版权头。因此没有发现需要先移除或补齐已复制第三方内容才能进行本次私有源码推送的项目。没有进行源码相似性检测，不能仅由“无版权头”证明所有代码来源。

根 `LICENSE`／`NOTICE` 尚未设置，本次没有替用户选择开源许可；不能把整个项目或其依赖统一描述为 MIT。未来公开分发、复制依赖源码、发布容器／安装包或捆绑字体时，应基于实际携带内容保留并核对各自许可、版权与通知文件。

另外，`pyproject.toml` 的构建后端仍声明 `hatchling>=1.27,<2`，它没有出现在此 `uv.lock` 的运行／开发依赖集合中。构建隔离环境尚未做到同等精确锁定。这是构建可复现性的待改进项，不是本次私有源码推送的秘密或许可阻断点。

## 放行边界

当前发现的发布阻断点：**无**。放行限于上述候选源码树及本报告，不包含被忽略的运行环境和数据。

定制规则可能漏掉未知格式、经过编码、缺少标识或具有特殊语义的秘密；没有用真实群名册逐一比对样例，也没有访问个人密钥或旧项目。本轮未做 CVE 数据库审计、包全部源码审计、完整 SBOM／许可兼容性评估、不可达 Git 对象或 reflog 审查。可达历史与待发布内容已检查，但这些限制不能被省略为“全面安全扫描通过”。
