# 任务事实与连续追问回归

任务族：`work_awareness`。版本化数据集：`tests/fixtures/work_awareness_dialogue.v1.json`。

合成对话共17轮：创建主播资料及两张梗图工作，穿插八轮其他话题，再自然追问、质疑已有结果、唤醒和补图。身份、对话、图片及地址均为合成数据，不包含真实群记录。

| 验证 | 执行入口 |
| --- | --- |
| 完整对话、后台与本地交付、每工作一次执行 | `tests/runtime/test_work_awareness_dialogue.py` |
| 权威索引、读取完整性、活动与产物、发送阶段 | `tests/runtime/test_work_facts.py` |
| 空概览中的历史检索、完整空查询和部分结果 | `tests/runtime/test_work_query_outcomes.py` |
| 成员 alias、元数据来源删除级联 | `tests/runtime/test_work_facts_privacy_dependencies.py` |

在隔离 PostgreSQL 中运行：

```sh
.venv/bin/pytest -q -m work_awareness
```

完整对话使用真实协调器、执行器、工具网关和数据库，配以明确的假模型、合成检索和本地通道。它验证事实流、控制边界、无重复与实际 PNG 字节；不代表真实模型的语言理解、互联网搜索、OS隔离或微信接受。

真实模型验收另用少量合成对话检查 `query_work` 路由与普通交流；测试群验收核对既有真实工作及其原生接受回执，避免重新执行已经交付的任务。调用费用沿用已授权策略并保留计量，不恢复旧金额或累计执行配额。验收原始记录保存在私有运行目录，不提交到 Git。
