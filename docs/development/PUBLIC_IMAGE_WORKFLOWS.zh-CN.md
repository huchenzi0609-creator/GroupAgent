# 公开图片检索与交付回归集

版本：1。任务数据：`tests/fixtures/public_image_workflows.v1.json`。

此任务族覆盖“找表情包，按指定格式直接发图片”。使用合成用户、公开地址占位符和真实可解码图片；普通单元测试不使用群聊、密钥或付费模型。完整的PNG交付用例实际调用控制器工具、转换、校验并生成图片交付草案；内联工作进程与模拟供应商不冒充真实隔离或微信验收。

| 编号 | 场景 | 执行覆盖 |
|---|---|---|
| IMGWF-001/002 | 两张／五张JPEG来源转换为PNG，按不同原始来源交付 | `tests/runtime/test_public_image_conversion.py`读取数据集并参数化运行 |
| IMGWF-003 | 五张图片按供应商和资源边界分批，保持顺序和真实绑定 | `tests/media/test_vision_batching.py` |
| IMGWF-004 | 后一批读取失败，保留已完成结果与全部费用状态，再继续剩余图片 | `tests/media/test_vision_batching.py` |
| IMGWF-005 | 同一来源的不同编码仅计一次；格式缺失与重复给出不同诊断 | `tests/research/test_image_transforms.py`、`tests/runtime/test_public_image_conversion.py` |
| IMGWF-006 | 授权跨工作复用、私有父版本边界、撤回与离群 | `tests/runtime/test_public_image_inputs.py`、`tests/runtime/test_public_image_conversion.py` |
| IMGWF-007 | 历史失败检查按可信只读契约恢复；未知写操作仍隔离 | `tests/execution/test_read_reconciliation.py` |

运行整个任务族（`GROUPAGENT_TEST_DSN`只能指向隔离数据库）：

```sh
.venv/bin/pytest -m public_image_workflow -q
```

该标记包含在普通CI中。带真实模型和微信的验收另存私有运行记录：检查实际下载及转换后的图片、最终微信接受回执、任务状态和旧收费记录保留，不用模型自述或`artifact_deliver`的选取结果代替真正交付。

保持的边界：公开来源血缘由受信转换与目录证据证明，不接受模型补写URL；格式转换不等于新来源；读取失败不等于外部业务写入未知；分批结果不声称进行过供应商未实际看到的跨批全局比较；没有新的必要用户输入时继续修复已有产物，而不是要求再次确认相同交付形式。
