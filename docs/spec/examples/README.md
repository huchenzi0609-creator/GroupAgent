# 契约示例说明

这些JSON文件是v0.1规格中的合成示例，不是生产数据，也不是可以直接启动机器人的配置。2030年的时间、group_demo等ID均为虚构值。

- event.example.json：来自已验证通道的图像事件，角色仍由服务端策略查询。
- work-order.example.json：已经通过工作准入的Agent目标，与人类TeamTask分开。
- schedule.example.json：具有时区、有效期、补跑和重叠策略的日常工作。

上级config.example.json为非秘密配置草案，生产启用前必须替换占位ID、选择已验证模型与引擎版本、填写日费用限额并确认数据保留策略。视频功能被显式关闭。

JSON可解析仅表示示例语法正确；正式JSON Schema、DDL和端口实现将在S1创建，并以技术契约为准进行兼容测试。
