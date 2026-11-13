# GroupAgent

微信群通用协作智能体的研究与规格准备仓库。GroupAgent 是暂用工程名。

当前阶段为 **Spec v0.1 Draft**：已形成产品行为、技术契约、验收与开发任务初稿。没有实现新版机器人，也没有修改现有线上服务。视频识别已按用户要求移出当前开发计划。

旧原型位于相邻的 `Project_WechatLeader` 目录。本仓库不复制其密钥、登录资料、群成员名单、聊天记录或数据库；研究只引用代码行为和公开资料。

## 本阶段文档

- [初版 Spec v0.1](docs/SPEC_V0.1.zh-CN.md)
- [技术契约与状态机](docs/spec/TECHNICAL_CONTRACTS_V0.1.md)
- [48项验收场景与发布门槛](docs/spec/ACCEPTANCE_V0.1.md)
- [开发任务与依赖](docs/spec/IMPLEMENTATION_PLAN_V0.1.md)
- [规格变更记录](docs/CHANGELOG.md)
- [开发前评估报告](docs/PRE_SPEC_REPORT.zh-CN.md)
- [开发流程与纪律草案](docs/ENGINEERING.md)
- [证据与来源登记](docs/SOURCES.md)
- [下一阶段 spec 准备清单](docs/SPEC_READINESS.md)

报告和开发流程中的选型、时间、性能与成本数字均有明确状态：已核实事实、设计建议、待验证假设。没有把候选架构当成已经实现的功能。

## 仓库状态

本地 Git 仓库已建立。远程仓库、托管平台分支保护与 CI 尚未配置；这些是正式进入实现阶段的准入项，不把文档约定当成已经启用的技术控制。

研究报告与初版spec各自在独立文档分支留存。下一步按规格执行关键验证实验，冻结运行时选型和部署配置，再推进实现；不直接把文档草案当作生产功能。
