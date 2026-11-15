# 货币预算账本

PostgreSQL 原子预算预留与真实用量结算。合成单价只允许显式 `allow_test_prices=True` 的离线夹具，严格实例不能启动夹具预留。公开 API 为 `configure / register_price / reserve / begin / settle / unknown / release_unstarted / report`。

`reserve` 和 `begin` 校验当前群/身份、原始 request、work 状态及来源范围。`begin` 在 memory → work → budget policy → reservation 的锁顺序下重新检查封锁、每日和总额上限，防止已预留调用在降额、超支封锁、forget 或取消后继续启动。跨午夜预留在 begin 时移到实际开始日并重查当日总额；`now` 仅供受信测试控制面注入。预留只会开始一次。

网络结果不确定时，unknown 保留全部预留成本，不能推定免费重试。实际已发生的用量在来源删除后仍可结算；超预留费用完整入账并封锁后续调用。若原调用者已经离群，可信 owner 可执行结算/核对；不能用已离群身份恢复付费调用。管理员调整预算不会自动清除已有 blocked。

测试使用随机隔离群和 synthetic price；不读取密钥、不访问供应商、不重置实际总账或用户授权的总额。2026-11-14 的 begin 边界回归覆盖 blocked、daily/total 降额、overrun、午夜、source/dependency forget、paused/cancelled/completed、fixture 价格和删除后真实用量结算。付费供应商用量与价格校验由父级统一受限预算的验收流程完成。
