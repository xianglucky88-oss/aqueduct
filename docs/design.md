# 采集链路设计说明

## 目标

把四类来源（供应商目录、商品、库存、公告）的增量数据稳定落到 `records` 表，
为下游对账、选品、运营看板提供单一数据源。

## 数据流

```
mocktarget / 上游站点
        │  HTTP GET（cursor | page | single）
        ▼
  internal/fetcher          单站点客户端，负责分页与解析
        │  []model.Record
        ▼
  internal/collector        单站点采集流程，负责游标与运行记录
        │
        ▼
  internal/store            批量 upsert 到 PostgreSQL
        │
        ├──> runs      每轮一条
        ├──> cursors   每站点一条
        └──> records   业务数据
```

## 三种分页模式

- `cursor`：响应里给 `next_cursor`，客户端原样回传。游标由上游决定，不保证可比较。
- `page`：客户端维护页码，从 1 开始递增。
- `single`：一次返回全量，没有游标。当前 `notice` 站点用它。

## 幂等性现状

`records` 表以 `item_key` 为主键，插入时 `ON CONFLICT DO NOTHING`。
`item_key` 由 `(site_id, source_id, price_cents, stock)` 的哈希得到。

这意味着：

- 同样的输入重复采，不会产生新行（幂等）；
- 输入发生内容变化后重复采，会产生新行（不幂等）。

第二点是当前最主要的线上问题。

## 运行记录

每轮采集写一条 `runs`，字段为 `run_id / site_id / started_at / finished_at / status / items_seen / items_wrote`。

- `items_seen`：上游返回的条目数（含重复）
- `items_wrote`：真正插入的行数（冲突忽略的不计）

两者差值持续偏大，通常说明去重键有问题。

## 观察点

| 指标 | 位置 | 正常范围 |
|---|---|---|
| 每站点行数 | `/stats` → `sites[].rows` | 与上游条目数同量级 |
| 重复率 | `/stats` → `sites[].duplicate_ratio` | < 1% |
| 失败轮次 | `/runs` → `status=failed` | 偶发 |
| 请求计数 | `/stats?with_requests=1` | 与失败率一起看 |

`scripts/verify_collector.py` 把前两项写成了断言。

## 采集周期

`Runner` 取所有启用站点里最短的 `interval` 作为统一 tick。
每个 tick 内串行遍历全部站点，单站点跑完才进下一个。
站点越多，实际周期越长——这是单机实现的固有限制。

## 待决策

1. 去重键是否改为 `(site_id, source_id)` + `DO UPDATE`，以及内容更新时 `updated_at` 的口径。
2. 是否需要跨进程的站点级限流与退避。
3. 多实例部署前是否启用 `leases`。
