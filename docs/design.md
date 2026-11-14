# 当前状态 + 历史版本模型

## 问题

采集结果曾把价格、库存这类**会变化的属性**当作对象身份的一部分
（`Identity()` 对 `site_id|source_id|price_cents|stock` 求哈希，冲突时 `DO
NOTHING`）。结果是：上游一调价，同一个来源对象就变成一条新行，历史事实
与对象身份混在一起，既无法去重，也无法回答「某个时点这个商品卖多少钱」。

同时业务方要求：给定任意历史时点，能还原当时可见的价格与库存。

## 模型

两张表并存：

| 表 | 含义 | 写入方式 |
|---|---|---|
| `records` | **当前状态**，每个 `(site_id, source_id)` 恰好一行 | 原地更新 |
| `record_versions` | **历史版本**，只追加 | 每次真实变化追加一行 |

`records` 保持原有列与读取方式，现有消费者（对账、看板）无需改动。
新增列 `revision / content_hash / observed_at / watermark_at` 均为附加信息。

### 身份与内容

- **身份** `Identity()` 只由 `(site_id, source_id)` 决定。价格、库存、标题
  都不参与。调价不会产生新对象。
- **内容指纹** `ContentHash()` 覆盖标题、价格、库存与 extras，不包含任何
  时间字段。内容相同则重复采集不产生新版本。
- **兼容性**：迁移前已存在的对象保留其原始 `item_key`。迁移后新对象使用新
  算法计算的 `item_key`。「一个来源对象只有一个当前状态」由
  `UNIQUE (site_id, source_id)` 保证，而不是靠 `item_key` 的取值。

### 版本与排序

每个版本有：

- `revision`：在单个 `(site_id, source_id)` 内严格递增，**稳定顺序的唯一依据**。
  不跨对象、不可重排。
- `valid_from` / `valid_to`：业务时间上的半开区间 `[valid_from, valid_to)`。
  `valid_to` 为空表示当前仍然有效。
- `observed_at`：系统时间，记录我们**何时**第一次观测到该事实。
- `from_legacy`：该版本由迁移从旧重复行还原，而非在线采集写入。

版本链的区间首尾相接：`v[i].valid_to == v[i+1].valid_from`。

## 更新冲突

上游可能给出**倒退**或**缺失**的业务时间。规则（`model.DecideUpdate`）：

| 当前版本时间 | 观测时间 | 结果 |
|---|---|---|
| 已知 | 更晚 | 接受，产生新版本 |
| 已知 | 相同 | 拒绝（内容不同时按确定规则裁决） |
| 已知 | 更早 | 拒绝 |
| 缺失 | 已知 | 接受（证据更强） |
| 已知 | 缺失 | 拒绝（不能用未知时间覆盖已知事实） |
| 缺失 | 缺失 | 接受，按观测顺序排定 |

判定基线是「当前版本业务时间」与「水位 `watermark_at`」的较大者。水位是该
来源**已确认的最大上游业务时间**，只会被上游时间抬高，不会被本机时间抬高。
因此即使内容没变，上游再次给出更新的时间也会抬高水位，之后携带更早时间的
观测都无法覆盖。

被拒绝的观测计入 `runs.items_stale`，不产生版本、不改当前行。
内容与当前版本一致的观测计入 `runs.items_unchanged`，只刷新 `observed_at`。

### 同批次乱序

一个批次里同一来源出现多次时，先折叠成一次观测：有业务时间优先、时间更晚
优先、内容哈希作最终 tie-break。因此**结果与到达顺序无关**。

### 同一时间戳

上游可能给两个不同内容但相同的 `updated_at`。此时后到的观测被拒绝
（`DecideUpdate` 视为冲突）。零长度区间允许存在，as-of 查询用
`revision` 作为同一时间戳下的仲裁顺序。

## 历史查询

- 单对象 as-of：`valid_from <= at` 的版本中，`valid_from` 最大、
  其次 `revision` 最大者。
- 站点快照 as-of：对每个 `source_id` 取同样规则的最新版本
  （`DISTINCT ON (source_id) ... ORDER BY source_id, valid_from DESC, revision DESC`）。

**成本**：单对象查询走 `(site_id, source_id, valid_from DESC, revision DESC)`
索引，是索引范围扫描。站点快照需扫描该站点 `valid_from <= at` 的全部版本
再按来源归并，成本与该站点历史变化总量成正比；大站点请用 `limit/offset`。

## 迁移

`Migrate()` 幂等、可重复执行、可中断恢复：

1. `CREATE TABLE IF NOT EXISTS` + `ADD COLUMN IF NOT EXISTS` 扩展 schema。
2. 按站点、按 `source_id` 分批（每批一个事务，默认 200 个对象）回填历史。
   每个事务内同时写历史版本、清理 `records` 重复行、记录进度
   `data_migrations(site_id, last_source_id, phase)`。崩溃后从
   `last_source_id` 续跑。
3. 用 `pg_advisory_xact_lock` 避免多实例同时迁移同一站点。
4. 全部回填完成后建立 `UNIQUE (site_id, source_id)`。若仍有重复则失败，
   而不是静默删除数据。

**迁移不依赖全量重采**：旧的重复行按 `(updated_at, item_key)` 的确定顺序
还原成版本链，最新一版留在 `records`，其余移入 `record_versions`。缺失时间
的旧行按已知时间的邻行对齐，查询时由 `revision` 裁决。

**新旧共存**：迁移期间 `records` 始终可读，行为与迁移前一致；
`/stats` 的 `migration.temporal_reads_ready` 指示历史视图是否就绪。
若迁移尚未开始而旧版本进程仍在写重复行，下一轮 `Migrate()` 会把它们并入
版本链，且**不会让旧观测覆盖较新的业务事实**。

## 写入并发

- 单站点一个事务，已存在的当前行用 `SELECT ... FOR UPDATE` 串行化。
- 新对象的并发插入由唯一约束兜底，冲突后退化为对已存在行的更新判定。
- 同批次内重复折叠，保证顺序无关。

## 接口

```
GET /items/{site}/{source_id}                 当前状态
GET /items/{site}/{source_id}/history         版本链（revision 倒序）
GET /items/{site}/{source_id}/as-of?at=RFC3339  指定时点可见版本
GET /snapshot/{site}?at=RFC3339&limit=&offset=  站点全量快照
GET /stats                                    统计、重复率、历史版本数、迁移进度
```

原有 `/stats`、`/runs`、`POST /collect/{site}` 字段保持兼容，仅新增字段。
