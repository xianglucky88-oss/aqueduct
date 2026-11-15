# aqueduct 生产运行方案

本文给出采集服务以多副本进入 Kubernetes 生产环境的运行语义、部署方式、
资源估算与故障处置。代码中的实现点用 `文件:行` 标注。

## 1. 设计结论

| 关注点 | 方案 |
|---|---|
| 任务归属 | 每个站点一行数据库租约，副本竞争获取；无主节点 |
| 防僵尸写入 | 租约带单调递增的 `fence` 令牌，每次写事务内校验 |
| 数据完整性 | 以「页」为原子单位：记录、游标、计数在同一事务提交 |
| 终止 | SIGTERM → 立即 not-ready + 停止领活 → 宽限期内收尾 → 释放租约 |
| 崩溃接管 | 租约 TTL 到期后其他副本接管；遗留 running 轮次标记 abandoned |
| 探针 | startupz / livez / readyz 三态分离，依赖故障不影响存活 |
| 重采控制 | 失败指数退避 + 连续失败计数 + 单轮页数上限 + 游标只前进不回退 |

## 2. 任务协调模型

没有主从。每个副本都运行同一个调度器（`internal/collector/collector.go:244`），
各自轮询数据库中「已到期且无有效租约」的站点（`internal/store/store.go` 的
`DueSites`），再用一条带条件的 UPSERT 抢占租约（`AcquireLease`）。
抢占成功时 `fence` 自增，并返回给持有者。

- **为什么用数据库而不是 Redis**：租约、围栏校验、数据写入必须同源，
  否则 Redis 与 PostgreSQL 之间的时序差会产生「租约有效但写入越界」。
  把租约放在业务数据库里，围栏校验可以和 `INSERT ... ON CONFLICT` 一起
  在同一个事务中完成。
- **Redlock 类方案被排除**：它无法为下游写入提供围栏，进程暂停
  （GC、节点卡顿）后仍可能以旧租约写入。
- **Redis 的角色**：仅请求计数，属可降级依赖。

### 围栏如何生效

`CommitPage`（`internal/store/store.go:416`）先对 `leases` 行执行
`SELECT ... FOR UPDATE`，校验 `worker_id`、`fence` 均匹配且未过期；
任一不符返回 `ErrLeaseLost`，整个事务回滚。

因此，一个在 GC 停顿或网络分区后「复活」的旧副本，即使仍以为自己是
持有者，也无法写入任何一页。它会在下一次心跳（`internal/collector/collector.go:121`）
发现续租失败并自行中止。

### 为什么不会遗漏

- 站点在租约获取时被推到 `now()+ttl`（`AcquireLease`），避免所有副本
  每秒重复抢同一个站点；终止或接管时，`FinishSite` 把 `next_run_at`
  设为 `now()`，让其他副本立刻接手（`internal/store/store.go:491`）。
- 游标只在页提交时前进，且末页把游标置空表示「本轮完成」。下一轮从空
  游标开始重新扫描，因此即使上一轮中途放弃，剩余页也一定会被扫到。
- `runs` 中遗留的 `running` 行在下一次合法接管时被标记 `abandoned`
  （`BeginSiteRun`），不会永久悬挂。

### 为什么不会无限重采

- 内容变化不再产生新行：`item_key = hash(site_id|source_id)`
  （`internal/model/model.go:38`），写入用 `ON CONFLICT (site_id, source_id) DO UPDATE`。
- 每轮有页数上限 `max_pages`，触顶按失败处理并保留检查点。
- 失败按 `5s × 2^n` 退避，上限为 `max(2m, 4×interval)`，并尊重上游
  `Retry-After`；连续失败只增加退避，不改变游标。
- 游标被上游拒绝（400/410）时，每轮最多重置一次，随后仍失败则按普通
  失败退避，不会形成紧密重试环。

## 3. 生命周期与探针

实现：`internal/health/health.go`，路由见 `internal/api/api.go`。

```
startupz  启动完成（迁移、调度器就绪）→ 之后才启用 livez/readyz
livez     进程可响应。不检查数据库/Redis。依赖故障绝不触发重启
readyz    可接工作负载：未 draining 且数据库可达。失败 → 从 Endpoints 摘除
```

| 场景 | startupz | livez | readyz | 行为 |
|---|---|---|---|---|
| 正常 | 200 | 200 | 200 | 接任务 |
| 数据库宕机 | 200 | 200 | 503 | 不接新任务，进程不重启；恢复后自动 200 |
| Redis 宕机 | 200 | 200 | 200（`degraded`） | 继续采集，仅计数丢失 |
| 收到 SIGTERM | 200 | 200 | 503（`draining`） | 停止领活，在途收尾 |
| 启动中 | 503 | 200 | 503 | 存活探针不生效，避免误杀 |

就绪检查结果按 `readiness_cache_ttl`（默认 2s）缓存，避免高频探针压垮数据库。

## 4. 终止时序

```
t=0     Pod 进入 Terminating；kubelet 发 SIGTERM
t=0     readyz → 503；调度器停止领取新站点
t≈5s    Endpoints 摘除完成，新工作不再路由到本实例
t=0~G   在途站点继续跑；每提交一页就落一次检查点
t=G     仍未完成则取消并写 abandoned，立即释放租约
t=G+    关闭 HTTP，进程退出
```

- `ShutdownGrace` 默认 20s，必须 **小于** `LeaseTTL`（45s），
  这样退出前的释放动作仍持有有效租约（配置校验强制此约束）。
- Kubernetes `terminationGracePeriodSeconds` 默认 45s，须大于
  `ShutdownGrace + 就绪摘除时间`。
- 第二个 SIGTERM 恢复默认处置，可直接强杀（`cmd/collector/main.go:176`）。

## 5. 部署

```bash
kubectl apply -f deploy/k8s.yaml
```

清单包含 Deployment / Service / ConfigMap / Secret 占位 / PDB。
关键配置：

- `replicas: 3`，`maxUnavailable: 0`，`maxSurge: 1`。
- `podAntiAffinity` 尽量分散到不同节点。
- `startupProbe` 指向 `/startupz`，`failureThreshold: 24` 容忍 120s 启动。
- `livenessProbe` 指向 `/livez`，不检查依赖。
- `readinessProbe` 指向 `/readyz`。
- `securityContext` 非 root（UID 10001），镜像与之匹配。

本地验证多副本：

```bash
make replicas   # docker compose --scale collector=3
make rollout    # 逐个重启并等待 readyz
make drain      # 触发优雅终止
```

## 6. 副本策略

| 站点数 | 建议副本 | 说明 |
|---|---|---|
| 1–10 | 3 | 允许一台节点维护/一次滚动更新时仍有 2 个可用 |
| 10–50 | 3–5 | 并发由 `MAX_CONCURRENT_SITES` 控制，不是副本越多越快 |
| >50 | 按上游配额分区 | 单站点的 RPS 与周期由配置决定，副本不能突破 |

- 同一站点同一时刻只有一个合法执行者，增加副本只提升**故障覆盖**与
  **并发度**，不会提升单站点吞吐。
- `MAX_CONCURRENT_SITES=2` 时，单副本最多同时持有 2 个站点租约；
  3 副本可覆盖 6 个站点的并行采集。
- 站点数远大于 `副本数 × 并发` 时，其余站点按到期时间排队，不会丢失。
- PDB `minAvailable: 1` 保证节点驱逐不会让整个采集面离线。

## 7. 资源估算

以默认配置（4 站点、`page_size` 40–100、每站点 30–120s 一轮）为例：

| 资源 | 每副本请求 | 每副本上限 | 说明 |
|---|---|---|---|
| CPU | 100m | 500m | 主要是 JSON 解析与批量 SQL |
| 内存 | 96Mi | 192Mi | 单页 ≤500 条；Go GC 余量 |
| 数据库连接 | 8 | — | `DB_MAX_OPEN_CONNS` |

容量换算：

- 数据库连接：`副本数 × 8`。3 副本 = 24 条，需保证 PostgreSQL
  `max_connections` ≥ 32（含运维余量）。
- 写入量：单页 500 条 × 约 200B ≈ 100KB/事务。默认 `max_pages=200`
  时单轮上限约 20MB，通常远低于此。
- 请求数：单站点 `rps × 轮次耗时`；退避与 `Retry-After` 会自动压低
  故障期间的压力。

估算公式（单站点）：

```
每轮请求数 ≈ min(max_pages, ceil(条目数 / page_size))
稳态 QPS ≈ 每轮请求数 / interval
```

## 8. 依赖故障时的流量处置

**数据库不可用**

- `readyz` 返回 503 → 所有副本被摘除，Service 不再转来管理流量。
- 调度器无法获取租约，自然停止采集；`livez` 保持 200，不触发重启。
- 恢复后下一次探针即转 200，无需人工干预。
- 管理接口（`/stats`、`/runs`、`/sites`）返回 503，调用方应视为可重试。

**Redis 不可用**

- 采集不受影响。`readyz` 返回 200，body 为 `"status":"degraded"`。
- 请求计数丢失（`/stats?with_requests=1` 归零），不影响数据一致性。
- 启动时 Redis 连接失败只记录告警，不阻断进程（`cmd/collector/main.go`）。

**上游站点故障**

- 429 / 5xx / 超时：本轮失败，按退避排期，游标停在检查点。
- 400 / 410：视为游标失效，重置一次后重试；仍失败则退避。
- 其他 4xx：按不可重试失败处理并退避，通常意味着配置或凭据问题。

## 9. 滚动更新、缩容与重启

| 操作 | 机制 | 保证 |
|---|---|---|
| 滚动更新 | `maxUnavailable=0` + readiness | 新副本 Ready 后才终止旧副本 |
| 缩容 | 被删 Pod 先 draining | 剩余副本承接租约，未完成页由检查点续采 |
| 节点驱逐 | PDB + draining | 至少 1 副本存活；站点不丢 |
| 进程崩溃 | 租约 TTL 到期 | 其他副本接管，遗留运行标记 abandoned |
| 强制删除 | 同上 | 无需人工清理数据库状态 |

需要人工关注的只有一种情况：上游持续返回错误导致所有副本同时退避。
此时 `/sites` 的 `consecutive_failures` 与 `last_error` 会持续增长，应据
此排查上游而不是重启进程。

## 10. 运维接口

```bash
curl /startupz /livez /readyz        # 探针
curl /stats                          # 行数、去重率、游标
curl /runs?limit=50                  # 运行历史（含 worker_id / last_error）
curl /sites                          # 租约归属 + 调度状态
POST /collect/{site}                 # 手动触发（需竞争租约；他人持有时 409）
POST /drain                          # 进入终止流程（不退出进程）
```

## 11. 一次性数据迁移

旧版本的 `item_key` 含价格与库存，内容变化会生成新行。启动时 `Migrate`
在 advisory lock 保护下执行一次 `stable_record_keys`
（`internal/store/store.go:171`）：

1. 按 `(site_id, source_id)` 保留 `updated_at` 最新的一行，删除其余；
2. 重算 `item_key`；
3. 建立 `(site_id, source_id)` 唯一索引；
4. 在 `schema_steps` 记录已执行。

该步骤幂等，多副本同时启动只会有一个执行。`records` 可由上游重采，
保留最新行是有损但安全的取舍——如需审计历史，应在此步骤之前做备份。
