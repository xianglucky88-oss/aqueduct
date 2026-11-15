# aqueduct

供应商目录 / 商品价格 / 库存快照 / 公告列表的周期性增量采集服务。

当前版本以**多副本**方式运行：所有副本对等，通过数据库中的站点租约竞争
工作；进程可在任意时刻终止，未完成的工作由其他副本从检查点接管。
生产运行语义见 [docs/production.md](docs/production.md)。

## 快速开始

```bash
make up        # 启动 db / redis / mocktarget / collector，等待 readyz
make demo      # 固定演示序列：去重、内容改写、429 退避、探针
make verify    # 一致性断言
make replicas  # 本机起 3 个副本，观察租约归属
make rollout   # 逐个重启副本并等待就绪
make down
```

`mocktarget` 是仓库自带的**上游模拟服务**，用确定性种子生成数据，
支持 429 / 500 / 超时注入与「内容改写」操作，因此所有实验都可以离线复现。

## 结构

```
cmd/collector        采集进程入口（-mode all|runner|api|once）
cmd/mocktarget       上游模拟服务（仅开发/测试用）
internal/config      配置加载、默认值与校验
internal/fetcher     单站点 HTTP 客户端、错误分类与 Retry-After
internal/collector   单站点采集流程（逐页提交）+ 租约调度器
internal/store       PostgreSQL 读写、租约围栏、检查点与调度状态
internal/health      启动/就绪/存活三态与依赖检查
internal/metrics     Redis 请求计数（可降级依赖）
internal/api         管理接口与探针
deploy/k8s.yaml      Kubernetes 清单
configs              站点配置（本地与容器两套）
scripts              演示与验证脚本
docs                 生产运行方案
```

## 接口

```
GET  /startupz              初始化完成
GET  /livez                 进程存活（不检查依赖）
GET  /readyz                可接收工作负载（兼作 /healthz）
GET  /stats?with_requests=1 落库统计、重复率
GET  /runs?limit=20         最近运行记录
GET  /sites                 租约归属与调度状态
POST /collect/{site}        手动触发一次采集
POST /drain                 进入终止流程
```

## 配置

站点配置见 `configs/sites.yaml`。每个站点有 `mode`（cursor / page / single）、
`page_size`、`interval`（采集周期）与 `rps`（节流参数）。

运行参数（租约、终止、并发、连接池）可在 YAML 中设置，也可用环境变量
覆盖：`DATABASE_URL`、`REDIS_URL`、`WORKER_ID`/`POD_NAME`、`LEASE_TTL`、
`LEASE_RENEW_INTERVAL`、`SHUTDOWN_GRACE`、`MAX_CONCURRENT_SITES`、
`DB_MAX_OPEN_CONNS` 等。完整列表见 `.env.example`。

配置在启动时校验，不合法直接退出——避免 Pod 反复重启却始终不 Ready。

## 测试

```bash
make test       # 单元测试（无数据库时自动跳过集成用例）
make test-int   # 需要 AQUEDUCT_TEST_DATABASE_URL 的租约/检查点集成测试
make lint       # go vet
```
