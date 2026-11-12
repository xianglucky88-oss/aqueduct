# aqueduct

供应商目录 / 商品价格 / 库存快照 / 公告列表的周期性增量采集服务。

当前版本是**单机 cron 采集器**：4 个站点 adapter 串行跑，落 PostgreSQL，Redis 只做请求计数。
已上线一年，下游对账、选品与运营看板都依赖它。

## 快速开始

```bash
make up        # 启动 db / redis / mocktarget / collector
make demo      # 跑一遍固定演示序列（含故障注入）
make verify    # 跑一致性断言脚本
make down      # 清理
```

`mocktarget` 是仓库自带的**上游模拟服务**，用确定性种子生成数据，
支持 429 / 500 / 超时注入与「内容改写」操作，因此所有实验都可以离线复现。
生产环境它指向真实站点，这里指向本地的 `mocktarget:9100`。

## 结构

```
cmd/collector        采集进程入口（-mode all|runner|api|once）
cmd/mocktarget       上游模拟服务（仅开发/测试用）
internal/config      站点配置加载
internal/fetcher     单站点 HTTP 客户端与分页逻辑
internal/collector   单站点采集流程 + 周期调度器
internal/store       PostgreSQL 读写与建表
internal/metrics     Redis 请求计数
internal/api         管理接口：/healthz /stats /runs /collect/{site}
configs              站点配置（本地与容器两套）
scripts              演示与验证脚本
docs                 设计说明与已知问题
```

## 接口

```
GET  /healthz
GET  /stats?with_requests=1
GET  /runs?limit=20
POST /collect/{site}
```

`/stats` 返回每个站点的行数、去重业务主键数与 `duplicate_ratio`，
这是判断采集链路是否正常的主要依据。

## 配置

站点配置见 `configs/sites.yaml`。每个站点有 `mode`（cursor / page / single）、
`page_size`、`interval`（采集周期）与 `rps`（节流参数）。

数据库与 Redis 连接可由环境变量覆盖：`DATABASE_URL`、`REDIS_URL`、`WORKER_ID`。

## 测试

```bash
make test    # go test ./...
make lint    # go vet ./...
```
