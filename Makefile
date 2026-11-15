export PATH := $(HOME)/go/bin:/usr/local/go/bin:$(PATH)
SHELL := /bin/sh

CONFIG ?= configs/sites.yaml
API ?= http://localhost:8081
COMPOSE ?= docker compose

.PHONY: help
help:
	@echo "make up         启动全部依赖（db / redis / mocktarget / collector）"
	@echo "make down       停止并清理容器"
	@echo "make demo       跑一次可复现的采集演示（含故障注入与去重检查）"
	@echo "make test       Go 单元测试（无数据库时自动跳过集成用例）"
	@echo "make test-int   需要真实 PostgreSQL 的存储层集成测试"
	@echo "make lint       go vet"
	@echo "make verify     跑 verify_collector.py 断言脚本"
	@echo "make replicas   docker compose --scale collector=3（本机模拟多副本）"
	@echo "make rollout    滚动重启所有副本并等待就绪"
	@echo "make drain      对本机副本触发优雅终止（readyz 转 503）"
	@echo "make local-mock 仅在本机起 mocktarget（端口 9100）"
	@echo "make local-run  仅在本机跑一次采集（需要本机 db/redis）"

.PHONY: up
up:
	$(COMPOSE) up --build -d
	@echo "等待 collector 就绪..."
	@sh scripts/wait_for.sh $(API)/readyz 90

.PHONY: down
down:
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs:
	$(COMPOSE) logs -f --tail=100 collector

.PHONY: demo
demo:
	@sh scripts/demo.sh

.PHONY: test
test:
	go test ./... -count=1

# 需要外部数据库。示例：
#   make test-int AQUEDUCT_TEST_DATABASE_URL=postgres://...
.PHONY: test-int
test-int:
	@test -n "$(AQUEDUCT_TEST_DATABASE_URL)" || (echo "set AQUEDUCT_TEST_DATABASE_URL" && exit 1)
	go test ./internal/store/ -count=1 -v

.PHONY: lint
lint:
	go vet ./...

.PHONY: verify
verify:
	python3 scripts/verify_collector.py --base $(API)

.PHONY: replicas
replicas:
	$(COMPOSE) up --build -d --scale collector=3

# 模拟 Kubernetes 滚动更新的最小步骤：逐副本重启，每次等待就绪。
.PHONY: rollout
rollout:
	@for id in $$($(COMPOSE) ps -q collector); do \
		echo "restarting $$id"; \
		docker restart --time 45 $$id; \
		sh scripts/wait_for.sh $(API)/readyz 90; \
	done

.PHONY: drain
drain:
	curl -fsS -X POST $(API)/drain; echo
	@echo "readyz:"; curl -s -o /dev/null -w '%{http_code}\n' $(API)/readyz

.PHONY: local-mock
local-mock:
	go run ./cmd/mocktarget -addr :9100

.PHONY: local-run
local-run:
	go run ./cmd/collector -config $(CONFIG) -mode once -site supplier
