export PATH := $(HOME)/go/bin:/usr/local/go/bin:$(PATH)
SHELL := /bin/sh

CONFIG ?= configs/sites.yaml

.PHONY: help
help:
	@echo "make up       启动全部依赖（db / redis / mocktarget / collector）"
	@echo "make down     停止并清理容器"
	@echo "make demo     跑一次可复现的采集演示（含故障注入与去重检查）"
	@echo "make test     Go 单元测试"
	@echo "make test-integration  真实 Postgres 集成测试（迁移/历史/并发）"
	@echo "make lint     go vet"
	@echo "make verify   跑 verify_collector.py 断言脚本"
	@echo "make local-mock  仅在本机起 mocktarget（端口 9100）"
	@echo "make local-run   仅在本机跑一次采集（需要本机 db/redis）"

.PHONY: up
up:
	docker compose up --build -d
	@echo "等待 collector 就绪..."
	@sh scripts/wait_for.sh http://localhost:8081/healthz 60

.PHONY: down
down:
	docker compose down -v --remove-orphans

.PHONY: logs
logs:
	docker compose logs -f --tail=100 collector

.PHONY: demo
demo:
	@sh scripts/demo.sh

.PHONY: test
test:
	go test ./... -count=1

# 真实 Postgres 上的迁移 / 历史 / 并发测试。
# 需要一个可建库的账号；每个用例自建独立 database。
.PHONY: test-integration
test-integration:
	AQUEDUCT_TEST_DSN='$(AQUEDUCT_TEST_DSN)' go test ./... -count=1 -race

.PHONY: lint
lint:
	go vet ./...
	@$(NULL)

.PHONY: verify
verify:
	python3 scripts/verify_collector.py --base http://localhost:8081

.PHONY: local-mock
local-mock:
	go run ./cmd/mocktarget -addr :9100

.PHONY: local-run
local-run:
	go run ./cmd/collector -config $(CONFIG) -mode once -site supplier

NULL :=
