SHELL := /bin/bash
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help build up up-obs down logs smoke happy test-all chaos demo clean

help:           ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

build:          ## Build all images (coordinator + node + ingestor)
	$(COMPOSE) build

up:             ## Start coordinator + 4 nodes (detached)
	$(COMPOSE) up -d coordinator node0 node1 node2 node3

up-obs:         ## Start full stack + Prometheus + Grafana
	$(COMPOSE) up -d coordinator node0 node1 node2 node3 prometheus grafana
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000"

down:           ## Stop and remove containers + volumes + dlq
	$(COMPOSE) down -v
	rm -rf dlq/

logs:           ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

smoke:          ## D1 smoke test — /health gate
	bash scripts/test_01_smoke.sh

happy:          ## D2 happy path — 10K events, ALL_DONE
	bash scripts/test_02_happy.sh

test-all:       ## Run all 10 acceptance tests (smoke + happy + 8 chaos)
	bash scripts/run_all.sh

chaos:          ## Run only the 8 chaos tests (03-10)
	@for t in 03_kill_node 04_revive_node 05_slow_node 06_partition 07_coordinator_down 08_double_report 09_stale_run 10_data_loss; do \
		bash scripts/test_$$t.sh || exit 1; \
	done

demo:           ## 10-minute live demo (§8.8 + §9.8)
	bash scripts/demo_live.sh

clean:          ## Full reset: down + remove images + dlq
	$(COMPOSE) down -v --rmi local 2>/dev/null || true
	rm -rf dlq/ deploy/grafana/data/
