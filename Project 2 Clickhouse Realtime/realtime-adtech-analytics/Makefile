SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help init up down restart clean ps logs build test test-java test-python test-sql smoke \
	    ch-client dashboards benchmark rate topics schema

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

init: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env")

up: init ## Build and start the full stack (ClickHouse 2x2, Kafka, Flink, MinIO, monitoring)
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Grafana     http://localhost:3000   (dashboards: Ad-Tech folder)"
	@echo "Flink UI    http://localhost:8081"
	@echo "Kafka UI    http://localhost:8080"
	@echo "ClickHouse  http://localhost:8123/play   (user adtech)"
	@echo "Prometheus  http://localhost:9090"
	@echo "MinIO       http://localhost:9001"

down: ## Stop (keep volumes)
	$(COMPOSE) down

restart: down up ## Restart

clean: ## Stop and delete all data
	$(COMPOSE) down -v --remove-orphans

ps: ## Service status
	$(COMPOSE) ps

logs: ## Tail logs (s=<service>)
	$(COMPOSE) logs -f --tail=100 $(s)

build: ## Build images
	$(COMPOSE) build flink-jobmanager generator

test: test-java test-python test-sql ## All tests

test-java: ## Flink job tests (Java 11 + Maven)
	cd flink-job && mvn -B verify

test-python: ## Generator tests
	cd generator && pip install -q -r requirements-dev.txt && pytest -q

test-sql: ## Validate all ClickHouse SQL with embedded ClickHouse (chdb)
	pip install -q chdb && python scripts/validate_sql.py

smoke: ## End-to-end check of the running stack
	bash scripts/smoke-test.sh

ch-client: ## ClickHouse client on shard 1 / replica 1
	$(COMPOSE) exec ch-s1-r1 bash -c 'clickhouse-client --user adtech --password "$$CH_PASSWORD" -d adtech'

schema: ## Re-apply the ClickHouse schema (idempotent)
	$(COMPOSE) run --rm clickhouse-init

dashboards: ## Regenerate Grafana dashboards from scripts/build_dashboards.py
	python scripts/build_dashboards.py

benchmark: ## Query benchmark (c=<concurrency> n=<iterations>)
	bash scripts/benchmark.sh $(or $(c),4) $(or $(n),200)

rate: ## Change generator rate, e.g. make rate eps=10000 workers=4
	EVENTS_PER_SECOND=$(eps) GENERATOR_WORKERS=$(or $(workers),2) $(COMPOSE) up -d generator

topics: ## Describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
