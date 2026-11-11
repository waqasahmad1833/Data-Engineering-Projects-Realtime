SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help init up down restart clean ps logs build test test-java test-python smoke ch-client topics dlq

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

init: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

up: init ## Build images and start the whole platform
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Kafka UI   -> http://localhost:8080"
	@echo "Flink UI   -> http://localhost:8081"
	@echo "ClickHouse -> http://localhost:8123/play"
	@echo "Grafana    -> http://localhost:3000"

down: ## Stop all services (keeps data volumes)
	$(COMPOSE) down

restart: down up ## Restart the platform

clean: ## Stop everything and delete all data volumes
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs (use s=<service> for one service)
	$(COMPOSE) logs -f --tail=100 $(s)

build: ## Build the Flink job and producer images
	$(COMPOSE) build flink-jobmanager producer

test: test-java test-python ## Run all unit tests

test-java: ## Run Flink job unit tests (needs Java 11 + Maven)
	cd flink-job && mvn -B verify

test-python: ## Run producer unit tests (needs Python 3.10+)
	cd producer && pip install -q -r requirements-dev.txt && pytest -q

smoke: ## End-to-end check: data is flowing into ClickHouse
	bash scripts/smoke-test.sh

ch-client: ## Open an interactive ClickHouse client
	$(COMPOSE) exec clickhouse bash -c 'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" -d analytics'

topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

dlq: ## Peek at the last dead-letter messages
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clickstream.dlq --from-beginning --max-messages 10 --timeout-ms 10000
