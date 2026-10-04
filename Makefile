COMPOSE ?= docker compose
.DEFAULT_GOAL := help
.PHONY: help init up up-llm up-all down reset ps logs sim-stop sim-start peek truth decisions alerts net-alerts analytics analytics-once eval graph-shell test

help: ## Show targets
	@awk 'BEGIN{FS=":.*## "} /^[a-z-]+:.*## /{printf "  %-12s %s\n",$$1,$$2}' $(MAKEFILE_LIST)

init: ## Create .env from .env.example (if missing)
	@test -f .env || cp .env.example .env && echo ".env ready"

up: init ## Start core stack (Neo4j, Redpanda, Redis, Postgres, simulator)
	$(COMPOSE) up -d --build

up-llm: init ## Core stack + Ollama (pulls the model on first run)
	$(COMPOSE) --profile llm up -d --build

up-all: init ## Everything incl. Redpanda Console
	$(COMPOSE) --profile llm --profile tools up -d --build

down: ## Stop everything (keep data)
	$(COMPOSE) --profile llm --profile tools down

reset: ## Stop everything and DELETE all volumes
	$(COMPOSE) --profile llm --profile tools down -v

ps: ## Container status
	$(COMPOSE) --profile llm --profile tools ps

logs: ## Tail logs (make logs s=simulator)
	$(COMPOSE) logs -f --tail=100 $(s)

sim-stop: ## Pause the transaction simulator
	$(COMPOSE) stop simulator

sim-start: ## Resume the transaction simulator
	$(COMPOSE) start simulator

peek: ## Show 5 raw transactions from Redpanda
	$(COMPOSE) exec redpanda rpk topic consume transactions.raw -n 5

truth: ## Show 5 ground-truth labels
	$(COMPOSE) exec redpanda rpk topic consume transactions.truth -n 5

decisions: ## Show 5 hot-path decisions
	$(COMPOSE) exec redpanda rpk topic consume transactions.decisions -n 5

alerts: ## Show 5 alerts (REVIEW / BLOCK)
	$(COMPOSE) exec redpanda rpk topic consume alerts -n 5

net-alerts: ## Show 5 network (cluster) alerts from the cold path
	$(COMPOSE) exec redpanda rpk topic consume alerts.network -n 5

analytics: ## Tail the graph-analytics cycle logs
	$(COMPOSE) logs -f --tail=50 graph-analytics

analytics-once: ## Run a single analytics cycle now and print its summary
	$(COMPOSE) run --rm graph-analytics python -m graph_analytics.main --once

eval: ## Detection quality + latency report (make eval ARGS="--idle 15")
	$(COMPOSE) run --rm stream-processor python -m stream_processor.evaluator $(ARGS)

graph-shell: ## Open cypher-shell
	$(COMPOSE) exec neo4j cypher-shell -u neo4j -p $${NEO4J_PASSWORD:-aml_password_123}

test: ## Run unit tests (pip install -r requirements-dev.txt)
	python -m pytest -q
