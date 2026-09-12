.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down restart build logs ps seed seed-loop metabase test test-unit test-integration lint validate clean smoke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Build images and start the whole platform
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow   http://localhost:8080  (airflow / airflow)"
	@echo "  MinIO     http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Metabase  http://localhost:3000"
	@echo ""
	@echo "  Next:  make metabase   then   make seed"

down: ## Stop the platform (volumes are kept)
	$(COMPOSE) down

clean: ## Stop the platform and delete all data volumes
	$(COMPOSE) down -v --remove-orphans

restart: down up ## Restart everything

build: ## Build the custom images only
	$(COMPOSE) build

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

seed: ## Generate one batch of synthetic sales data into MinIO
	$(COMPOSE) run --rm data-generator --rows 2000

seed-loop: ## Keep generating a batch every 5 minutes in the background
	$(COMPOSE) --profile generator up -d data-generator

metabase: ## Create the Metabase admin user and connect the analytics database
	$(COMPOSE) --profile setup run --rm metabase-init

lint: ## Lint Python and validate the compose file
	ruff check .
	ruff format --check .
	$(COMPOSE) config --quiet

validate: lint ## Alias for lint

test-unit: ## Run fast unit tests (no Docker required)
	pytest tests/unit -v

test-integration: ## Run end-to-end data flow validation against the running stack
	pytest tests/integration -v

test: test-unit test-integration ## Run the full suite

smoke: ## One command: start, seed, and validate the whole flow
	$(MAKE) up
	$(MAKE) metabase
	$(MAKE) seed
	$(MAKE) test-integration
