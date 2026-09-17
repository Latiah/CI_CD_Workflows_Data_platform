.DEFAULT_GOAL := help
COMPOSE := docker compose

# Overridable so CI can pin the interpreter it set up: `make test PY="python"`.
PY ?= python

# The services the end-to-end flow needs. The triggerer is excluded on purpose
# (nothing defers) and the generator is on-demand.
CORE_SERVICES := postgres minio airflow-apiserver airflow-scheduler airflow-dag-processor metabase

# Host-facing endpoints, derived from .env so the targets follow a remapped
# stack. Without this, `make metabase` and `make e2e` always knocked on the
# default ports and failed with "connection refused" whenever .env published
# the services somewhere else. CI never saw it because CI uses the defaults.
-include .env
METABASE_URL   ?= http://localhost:$(or $(METABASE_PORT),3000)
AIRFLOW_URL    ?= http://localhost:$(or $(AIRFLOW_PORT),8080)
MINIO_ENDPOINT ?= http://localhost:$(or $(MINIO_API_PORT),9000)
POSTGRES_PORT  ?= 5432
export METABASE_URL AIRFLOW_URL MINIO_ENDPOINT POSTGRES_PORT
export MINIO_BUCKET MINIO_PREFIX METABASE_ADMIN_EMAIL METABASE_ADMIN_PASSWORD
export AIRFLOW_ADMIN_USER AIRFLOW_ADMIN_PASSWORD POSTGRES_USER POSTGRES_PASSWORD ANALYTICS_DB

.PHONY: help env up down restart build logs ps seed seed-loop metabase \
        lint validate test e2e clean smoke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from the template and generate fresh secrets
	@test -f .env || cp .env.example .env
	./scripts/gen_secrets.sh

# ---------------------------------------------------------------- checks
lint: ## Ruff + hadolint (containerised, so CI and local agree)
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	@for f in docker/airflow/Dockerfile docker/data_generator/Dockerfile; do \
		echo "hadolint $$f"; \
		docker run --rm -i hadolint/hadolint:latest \
			hadolint --failure-threshold error - < "$$f" || exit 1; \
	done

validate: ## Validate the compose file and the SQL bootstrap
	$(COMPOSE) config --quiet
	@$(COMPOSE) config --services
	@for f in config/postgres/init/*.sql; do \
		test -s "$$f" || { echo "Empty SQL file: $$f"; exit 1; }; \
		echo "ok: $$f"; \
	done

test: ## Unit tests (fast, no Docker)
	$(PY) -m pytest tests/unit -v

e2e: ## End-to-end data flow validation against the running stack
	$(PY) -m pytest tests/integration -v

# ---------------------------------------------------------------- platform
up: ## Build and start the platform, waiting until every service is healthy
	$(COMPOSE) up -d --build --wait --wait-timeout 600 $(CORE_SERVICES)
	@echo ""
	@echo "  Airflow   $(AIRFLOW_URL)"
	@echo "  MinIO     http://localhost:$(or $(MINIO_CONSOLE_PORT),9001)"
	@echo "  Metabase  $(METABASE_URL)"
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
	$(COMPOSE) ps --all

logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

metabase: ## Create the Metabase admin user and connect the analytics database
	$(PY) -m scripts.provision_metabase

seed: ## Generate one batch of synthetic sales data into MinIO
	$(COMPOSE) run --rm data-generator --rows 2000

seed-loop: ## Keep generating a batch every 5 minutes in the background
	$(COMPOSE) --profile generator up -d data-generator

smoke: ## One command: start, provision, seed, and validate the whole flow
	$(MAKE) up
	$(MAKE) metabase
	$(MAKE) seed
	$(MAKE) e2e
