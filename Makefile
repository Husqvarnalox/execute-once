# Local developer entry points. Everything here also runs in CI (.github/workflows/ci.yml).

COMPOSE ?= docker compose
MVN     ?= ./mvnw -q -B

.PHONY: help env up down logs ps build lint test test-gateway test-java test-bench e2e bench clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example if missing
	@test -f .env || cp .env.example .env

up: ## Build and start the full stack, wait for health checks
	$(COMPOSE) up -d --build --wait

down: ## Stop the stack and drop volumes
	$(COMPOSE) down -v --remove-orphans

logs: ## Follow service logs
	$(COMPOSE) logs -f gateway relay consumer

ps: ## Show service status
	$(COMPOSE) ps

build: ## Build all images without starting them
	$(COMPOSE) build

lint: ## Lint and format-check every component
	cd services/gateway-node && npm ci --no-audit --no-fund && npm run lint && npm run format:check
	cd services && $(MVN) spotless:check
	cd scripts/python-bench && ruff check . && ruff format --check .

test: test-gateway test-java ## Run unit and integration tests for all services (needs Docker)

test-gateway: ## Gateway unit + integration tests
	cd services/gateway-node && npm ci --no-audit --no-fund && npm run test:all

test-java: ## Relay and consumer unit + integration tests
	cd services && $(MVN) verify

test-bench: ## Bench static checks
	cd scripts/python-bench && ruff check . && python3 -m py_compile run.py

e2e: ## Full-stack end-to-end run: dup30 profile with invariant verification
	$(COMPOSE) up -d --build --wait
	mkdir -p e2e-out && chmod 777 e2e-out
	$(COMPOSE) --profile bench run --rm -v "$$PWD/e2e-out:/out" bench --profile dup30 --total 500 --report /out/dup30.json
	$(COMPOSE) down -v --remove-orphans

bench: ## Run a load profile against a running stack: make bench PROFILE=normal TOTAL=1000
	$(COMPOSE) --profile bench run --rm bench --profile $(or $(PROFILE),normal) --total $(or $(TOTAL),500)

clean: down ## Remove build artefacts
	rm -rf services/*/target services/gateway-node/node_modules
