.PHONY: help install lint fmt type-check up down logs restart ps nuke build env \
       docker-build docker-up docker-down docker-logs docker-restart \
       docker-logs-api docker-logs-worker docker-logs-monitor \
       docker-shell-api docker-shell-worker clean

COMPOSE := docker compose -f docker-compose.dev.yml

# ─────────────────────────────────────────────────────────────────────────────
# Quick shortcuts — the only commands you need to remember
# ─────────────────────────────────────────────────────────────────────────────

help: Makefile ## Show all commands
	@echo ""
	@echo "  \033[1mSentinel AI Task Processor\033[0m"
	@echo "  ─────────────────────────────────────"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

env: ## Create .env from .env.example (if missing)
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "\033[32m✓ Created .env from .env.example — edit it and set GEMINI_API_KEY\033[0m"; \
	else \
		echo "\033[33m⚠ .env already exists, skipping\033[0m"; \
	fi

up: env ## Start all services (builds if needed)
	@echo "\033[1m🚀 Starting Sentinel AI Task Processor...\033[0m"
	$(COMPOSE) up -d --build
	@echo ""
	@echo "\033[32m✓ All services started!\033[0m"
	@echo ""
	@echo "  Frontend:   \033[36mhttp://localhost:3000\033[0m"
	@echo "  API Docs:   \033[36mhttp://localhost:8000/docs\033[0m"
	@echo "  SSE Stream: \033[36mhttp://localhost:9000/events\033[0m"
	@echo "  DB Admin:   \033[36mhttp://localhost:8080\033[0m"
	@echo ""

down: ## Stop all services
	@echo "\033[1m⏹  Stopping services...\033[0m"
	$(COMPOSE) down
	@echo "\033[32m✓ All services stopped\033[0m"

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

restart: ## Restart all services
	$(COMPOSE) restart

ps: ## Show running services and health status
	$(COMPOSE) ps

build: ## Rebuild all Docker images (no cache)
	$(COMPOSE) build --no-cache

nuke: ## Stop everything, remove volumes and images (fresh start)
	@echo "\033[31m⚠ This will destroy all data (DB, Redis). Press Ctrl+C to cancel.\033[0m"
	@sleep 3
	$(COMPOSE) down -v --rmi local
	@echo "\033[32m✓ Clean slate\033[0m"

# ─────────────────────────────────────────────────────────────────────────────
# Per-service logs
# ─────────────────────────────────────────────────────────────────────────────

logs-api: ## Tail API logs only
	$(COMPOSE) logs -f api

logs-worker: ## Tail Worker logs only
	$(COMPOSE) logs -f worker

logs-monitor: ## Tail Monitor logs only
	$(COMPOSE) logs -f monitor

# ─────────────────────────────────────────────────────────────────────────────
# Dev utilities
# ─────────────────────────────────────────────────────────────────────────────

install: ## Install Python dependencies locally
	pip install -r requirements/base.txt
	pip install -r requirements/api.txt
	pip install -r requirements/worker.txt
	pip install -r requirements/monitor.txt
	pip install pytest ruff mypy pyright

lint: ## Run ruff linter
	ruff check .

fmt: ## Format code with ruff
	ruff format .
	ruff check --fix .

type-check: ## Run mypy type checker
	mypy core services --ignore-missing-imports || true

shell-api: ## Shell into API container
	$(COMPOSE) exec api /bin/sh

shell-worker: ## Shell into Worker container
	$(COMPOSE) exec worker /bin/sh

clean: ## Remove build artifacts and cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true

# ─────────────────────────────────────────────────────────────────────────────
# Legacy aliases (kept for backward compat)
# ─────────────────────────────────────────────────────────────────────────────

docker-build: build
docker-up: up
docker-down: down
docker-logs: logs
docker-restart: restart
docker-logs-api: logs-api
docker-logs-worker: logs-worker
docker-logs-monitor: logs-monitor
docker-shell-api: shell-api
docker-shell-worker: shell-worker