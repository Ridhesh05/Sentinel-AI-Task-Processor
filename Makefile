.PHONY: help install lint fmt type-check docker-build docker-up docker-down docker-logs clean

help: Makefile
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies
	pip install -r requirements/base.txt
	pip install fastapi uvicorn google-generativeai prometheus-client pydantic pydantic-settings python-dotenv pytest

lint: ## Run ruff linter
	ruff check .

fmt: ## Format code with ruff
	ruff format .
	ruff check --fix .

type-check: ## Run mypy type checker
	mypy core services --ignore-missing-imports || true

docker-build: ## Build all Docker images
	docker compose -f docker-compose.dev.yml build

docker-up: ## Start all services
	docker compose -f docker-compose.dev.yml up -d

docker-down: ## Stop all services
	docker compose -f docker-compose.dev.yml down

docker-logs: ## Show logs from all services
	docker compose -f docker-compose.dev.yml logs -f

docker-logs-api: ## Show API logs
	docker compose -f docker-compose.dev.yml logs -f api

docker-logs-worker: ## Show Worker logs
	docker compose -f docker-compose.dev.yml logs -f worker

docker-logs-monitor: ## Show Monitor logs
	docker compose -f docker-compose.dev.yml logs -f monitor

docker-restart: ## Restart all services
	docker compose -f docker-compose.dev.yml restart

docker-shell-api: ## Shell into API container
	docker compose -f docker-compose.dev.yml exec api /bin/sh

docker-shell-worker: ## Shell into Worker container
	docker compose -f docker-compose.dev.yml exec worker /bin/sh

clean: ## Remove build artifacts and cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	docker compose -f docker-compose.dev.yml down -v 2>/dev/null || true