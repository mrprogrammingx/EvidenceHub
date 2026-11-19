SHELL := /bin/bash
.DEFAULT_GOAL := help

UV := uv
CORE_SERVICES := mysql-catalog mysql-shard-1 mysql-shard-2 mysql-shard-3 mysql-shard-4 redis

.PHONY: help up down down-clean ps migrate-legacy seed seed-full lint fmt typecheck test check demo clean bench bench-capture bench-load bench-report

help:
	@echo "EvidenceHub -- make targets:"
	@echo "  up             Start core services (4 MySQL shards + catalog + Redis) and wait for healthy"
	@echo "  down           Stop core services (keeps volumes)"
	@echo "  down-clean     Stop core services AND delete volumes (destructive)"
	@echo "  ps             Show status of core services"
	@echo "  migrate-legacy Apply the legacy schema migration to all 4 shards"
	@echo "  seed           Seed the legacy schema at the fast dev scale (SEED_ROWS_DEFAULT)"
	@echo "  seed-full      Seed the legacy schema at the full P1/P2 perf-report scale (SEED_ROWS_FULL)"
	@echo "  lint           Run ruff check"
	@echo "  fmt            Run ruff format + ruff check --fix"
	@echo "  typecheck      Run mypy --strict"
	@echo "  test           Run unit tests with coverage"
	@echo "  check          lint + typecheck + test (what CI runs)"
	@echo "  demo           up + migrate-legacy + seed, end to end (grows each phase)"
	@echo "  bench          bench-capture + bench-load + bench-report, writes docs/perf/baseline.md"
	@echo "  bench-capture  Capture EXPLAIN ANALYZE + performance_schema stats per critical query"
	@echo "  bench-load     Run the Locust load harness against the critical queries"
	@echo "  bench-report   Render docs/perf/baseline.md from the two captures above"
	@echo "  clean          down-clean + remove local tool caches"

.env:
	cp .env.example .env

up: .env
	docker compose --profile core up -d
	@echo "Waiting for core services to report healthy..."
	@for svc in $(CORE_SERVICES); do \
		echo "  waiting on evidencehub-$$svc"; \
		tries=0; \
		until [ "$$(docker inspect -f '{{.State.Health.Status}}' evidencehub-$$svc 2>/dev/null)" = "healthy" ]; do \
			tries=$$((tries+1)); \
			if [ $$tries -ge 60 ]; then echo "evidencehub-$$svc did not become healthy in time" >&2; exit 1; fi; \
			sleep 2; \
		done; \
	done
	@echo "All core services healthy."

down:
	docker compose --profile core down

down-clean:
	docker compose --profile core down -v

ps:
	docker compose --profile core ps

migrate-legacy: .env
	$(UV) run alembic -c db/legacy/alembic.ini upgrade head

SHARD ?=
migrate-optimized: .env
	@test -n "$(SHARD)" || (echo "Usage: make migrate-optimized SHARD=<index|all>"; exit 2)
	$(UV) run alembic -c db/optimized/alembic.ini -x shard=$(SHARD) upgrade head

seed: .env
	$(UV) run python db/legacy/seed_legacy.py

seed-full: .env
	$(UV) run python db/legacy/seed_legacy.py --full

lint:
	$(UV) run ruff check .

fmt:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest tests/unit --cov --cov-report=term-missing

check: lint typecheck test

bench-capture: .env
	$(UV) run python bench/capture_explain.py

bench-load: .env
	$(UV) run locust -f bench/locustfile.py --headless \
		--users 8 --spawn-rate 8 --run-time 60s \
		--csv bench/out/baseline --host mysql://legacy

bench-report: .env
	$(UV) run python bench/report.py --csv-prefix bench/out/baseline --locust-users 8 --locust-duration 60s

bench: bench-capture bench-load bench-report

demo: up migrate-legacy seed
	@echo "Demo (current phase slice): core services up, legacy schema migrated, dev-scale data seeded."

clean: down-clean
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .hypothesis htmlcov .coverage
