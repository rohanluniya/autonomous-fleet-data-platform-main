.PHONY: setup detect-env lint typecheck test test-fault run-demo run-portable serve-api \
        benchmark optimize-bench sbom check-licenses docker-standalone docker-core docker-full docker-down clean

PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest
RUFF := .venv/bin/ruff
MYPY := .venv/bin/mypy

setup:
	uv sync --extra dev

detect-env:
	$(PYTHON) scripts/detect_env.py

lint:
	$(RUFF) check .
	$(RUFF) format --check .

typecheck:
	$(MYPY) fleet_platform apps/fleet-api services/ingestion

test:
	$(PYTEST) -v

test-fault:
	$(PYTEST) -v tests/fault/

run-demo:
	rm -rf .local_data
	$(PYTHON) services/fleet-simulator/simulator.py --profile local --ticks 10
	$(PYTHON) services/stream-processing/runner.py
	$(PYTHON) services/ingestion/ingestor.py
	$(PYTHON) pipelines/spark/local_batch_runner.py --seed-cdc-vehicles 1000 --run-dq --run-maintenance

serve-api:
	.venv/bin/uvicorn main:app --app-dir apps/fleet-api --host 127.0.0.1 --port 8000

benchmark:
	$(PYTHON) benchmarks/run_benchmarks.py --profiles smoke local stress virtual-population

optimize-bench:
	$(PYTHON) benchmarks/lakehouse_optimizations.py

sbom:
	$(PYTHON) scripts/generate_sbom.py --output results/security/sbom.cdx.json

check-licenses:
	$(PYTHON) scripts/check_licenses.py

run-portable:
	./scripts/run_portable.sh

docker-standalone:
	docker compose -f infra/docker/docker-compose.standalone.yml up --build

docker-core:
	docker compose -f infra/docker/docker-compose.yml --profile core up -d

docker-full:
	docker compose -f infra/docker/docker-compose.yml --profile full up -d

docker-down:
	docker compose -f infra/docker/docker-compose.yml --profile full down

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache
