#!/usr/bin/env bash
# Zero-Docker portable launcher for any macOS or Linux machine with Python 3.12+ (or uv).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT_DIR}"

if command -v uv >/dev/null 2>&1; then
  echo "[portable] Installing locked Python 3.12 dependencies with uv..."
  uv sync --extra dev
  PYTHON=".venv/bin/python"
  UVICORN=".venv/bin/uvicorn"
else
  echo "[portable] uv not found; creating .venv with python3..."
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install -e ".[dev]"
  PYTHON=".venv/bin/python"
  UVICORN=".venv/bin/uvicorn"
fi

if [ ! -f ".local_data/iceberg_catalog.db" ] || [ "${FLEET_RESEED:-false}" = "true" ]; then
  echo "[portable] Running 1,000-vehicle end-to-end pipeline into .local_data/..."
  rm -rf .local_data
  "${PYTHON}" services/fleet-simulator/simulator.py --profile "${FLEET_SEED_PROFILE:-local}" --ticks "${FLEET_SEED_TICKS:-10}"
  "${PYTHON}" services/stream-processing/runner.py
  "${PYTHON}" services/ingestion/ingestor.py
  "${PYTHON}" pipelines/spark/local_batch_runner.py --seed-cdc-vehicles 1000 --run-dq --run-maintenance
fi

echo "[portable] Starting FastAPI + MapLibre Dashboard at http://127.0.0.1:${FLEET_API_PORT:-8000}"
exec "${UVICORN}" main:app --app-dir apps/fleet-api --host "${FLEET_API_HOST:-127.0.0.1}" --port "${FLEET_API_PORT:-8000}"
