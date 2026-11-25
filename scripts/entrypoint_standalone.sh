#!/usr/bin/env bash
set -euo pipefail

WAREHOUSE_CATALOG="${FLEET_SQLITE_CATALOG_PATH:-/data/iceberg_catalog.db}"
export FLEET_ENV="${FLEET_ENV:-dev}"
export FLEET_EXECUTION_PROFILE="${FLEET_EXECUTION_PROFILE:-local_native}"
export FLEET_TRANSPORT_MODE="${FLEET_TRANSPORT_MODE:-local_wal}"
export FLEET_LOCAL_DATA_DIR="${FLEET_LOCAL_DATA_DIR:-/data}"
export FLEET_WAREHOUSE_DIR="${FLEET_WAREHOUSE_DIR:-/data/warehouse}"
export FLEET_WAL_DIR="${FLEET_WAL_DIR:-/data/wal}"
export FLEET_CHECKPOINT_DIR="${FLEET_CHECKPOINT_DIR:-/data/checkpoints}"
export FLEET_LINEAGE_DIR="${FLEET_LINEAGE_DIR:-/data/lineage}"
export FLEET_SQLITE_CATALOG_PATH="${WAREHOUSE_CATALOG}"

mkdir -p "${FLEET_LOCAL_DATA_DIR}"

if [ "${FLEET_RESEED:-false}" = "true" ] || [ ! -f "${WAREHOUSE_CATALOG}" ]; then
  echo "[fleet-standalone] Seeding synthetic 1,000-vehicle lakehouse into ${FLEET_LOCAL_DATA_DIR}..."
  rm -rf "${FLEET_WAREHOUSE_DIR}" "${FLEET_WAL_DIR}" "${FLEET_CHECKPOINT_DIR}" "${WAREHOUSE_CATALOG}"
  python services/fleet-simulator/simulator.py --profile "${FLEET_SEED_PROFILE:-local}" --ticks "${FLEET_SEED_TICKS:-10}"
  python services/stream-processing/runner.py
  python services/ingestion/ingestor.py
  python pipelines/spark/local_batch_runner.py --seed-cdc-vehicles 1000 --run-dq --run-maintenance
  echo "[fleet-standalone] Lakehouse ready. Launching FastAPI + MapLibre Dashboard on 0.0.0.0:8000"
else
  echo "[fleet-standalone] Reusing persisted Iceberg lakehouse at ${FLEET_LOCAL_DATA_DIR}"
fi

exec uvicorn main:app --app-dir apps/fleet-api --host 0.0.0.0 --port "${FLEET_API_PORT:-8000}"
