"""Apache Airflow 3.4.0 DAGs for Incremental Lakehouse, Daily Aggregates, Backfills,
Data Quality, Iceberg Maintenance, and Benchmarks.

Airflow orchestrates batch curation, reconciliation, maintenance, and backfills only;
it NEVER replaces continuous Apache Flink event-time streaming.
All tasks enforce idempotent boundaries, retries, execution timeouts, and concurrency limits.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.models.dag import DAG
    from airflow.operators.bash import BashOperator
except (
    ImportError
):  # Allows syntax & unit inspection in lightweight dev environments without Airflow installed
    DAG = None  # type: ignore[assignment]
    BashOperator = None  # type: ignore[assignment]

DEFAULT_ARGS = {
    "owner": "fleet-data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


def build_dags() -> dict[str, object]:
    """Construct all 6 platform Airflow 3.4.0 DAGs with deterministic idempotent tasks."""
    if DAG is None or BashOperator is None:
        return {}

    dags: dict[str, object] = {}

    # 1. Incremental Lakehouse Curation (every 15 minutes)
    with DAG(
        dag_id="fleet_incremental_lakehouse",
        description="Incremental Bronze ingestion -> CDC SCD2 -> Silver -> Gold -> DQ",
        schedule="*/15 * * * *",
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "incremental", "iceberg"],
    ) as dag_inc:
        ingest_bronze = BashOperator(
            task_id="drain_kafka_to_iceberg_bronze",
            bash_command="python services/ingestion/ingestor.py",
        )
        curate_medallion = BashOperator(
            task_id="curate_bronze_to_silver_to_gold",
            bash_command="python pipelines/spark/local_batch_runner.py --run-dq",
        )
        ingest_bronze >> curate_medallion
        dags[dag_inc.dag_id] = dag_inc

    # 2. Daily Fleet Aggregates & Reconciliation
    with DAG(
        dag_id="fleet_daily_aggregates",
        description="Daily Gold KPI recomputation and cross-layer reconciliation",
        schedule="0 2 * * *",
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["gold", "reconciliation"],
    ) as dag_daily:
        daily_gold = BashOperator(
            task_id="recompute_daily_gold_aggregates",
            bash_command="python pipelines/spark/local_batch_runner.py --backfill-date {{ ds }} --run-dq",
        )
        dags[dag_daily.dag_id] = dag_daily

    # 3. Parameterized Idempotent Historical Backfill
    with DAG(
        dag_id="fleet_historical_backfill",
        description="On-demand idempotent partition backfill for a specific execution date",
        schedule=None,
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=2,
        default_args=DEFAULT_ARGS,
        tags=["backfill", "idempotent"],
    ) as dag_backfill:
        replay_bronze = BashOperator(
            task_id="replay_bounded_bronze_slice",
            bash_command="python services/ingestion/replay.py --replay-batch-id backfill-{{ ds_nodash }}",
        )
        backfill_silver_gold = BashOperator(
            task_id="backfill_silver_and_gold_date",
            bash_command="python pipelines/spark/local_batch_runner.py --backfill-date {{ params.target_date | default(ds) }} --run-dq",
        )
        replay_bronze >> backfill_silver_gold
        dags[dag_backfill.dag_id] = dag_backfill

    # 4. Standalone Data Quality & Freshness Audit
    with DAG(
        dag_id="fleet_data_quality_audit",
        description="Scheduled DQ threshold checks, quarantine audit, and reconciliation",
        schedule="*/30 * * * *",
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["data-quality", "governance"],
    ) as dag_dq:
        run_dq = BashOperator(
            task_id="run_data_quality_checks",
            bash_command="python -c 'from fleet_platform.config import load_config; from fleet_platform.lakehouse import IcebergLakehouse; from fleet_platform.quality import DataQualityRunner; import json; r = DataQualityRunner(load_config(), IcebergLakehouse(load_config())).run_all_checks(); print(json.dumps(r[\"summary\"]))'",
        )
        dags[dag_dq.dag_id] = dag_dq

    # 5. Iceberg Table Maintenance & Retention (Compaction, Expiration, Orphan Safety Audit)
    with DAG(
        dag_id="fleet_iceberg_maintenance",
        description="Nightly small-file compaction, safe snapshot expiration, and >=72h orphan file audit",
        schedule="30 3 * * *",
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["iceberg", "maintenance", "retention"],
    ) as dag_maint:
        compact_and_expire = BashOperator(
            task_id="iceberg_compact_expire_orphan_audit",
            bash_command="python pipelines/spark/local_batch_runner.py --run-maintenance",
        )
        dags[dag_maint.dag_id] = dag_maint

    # 6. Manual Scheduled Benchmark Workflow
    with DAG(
        dag_id="fleet_benchmark_runner",
        description="Manual sustained performance & lakehouse optimization benchmark suite",
        schedule=None,
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["benchmarks", "performance"],
    ) as dag_bench:
        run_bench = BashOperator(
            task_id="execute_sustained_benchmarks",
            bash_command="python benchmarks/run_benchmarks.py --profiles smoke local",
        )
        dags[dag_bench.dag_id] = dag_bench

    return dags


globals().update(build_dags())
