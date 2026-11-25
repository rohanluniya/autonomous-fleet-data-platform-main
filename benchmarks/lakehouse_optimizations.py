#!/usr/bin/env python3
"""Apache Iceberg Format V2 & Analytical Query Optimization Benchmark Suite.

Measures reproducible baseline vs. optimized performance directly on real Iceberg V2
Parquet tables via PyIceberg 0.12.0 + DuckDB 2.0.0:
1. Partition Pruning (5-day unpruned Iceberg scan vs. single-day pruned scan)
2. Small-File Compaction (20 micro-batch Parquet files vs. 1 compacted ZSTD file)
3. Predicate Pushdown (full scan + post-filter vs. Iceberg row-filter scan)
4. Skewed Join / Aggregation vs. Salted Two-Stage Aggregation (with exact parity check)
5. Incremental Date-Scoped Backfill (1 day) vs. Full Multi-Day Refresh
6. Wire Format Size (JSON vs. Avro Binary)
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.transport.local_wal import LocalWalEventBus


def _median_ms(fn: Any, runs: int = 7) -> tuple[float, Any]:
    times = []
    last = None
    for _ in range(runs):
        t0 = time.perf_counter()
        last = fn()
        times.append((time.perf_counter() - t0) * 1000.0)
    times.sort()
    return round(times[len(times) // 2], 3), last


def run_optimizations() -> dict[str, Any]:
    tmp_dir = Path(tempfile.mkdtemp(prefix="fleet-opt-bench-"))
    try:
        cfg = PlatformConfig(
            env="benchmark",
            execution_profile="local_native",
            transport_mode="local_wal",
            local_data_dir=tmp_dir / "data",
            warehouse_dir=tmp_dir / "warehouse",
            wal_dir=tmp_dir / "wal",
            checkpoint_dir=tmp_dir / "checkpoints",
            lineage_dir=tmp_dir / "lineage",
            sqlite_catalog_path=tmp_dir / "iceberg_catalog.db",
        )
        cfg.ensure_local_dirs()
        bus = LocalWalEventBus(cfg.wal_dir)
        reg = AvroContractRegistry()
        lh = IcebergLakehouse(cfg)
        ingestor = BronzeIngestor(cfg, bus, reg, lh)

        # Populate 5 distinct days x 4 micro-batches/day = 20 small Parquet files in Bronze
        base_day = datetime(2026, 11, 20, 10, 0, 0, tzinfo=UTC)
        for day_offset in range(5):
            for micro in range(4):
                sim_cfg = SimulationConfig(
                    virtual_fleet_size=250,
                    active_vehicles=250,
                    ticks=3,
                    step_seconds=10,
                    seed=1000 + day_offset * 10 + micro,
                    start_time_utc=base_day + timedelta(days=day_offset, hours=micro),
                    poison_count=0,
                    emit_auxiliary_topics=False,
                )
                FleetSimulator(cfg, bus, reg, sim_cfg).run()
                ingestor.drain_all_topics()

        bronze_tbl = lh.load_table("bronze.telemetry_raw")
        uncompacted_stats = lh.table_stats("bronze.telemetry_raw")

        # 1. Partition Pruning: 5-day full Iceberg scan vs. single-day partition-pruned Iceberg scan
        day_start = datetime(2026, 11, 22, 0, 0, 0)
        day_end = datetime(2026, 11, 23, 0, 0, 0)
        full_tasks = list(bronze_tbl.scan().plan_files())
        pruned_filter = And(
            GreaterThanOrEqual("event_time", day_start.isoformat()),
            LessThan("event_time", day_end.isoformat()),
        )
        pruned_tasks = list(bronze_tbl.scan(row_filter=pruned_filter).plan_files())
        full_ms, full_arrow = _median_ms(lambda: bronze_tbl.scan().to_arrow())
        pruned_ms, pruned_arrow = _median_ms(
            lambda: bronze_tbl.scan(row_filter=pruned_filter).to_arrow()
        )
        exp_pruning = {
            "baseline_files_scanned": len(full_tasks),
            "optimized_files_scanned": len(pruned_tasks),
            "baseline_rows_read": full_arrow.num_rows,
            "optimized_rows_read": pruned_arrow.num_rows,
            "baseline_median_ms": full_ms,
            "optimized_median_ms": pruned_ms,
            "speedup_x": round(full_ms / max(0.01, pruned_ms), 2),
        }

        # 2. Small-File Compaction (20 micro-batch Parquet files -> 5 day-partitioned files)
        uncomp_scan_ms, uncomp_tbl = _median_ms(lambda: lh.read_arrow("bronze.telemetry_raw"))
        comp_res = lh.compact_table("bronze.telemetry_raw")
        comp_scan_ms, comp_tbl = _median_ms(lambda: lh.read_arrow("bronze.telemetry_raw"))
        assert uncomp_tbl.num_rows == comp_tbl.num_rows
        exp_compaction = {
            "files_before": uncompacted_stats["data_file_count"],
            "files_after": comp_res["files_after"],
            "bytes_before": comp_res["bytes_before"],
            "bytes_after": comp_res["bytes_after"],
            "baseline_scan_median_ms": uncomp_scan_ms,
            "compacted_scan_median_ms": comp_scan_ms,
            "speedup_x": round(uncomp_scan_ms / max(0.01, comp_scan_ms), 2),
            "records_preserved": comp_tbl.num_rows,
        }

        # 3. Predicate Pushdown (full Arrow read + DuckDB filter vs. Iceberg row_filter pushdown)
        bronze_tbl = lh.load_table("bronze.telemetry_raw")
        pred_filter = And(
            GreaterThanOrEqual("event_time", day_start.isoformat()),
            And(LessThan("event_time", day_end.isoformat()), EqualTo("region", "sf_bay")),
        )
        no_push_ms, no_push_cnt = _median_ms(
            lambda: lh.query_sql(
                "SELECT COUNT(*) AS c FROM bronze_telemetry WHERE event_time >= TIMESTAMP '2026-11-22 00:00:00' AND event_time < TIMESTAMP '2026-11-23 00:00:00' AND region = 'sf_bay'"
            )[0]["c"]
        )
        push_ms, push_arrow = _median_ms(
            lambda: bronze_tbl.scan(
                row_filter=pred_filter, selected_fields=("event_id", "region", "speed_mps")
            ).to_arrow()
        )
        assert no_push_cnt == push_arrow.num_rows
        exp_pushdown = {
            "baseline_full_read_ms": no_push_ms,
            "optimized_pushdown_projection_ms": push_ms,
            "speedup_x": round(no_push_ms / max(0.01, push_ms), 2),
            "rows_verified": push_arrow.num_rows,
        }

        # 4. Incremental Single-Day Backfill vs. Full 5-Day Silver Refresh
        pipe = MedallionBatchPipeline(cfg, lh)
        full_ref_t0 = time.perf_counter()
        full_ref = pipe.run_bronze_to_silver(target_date=None)
        full_ref_ms = round((time.perf_counter() - full_ref_t0) * 1000.0, 2)

        inc_t0 = time.perf_counter()
        inc_ref = pipe.run_bronze_to_silver(target_date="2026-11-22")
        inc_ms = round((time.perf_counter() - inc_t0) * 1000.0, 2)
        assert full_ref["silver_clean_rows"] == inc_ref["silver_clean_rows"]
        exp_incremental = {
            "full_5day_refresh_ms": full_ref_ms,
            "incremental_1day_backfill_ms": inc_ms,
            "speedup_x": round(full_ref_ms / max(0.01, inc_ms), 2),
            "rows_verified": inc_ref["silver_clean_rows"],
        }

        # 5. Skewed Key Aggregation vs. Two-Stage Salted Aggregation (DuckDB verification)
        silver_arrow = lh.read_arrow("silver.telemetry_clean")
        con = duckdb.connect(":memory:")
        con.register("sil", silver_arrow)
        con.execute(
            """
            CREATE TABLE skewed AS
            SELECT CASE WHEN hash(event_id) % 10 < 8 THEN 'VEH-HOT-KEY' ELSE vehicle_id END AS skew_key,
                   speed_mps,
                   CAST(ABS(hash(event_id)) % 8 AS INTEGER) AS salt
            FROM sil
            """
        )
        unsalted_ms, unsalted_res = _median_ms(
            lambda: con.execute(
                "SELECT skew_key, ROUND(AVG(speed_mps), 6) AS avg_s, COUNT(*) AS cnt FROM skewed GROUP BY skew_key ORDER BY skew_key"
            ).fetchall()
        )
        salted_ms, salted_res = _median_ms(
            lambda: con.execute(
                """
                WITH stage1 AS (
                    SELECT skew_key, salt, SUM(speed_mps) AS sum_s, COUNT(*) AS cnt
                    FROM skewed GROUP BY skew_key, salt
                )
                SELECT skew_key, ROUND(SUM(sum_s) / SUM(cnt), 6) AS avg_s, SUM(cnt) AS cnt
                FROM stage1 GROUP BY skew_key ORDER BY skew_key
                """
            ).fetchall()
        )
        con.close()
        assert len(unsalted_res) == len(salted_res)
        for (k1, avg1, cnt1), (k2, avg2, cnt2) in zip(unsalted_res, salted_res, strict=True):
            assert k1 == k2 and cnt1 == cnt2 and abs(float(avg1) - float(avg2)) < 1e-5
        exp_skew = {
            "unsalted_median_ms": unsalted_ms,
            "salted_two_stage_median_ms": salted_ms,
            "exact_numeric_parity": True,
            "keys_verified": len(salted_res),
        }

        # 6. Wire Serialization Size: JSON vs. Avro Binary
        sample_rows = full_arrow.to_pylist()[:500]
        json_bytes = sum(len(json.dumps(r, default=str).encode("utf-8")) for r in sample_rows)
        avro_sample_rec = {
            "event_id": "evt-size-check",
            "vehicle_id": "VEH-000001",
            "trip_id": "TRIP-42-VEH-000001-001",
            "event_type": "telemetry",
            "event_time": "2026-11-24T12:00:00+00:00",
            "ingest_time": "2026-11-24T12:00:00.200000+00:00",
            "sequence_number": 42,
            "schema_version": "1.0.0",
            "source_id": "veh-gateway-1",
            "correlation_id": "abcd1234abcd1234",
            "trace_id": "0123456789abcdef0123456789abcdef",
            "location": {
                "latitude": 37.7749,
                "longitude": -122.4194,
                "heading_deg": 45.0,
                "altitude_m": 15.0,
            },
            "payload": {
                "vehicle_state": "driving",
                "region": "sf_bay",
                "vehicle_model": "AV-Sedan-G3",
                "software_version": "v4.2.0",
                "speed_mps": 14.2,
                "acceleration_mps2": 0.3,
                "braking_force_pct": 0.0,
                "battery_soc_pct": 78.4,
                "battery_temp_c": 31.2,
                "motor_temp_c": 58.1,
                "odometer_km": 14231.5,
                "fault_codes": [],
                "sensor_health_score": 0.985,
                "lidar_status": "OK",
                "camera_status": "OK",
                "radar_status": "OK",
            },
            "metadata": {"synthetic": "true", "sim_seed": "42", "route_id": "sf_route_market"},
        }
        avro_single = len(reg.serialize("fleet.telemetry.raw", avro_sample_rec))
        json_single = len(json.dumps(avro_sample_rec).encode("utf-8"))
        exp_wire = {
            "json_event_bytes": json_single,
            "avro_binary_event_bytes": avro_single,
            "avro_size_reduction_pct": round((1.0 - avro_single / json_single) * 100.0, 2),
            "sample_bronze_json_500_bytes": json_bytes,
        }

        report = {
            "engine": "pyiceberg-0.12.0 + duckdb-2.0.0 (local_native parity; Spark 4.1.4 suite in pipelines/spark/spark_optimization_benchmarks.py)",
            "dataset_total_rows": full_arrow.num_rows,
            "experiments": {
                "partition_pruning": exp_pruning,
                "small_file_compaction": exp_compaction,
                "predicate_pushdown_projection": exp_pushdown,
                "incremental_vs_full_refresh": exp_incremental,
                "skew_salting_parity": exp_skew,
                "avro_vs_json_wire_size": exp_wire,
            },
        }

        out_path = Path("results/benchmarks/lakehouse_optimizations.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        render_opt_plot(report, Path("docs/assets/lakehouse_optimizations.png"))
        return report
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def render_opt_plot(report: dict[str, Any], plot_path: Path) -> None:
    ex = report["experiments"]
    labels = [
        "Partition Pruning\n(5d -> 1d)",
        "Small-File Compaction\n(20 -> 5 files)",
        "Predicate Pushdown\n+ Projection",
        "Incremental Backfill\n(5d -> 1d)",
    ]
    baseline = [
        ex["partition_pruning"]["baseline_median_ms"],
        ex["small_file_compaction"]["baseline_scan_median_ms"],
        ex["predicate_pushdown_projection"]["baseline_full_read_ms"],
        ex["incremental_vs_full_refresh"]["full_5day_refresh_ms"],
    ]
    optimized = [
        ex["partition_pruning"]["optimized_median_ms"],
        ex["small_file_compaction"]["compacted_scan_median_ms"],
        ex["predicate_pushdown_projection"]["optimized_pushdown_projection_ms"],
        ex["incremental_vs_full_refresh"]["incremental_1day_backfill_ms"],
    ]

    fig, ax = plt.subplots(figsize=(10.5, 4.6), dpi=150)
    fig.patch.set_facecolor("#0b1120")
    ax.set_facecolor("#11192c")
    ax.tick_params(colors="#cbd5e1")
    for spine in ax.spines.values():
        spine.set_color("#253454")

    x = list(range(len(labels)))
    w = 0.34
    ax.bar([i - w / 2 for i in x], baseline, width=w, label="Baseline (ms)", color="#f87171")
    ax.bar([i + w / 2 for i in x], optimized, width=w, label="Optimized (ms)", color="#34d399")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.5)
    ax.set_ylabel("Median Runtime (ms, log scale)", color="#e2e8f0")
    ax.set_title(
        "Apache Iceberg Format V2 Lakehouse Optimization Benchmarks (Measured)",
        color="#f8fafc",
        fontsize=11,
    )
    ax.legend(facecolor="#0e1628", edgecolor="#253454", labelcolor="#e2e8f0")
    ax.grid(axis="y", color="#1e2c4a", linestyle="--", alpha=0.6)

    for i, (b, o) in enumerate(zip(baseline, optimized, strict=False)):
        ax.text(
            i + w / 2,
            o * 1.15,
            f"{b / max(0.01, o):.1f}x",
            color="#34d399",
            ha="center",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_path, facecolor=fig.get_facecolor())
    plt.close(fig)


if __name__ == "__main__":
    print(json.dumps(run_optimizations(), indent=2))
