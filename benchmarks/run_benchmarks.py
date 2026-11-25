#!/usr/bin/env python3
"""Sustained End-to-End Platform Benchmark Suite (Phase 13).

Executes configurable load profiles (smoke, local, stress, virtual-population),
measures sustained throughput, latency percentiles, state size, checkpoint save/restore
latency, Iceberg query latency, peak RSS memory, and storage amplification, and writes
raw JSON/CSV artifacts plus a visualization plot.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psutil

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.streaming.state_engine import StatefulStreamEngine, StreamEngineConfig
from fleet_platform.transport.local_wal import LocalWalEventBus

PROFILE_BENCH_TICKS: dict[str, dict[str, Any]] = {
    "smoke": {"virtual_fleet_size": 100, "active_vehicles": 100, "ticks": 12},
    "local": {"virtual_fleet_size": 1_000, "active_vehicles": 1_000, "ticks": 10},
    "stress": {"virtual_fleet_size": 10_000, "active_vehicles": 10_000, "ticks": 5},
    "virtual-population": {
        "virtual_fleet_size": 1_000_000,
        "active_vehicles": 2_000,
        "ticks": 8,
    },
}


def _pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int((q / 100.0) * len(s)) - 1))
    return round(s[idx], 3)


def benchmark_profile(profile_name: str, seed: int = 42) -> dict[str, Any]:
    spec = PROFILE_BENCH_TICKS[profile_name]
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"fleet-bench-{profile_name}-"))
    proc = psutil.Process(os.getpid())
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

        rss_before_mb = proc.memory_info().rss / (1024**2)

        # 1. Sustained Simulation & Avro Produce
        sim_cfg = SimulationConfig(
            virtual_fleet_size=spec["virtual_fleet_size"],
            active_vehicles=spec["active_vehicles"],
            ticks=spec["ticks"],
            step_seconds=10,
            seed=seed,
            duplicate_rate=0.015,
            out_of_order_rate=0.02,
            anomaly_rate=0.012,
            poison_count=5,
            emit_auxiliary_topics=True,
        )
        sim_res = FleetSimulator(cfg, bus, reg, sim_cfg).run()

        # 2. Stateful Stream Processing (in bounded micro-batches to measure p50/p95/p99 batch latency)
        engine = StatefulStreamEngine(
            cfg,
            bus,
            reg,
            engine_config=StreamEngineConfig(checkpoint_interval_events=10_000),
        )
        stream_t0 = time.perf_counter()
        batch_latencies_ms: list[float] = []
        total_stream_recs = 0
        while True:
            b0 = time.perf_counter()
            step = engine.process_available(max_records=2_500)
            if step["records_read"] == 0:
                break
            batch_latencies_ms.append((time.perf_counter() - b0) * 1000.0)
            total_stream_recs += step["records_read"]
        stream_dur = max(1e-6, time.perf_counter() - stream_t0)

        # Measure checkpoint save & restore latency explicitly
        ckpt_save = engine.save_checkpoint()
        t_rest0 = time.perf_counter()
        restored_engine = StatefulStreamEngine(cfg, bus, reg)
        restored_engine.restore_checkpoint()
        ckpt_restore_ms = (time.perf_counter() - t_rest0) * 1000.0
        ckpt_size_kb = round(engine.checkpoint_path.stat().st_size / 1024.0, 2)

        # 3. Durable Iceberg Bronze Ingestion
        ingestor = BronzeIngestor(cfg, bus, reg, lh)
        ing_res = ingestor.drain_all_topics(max_records_per_topic=500_000)
        bronze_stats = lh.table_stats("bronze.telemetry_raw")

        # 4. Silver + Gold Medallion Batch Curation
        batch = MedallionBatchPipeline(cfg, lh)
        silver_res = batch.run_bronze_to_silver()
        gold_res = batch.run_silver_to_gold()

        # 5. Analytical SQL Query Latency over Iceberg Gold/Silver (15 timed runs across 3 queries)
        queries = [
            "SELECT region, software_version, SUM(total_events), AVG(avg_speed_mps) FROM gold_fleet_activity GROUP BY 1, 2",
            "SELECT region, rule_id, SUM(anomaly_count) FROM gold_anomaly_rates GROUP BY 1, 2 ORDER BY 3 DESC",
            "SELECT region, COUNT(*) AS trips, AVG(risk_score) FROM gold_trip_risk GROUP BY 1",
        ]
        sql_latencies_ms: list[float] = []
        for _ in range(5):
            for q in queries:
                q0 = time.perf_counter()
                lh.query_sql(q)
                sql_latencies_ms.append((time.perf_counter() - q0) * 1000.0)

        rss_peak_mb = proc.memory_info().rss / (1024**2)
        avro_wire_bytes = max(1, sim_res["counts"]["bytes_produced"])
        wal_tel_bytes = max(1, bus.topic_stats()["fleet.telemetry.raw"]["bytes"])
        parquet_bronze_bytes = bronze_stats["total_bytes"]
        storage_ratio = round(parquet_bronze_bytes / wal_tel_bytes, 3)

        return {
            "profile": profile_name,
            "execution_profile": "local_native",
            "virtual_fleet_size": spec["virtual_fleet_size"],
            "active_vehicles": spec["active_vehicles"],
            "ticks": spec["ticks"],
            "total_events_produced": sim_res["total_events_produced"],
            "telemetry_events_streamed": total_stream_recs,
            "anomalies_detected": engine.total_anomalies_emitted,
            "sim_duration_s": sim_res["duration_seconds"],
            "sim_throughput_eps": sim_res["throughput_events_per_sec"],
            "sim_throughput_mb_s": round(sim_res["throughput_bytes_per_sec"] / (1024**2), 2),
            "stream_duration_s": round(stream_dur, 4),
            "stream_throughput_eps": round(total_stream_recs / stream_dur, 2),
            "stream_batch_p50_ms": _pct(batch_latencies_ms, 50),
            "stream_batch_p95_ms": _pct(batch_latencies_ms, 95),
            "stream_batch_p99_ms": _pct(batch_latencies_ms, 99),
            "stream_state_vehicles": len(engine.vehicle_states),
            "stream_dedup_keys": len(engine.dedup_seen),
            "checkpoint_size_kb": ckpt_size_kb,
            "checkpoint_save_ms": round(ckpt_save["duration_seconds"] * 1000.0, 2),
            "checkpoint_restore_ms": round(ckpt_restore_ms, 2),
            "bronze_ingest_duration_s": ing_res["duration_seconds"],
            "bronze_ingest_eps": round(
                ing_res["topics"]["fleet.telemetry.raw"]["committed"]
                / max(1e-6, ing_res["duration_seconds"]),
                2,
            ),
            "silver_curation_s": silver_res["duration_seconds"],
            "gold_curation_s": gold_res["duration_seconds"],
            "sql_query_p50_ms": _pct(sql_latencies_ms, 50),
            "sql_query_p95_ms": _pct(sql_latencies_ms, 95),
            "sql_query_p99_ms": _pct(sql_latencies_ms, 99),
            "wal_telemetry_bytes": wal_tel_bytes,
            "bronze_parquet_bytes": parquet_bronze_bytes,
            "avro_wire_bytes_total": avro_wire_bytes,
            "parquet_to_wal_ratio": storage_ratio,
            "rss_before_mb": round(rss_before_mb, 1),
            "rss_after_mb": round(rss_peak_mb, 1),
            "final_consumer_lag": bus.get_consumer_lag(
                "fleet.telemetry.raw", "fleet-bronze-ingestor"
            )["total_lag"],
        }
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def render_plots(rows: list[dict[str, Any]], plot_path: Path) -> None:
    profiles = [r["profile"] for r in rows]
    sim_eps = [r["sim_throughput_eps"] / 1000.0 for r in rows]
    stream_eps = [r["stream_throughput_eps"] / 1000.0 for r in rows]
    bronze_eps = [r["bronze_ingest_eps"] / 1000.0 for r in rows]
    sql_p50 = [r["sql_query_p50_ms"] for r in rows]
    sql_p95 = [r["sql_query_p95_ms"] for r in rows]
    ckpt_ms = [r["checkpoint_restore_ms"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.8), dpi=150)
    fig.patch.set_facecolor("#0b1120")
    for ax in (ax1, ax2):
        ax.set_facecolor("#11192c")
        ax.tick_params(colors="#cbd5e1")
        for spine in ax.spines.values():
            spine.set_color("#253454")

    x = list(range(len(profiles)))
    w = 0.26
    ax1.bar([i - w for i in x], sim_eps, width=w, label="Simulator Avro Produce", color="#38bdf8")
    ax1.bar(x, stream_eps, width=w, label="Stateful Stream Engine", color="#34d399")
    ax1.bar([i + w for i in x], bronze_eps, width=w, label="Bronze Iceberg Commit", color="#fbbf24")
    ax1.set_xticks(x)
    ax1.set_xticklabels(profiles, fontsize=9)
    ax1.set_ylabel("Throughput (k events/sec)", color="#e2e8f0")
    ax1.set_title("Measured Sustained Throughput (local_native)", color="#f8fafc", fontsize=11)
    ax1.legend(facecolor="#0e1628", edgecolor="#253454", labelcolor="#e2e8f0", fontsize=8.5)
    ax1.grid(axis="y", color="#1e2c4a", linestyle="--", alpha=0.6)

    ax2.bar([i - w for i in x], sql_p50, width=w, label="Iceberg SQL p50 (ms)", color="#38bdf8")
    ax2.bar(x, sql_p95, width=w, label="Iceberg SQL p95 (ms)", color="#a78bfa")
    ax2.bar([i + w for i in x], ckpt_ms, width=w, label="Checkpoint Restore (ms)", color="#f87171")
    ax2.set_xticks(x)
    ax2.set_xticklabels(profiles, fontsize=9)
    ax2.set_ylabel("Latency (ms)", color="#e2e8f0")
    ax2.set_title("Analytical SQL & Checkpoint Recovery Latency", color="#f8fafc", fontsize=11)
    ax2.legend(facecolor="#0e1628", edgecolor="#253454", labelcolor="#e2e8f0", fontsize=8.5)
    ax2.grid(axis="y", color="#1e2c4a", linestyle="--", alpha=0.6)

    plt.tight_layout()
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_path, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sustained fleet platform benchmarks.")
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=["smoke", "local", "stress", "virtual-population"],
        choices=sorted(PROFILE_BENCH_TICKS.keys()),
    )
    args = parser.parse_args()

    mem = psutil.virtual_memory()
    hardware = {
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "cpu_cores_logical": os.cpu_count(),
        "memory_total_gib": round(mem.total / (1024**3), 2),
        "python_version": sys.version.split()[0],
        "execution_profile": "local_native",
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }

    rows = [benchmark_profile(p) for p in args.profiles]
    payload = {"hardware": hardware, "results": rows}

    out_dir = Path("results/benchmarks")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "sustained_benchmarks.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )

    csv_path = out_dir / "sustained_benchmarks.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    render_plots(rows, Path("docs/assets/benchmark_throughput_latency.png"))
    print(json.dumps(payload, indent=2))
    print(
        f"\nMedian SQL p95 across profiles: {round(statistics.median(r['sql_query_p95_ms'] for r in rows), 2)} ms"
    )


if __name__ == "__main__":
    main()
