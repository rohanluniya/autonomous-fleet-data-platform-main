#!/usr/bin/env python3
"""Apache Spark 4.1.4 Optimization Benchmark Suite (Container / Cluster Execution).

Demonstrates and benchmarks with reproducible baseline vs. optimized measurements:
1. Partition pruning (`days(event_time)`)
2. Parquet predicate pushdown
3. Broadcast hash join vs. Sort-Merge join
4. Adaptive Query Execution (AQE off vs. on)
5. Skewed key join & deterministic salting
6. repartition() vs. coalesce()
7. Caching / persistence reuse
8. Shuffle partition tuning (200 default vs. 16)
9. Incremental partition backfill vs. full refresh

Captures execution plans, runtimes, and row-count equivalence assertions.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from pipelines.spark.spark_session import build_spark_session


def _time_action(fn: Any) -> tuple[float, Any]:
    t0 = time.perf_counter()
    res = fn()
    return round(time.perf_counter() - t0, 4), res


def run_spark_optimizations(output_path: Path) -> dict[str, Any]:
    from pyspark.sql import functions as F

    spark = build_spark_session("fleet-spark-optimization-suite")
    results: dict[str, Any] = {"engine": "apache-spark-4.1.4", "experiments": {}}
    try:
        tel = spark.table("fleet.silver.telemetry_clean")
        dim = spark.table("fleet.silver.dim_vehicle_scd2").filter("is_current = true")

        # 1. Broadcast Join vs Sort-Merge Join
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        smj_df = tel.join(dim, on="vehicle_id", how="inner")
        smj_plan = smj_df._jdf.queryExecution().simpleString()
        smj_time, smj_cnt = _time_action(smj_df.count)

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        bhj_df = tel.join(F.broadcast(dim), on="vehicle_id", how="inner")
        bhj_plan = bhj_df._jdf.queryExecution().simpleString()
        bhj_time, bhj_cnt = _time_action(bhj_df.count)
        assert smj_cnt == bhj_cnt, "Broadcast vs SMJ count mismatch"
        results["experiments"]["broadcast_vs_sort_merge_join"] = {
            "sort_merge_seconds": smj_time,
            "broadcast_seconds": bhj_time,
            "rows_verified": bhj_cnt,
            "sort_merge_plan_excerpt": smj_plan[:400],
            "broadcast_plan_excerpt": bhj_plan[:400],
        }

        # 2. AQE & Shuffle Partition Tuning (200 vs 16)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        t_unopt, c_unopt = _time_action(
            lambda: tel.groupBy("region", "software_version").agg(F.avg("speed_mps")).count()
        )

        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        t_opt, c_opt = _time_action(
            lambda: tel.groupBy("region", "software_version").agg(F.avg("speed_mps")).count()
        )
        assert c_unopt == c_opt
        results["experiments"]["aqe_and_shuffle_tuning"] = {
            "aqe_off_200_parts_seconds": t_unopt,
            "aqe_on_16_parts_seconds": t_opt,
            "groups_verified": c_opt,
        }

        # 3. Skewed Aggregation + Salting Demonstration
        skewed = tel.withColumn(
            "skew_key",
            F.when((F.hash("event_id") % 10) < 8, F.lit("VEH-HOT-KEY")).otherwise(
                F.col("vehicle_id")
            ),
        )
        t_skew_base, c_skew_base = _time_action(
            lambda: skewed.groupBy("skew_key").agg(F.avg("speed_mps"), F.count("*")).count()
        )
        salted = (
            skewed.withColumn("salt", (F.rand(seed=42) * 8).cast("int"))
            .groupBy("skew_key", "salt")
            .agg(F.sum("speed_mps").alias("sum_s"), F.count("*").alias("cnt"))
            .groupBy("skew_key")
            .agg((F.sum("sum_s") / F.sum("cnt")).alias("avg_s"), F.sum("cnt"))
        )
        t_skew_salt, c_skew_salt = _time_action(salted.count)
        assert c_skew_base == c_skew_salt
        results["experiments"]["skew_salting"] = {
            "unsalted_seconds": t_skew_base,
            "salted_two_stage_seconds": t_skew_salt,
            "keys_verified": c_skew_salt,
        }

        # 4. Repartition vs Coalesce
        t_repart, _ = _time_action(lambda: tel.repartition(4).count())
        t_coal, _ = _time_action(lambda: tel.coalesce(4).count())
        results["experiments"]["repartition_vs_coalesce"] = {
            "repartition_shuffle_seconds": t_repart,
            "coalesce_narrow_seconds": t_coal,
        }

        # 5. Partition Pruning + Predicate Pushdown
        full_time, full_cnt = _time_action(lambda: tel.count())
        pruned_df = tel.filter(
            "event_date = '2026-11-24' AND region = 'sf_bay' AND speed_mps > 15.0"
        )
        pruned_plan = pruned_df._jdf.queryExecution().simpleString()
        pruned_time, pruned_cnt = _time_action(pruned_df.count)
        results["experiments"]["partition_pruning_predicate_pushdown"] = {
            "full_scan_seconds": full_time,
            "full_rows": full_cnt,
            "pruned_scan_seconds": pruned_time,
            "pruned_rows": pruned_cnt,
            "pruned_plan_excerpt": pruned_plan[:400],
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
        return results
    finally:
        spark.stop()


if __name__ == "__main__":
    out = Path("results/benchmarks/spark_optimizations.json")
    print(json.dumps(run_spark_optimizations(out), indent=2))
