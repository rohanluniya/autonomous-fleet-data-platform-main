"""Configurable Data Quality & Cross-Layer Reconciliation Framework.

Thresholds are explicitly parameterized operational assumptions, never represented
as universal automotive safety standards. Every check outcome is persisted to
`quality.dq_check_results` and exposed through FastAPI and operational dashboards.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fleet_platform.config import PlatformConfig
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import get_logger, get_metrics


@dataclass(frozen=True)
class DataQualityThresholds:
    max_dlq_rate_warn_pct: float = 1.0
    max_dlq_rate_fail_pct: float = 5.0
    max_duplicate_rate_warn_pct: float = 5.0
    max_duplicate_rate_fail_pct: float = 15.0
    max_quarantine_rate_warn_pct: float = 1.0
    max_quarantine_rate_fail_pct: float = 5.0
    max_late_arrival_warn_pct: float = 10.0
    max_late_arrival_fail_pct: float = 25.0
    min_regions_expected: int = 3
    max_reconciliation_diff: int = 0


class DataQualityRunner:
    """Executes schema, range, uniqueness, referential, and reconciliation checks on Iceberg tables."""

    def __init__(
        self,
        config: PlatformConfig,
        lakehouse: IcebergLakehouse,
        thresholds: DataQualityThresholds | None = None,
    ) -> None:
        self.config = config
        self.lakehouse = lakehouse
        self.th = thresholds or DataQualityThresholds()
        self.logger = get_logger("data-quality", "validator", environment=config.env)
        self.metrics = get_metrics()

    def _evaluate_upper_bound(self, value: float, warn: float, fail: float) -> str:
        if value > fail:
            return "FAIL"
        if value > warn:
            return "WARN"
        return "PASS"

    def run_all_checks(self) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        executed_at = datetime.now(UTC).replace(tzinfo=None)
        checks: list[dict[str, Any]] = []

        def add_check(
            layer: str,
            name: str,
            status: str,
            observed: float,
            warn_th: float,
            fail_th: float,
            details: dict[str, Any],
        ) -> None:
            checks.append(
                {
                    "run_id": run_id,
                    "executed_at": executed_at,
                    "layer": layer,
                    "check_name": name,
                    "status": status,
                    "observed_value": round(float(observed), 4),
                    "warn_threshold": float(warn_th),
                    "fail_threshold": float(fail_th),
                    "details_json": json.dumps(details, default=str),
                }
            )
            self.metrics.dq_checks_total.labels(check_name=name, status=status).inc()

        # 1. Bronze counts & DLQ rate
        bronze_cnt = self.lakehouse.query_sql(
            "SELECT COUNT(*) AS cnt, COUNT(DISTINCT event_id) AS uniq FROM bronze_telemetry"
        )[0]
        dlq_cnt = self.lakehouse.query_sql("SELECT COUNT(*) AS cnt FROM bronze_dlq")[0]["cnt"]
        total_ingested = max(1, bronze_cnt["cnt"] + dlq_cnt)
        dlq_pct = (dlq_cnt / total_ingested) * 100.0
        add_check(
            "bronze",
            "dlq_poison_rate_pct",
            self._evaluate_upper_bound(
                dlq_pct, self.th.max_dlq_rate_warn_pct, self.th.max_dlq_rate_fail_pct
            ),
            dlq_pct,
            self.th.max_dlq_rate_warn_pct,
            self.th.max_dlq_rate_fail_pct,
            {"dlq_records": dlq_cnt, "bronze_records": bronze_cnt["cnt"]},
        )

        # 2. Silver uniqueness & nullability
        sil_stats = self.lakehouse.query_sql(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT event_id) AS distinct_events,
                SUM(CASE WHEN event_id IS NULL OR vehicle_id IS NULL OR event_time IS NULL THEN 1 ELSE 0 END) AS null_keys,
                SUM(CASE WHEN speed_mps < 0 OR speed_mps > 55 OR battery_soc_pct < 0 OR battery_soc_pct > 100 THEN 1 ELSE 0 END) AS out_of_bounds,
                SUM(CASE WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180) THEN 1 ELSE 0 END) AS bad_gps,
                COUNT(DISTINCT region) AS regions_present
            FROM silver_telemetry
            """
        )[0]
        dup_in_silver = int(sil_stats["total_rows"]) - int(sil_stats["distinct_events"])
        add_check(
            "silver",
            "silver_event_id_uniqueness",
            "PASS" if dup_in_silver == 0 else "FAIL",
            dup_in_silver,
            0.0,
            0.0,
            {
                "total_rows": sil_stats["total_rows"],
                "distinct_events": sil_stats["distinct_events"],
            },
        )
        add_check(
            "silver",
            "silver_key_nullability",
            "PASS" if int(sil_stats["null_keys"] or 0) == 0 else "FAIL",
            int(sil_stats["null_keys"] or 0),
            0.0,
            0.0,
            {"null_key_rows": int(sil_stats["null_keys"] or 0)},
        )
        add_check(
            "silver",
            "silver_kinematic_and_battery_bounds",
            "PASS" if int(sil_stats["out_of_bounds"] or 0) == 0 else "FAIL",
            int(sil_stats["out_of_bounds"] or 0),
            0.0,
            0.0,
            {"out_of_bounds_rows": int(sil_stats["out_of_bounds"] or 0)},
        )
        add_check(
            "silver",
            "silver_gps_valid_range",
            "PASS" if int(sil_stats["bad_gps"] or 0) == 0 else "FAIL",
            int(sil_stats["bad_gps"] or 0),
            0.0,
            0.0,
            {"invalid_gps_rows": int(sil_stats["bad_gps"] or 0)},
        )

        # 3. Bronze -> Silver reconciliation: distinct Bronze event_ids == Silver clean + Silver quarantine
        quar_cnt = self.lakehouse.query_sql("SELECT COUNT(*) AS cnt FROM silver_quarantine")[0][
            "cnt"
        ]
        reconcile_diff = abs(
            int(bronze_cnt["uniq"]) - (int(sil_stats["total_rows"]) + int(quar_cnt))
        )
        add_check(
            "reconciliation",
            "bronze_unique_to_silver_plus_quarantine",
            "PASS" if reconcile_diff <= self.th.max_reconciliation_diff else "FAIL",
            reconcile_diff,
            0.0,
            float(self.th.max_reconciliation_diff),
            {
                "bronze_distinct_event_ids": int(bronze_cnt["uniq"]),
                "silver_clean_rows": int(sil_stats["total_rows"]),
                "silver_quarantine_rows": int(quar_cnt),
            },
        )

        # 4. Silver -> Gold event count reconciliation
        gold_events = self.lakehouse.query_sql(
            "SELECT COALESCE(SUM(total_events), 0) AS cnt FROM gold_fleet_activity"
        )[0]["cnt"]
        gold_diff = abs(int(sil_stats["total_rows"]) - int(gold_events))
        add_check(
            "reconciliation",
            "silver_to_gold_activity_event_count",
            "PASS" if gold_diff == 0 else "FAIL",
            gold_diff,
            0.0,
            0.0,
            {
                "silver_clean_rows": int(sil_stats["total_rows"]),
                "gold_sum_events": int(gold_events),
            },
        )

        # 5. Trip temporal consistency (end_time >= start_time)
        bad_trips = self.lakehouse.query_sql(
            "SELECT COUNT(*) AS cnt FROM silver_trips WHERE end_time < start_time"
        )[0]["cnt"]
        add_check(
            "silver",
            "trip_temporal_consistency",
            "PASS" if bad_trips == 0 else "FAIL",
            bad_trips,
            0.0,
            0.0,
            {"invalid_trip_windows": bad_trips},
        )

        # 6. Referential integrity: anomalies.vehicle_id exists in silver_dim_vehicle (when populated)
        dim_cnt = self.lakehouse.query_sql("SELECT COUNT(*) AS cnt FROM silver_dim_vehicle")[0][
            "cnt"
        ]
        orphan_anoms = 0
        if dim_cnt > 0:
            orphan_anoms = self.lakehouse.query_sql(
                """
                SELECT COUNT(*) AS cnt
                FROM silver_anomalies a
                LEFT JOIN (SELECT DISTINCT vehicle_id FROM silver_dim_vehicle) d
                  ON a.vehicle_id = d.vehicle_id
                WHERE d.vehicle_id IS NULL
                """
            )[0]["cnt"]
        add_check(
            "silver",
            "referential_integrity_anomalies_to_dim_vehicle",
            "PASS" if orphan_anoms == 0 else "FAIL",
            orphan_anoms,
            0.0,
            0.0,
            {"dim_vehicle_rows": dim_cnt, "orphan_anomaly_vehicles": orphan_anoms},
        )

        # 7. Regional completeness
        regions_present = int(sil_stats["regions_present"] or 0)
        add_check(
            "gold",
            "regional_completeness",
            "PASS" if regions_present >= self.th.min_regions_expected else "WARN",
            regions_present,
            float(self.th.min_regions_expected),
            1.0,
            {"regions_present": regions_present, "expected_min": self.th.min_regions_expected},
        )

        # 8. Live duplicate ingestion rate (excludes intentional operator replay batches tagged with replay_batch_id)
        live_bronze = self.lakehouse.query_sql(
            """
            SELECT
                COUNT(*) AS live_total,
                COUNT(DISTINCT event_id) AS live_unique,
                (SELECT COUNT(*) FROM bronze_telemetry WHERE replay_batch_id IS NOT NULL) AS replay_rows
            FROM bronze_telemetry
            WHERE replay_batch_id IS NULL
            """
        )[0]
        dup_pct = (
            max(0, int(live_bronze["live_total"]) - int(live_bronze["live_unique"]))
            / max(1, int(live_bronze["live_total"]))
        ) * 100.0
        add_check(
            "reliability",
            "duplicate_ingestion_rate_pct",
            self._evaluate_upper_bound(
                dup_pct, self.th.max_duplicate_rate_warn_pct, self.th.max_duplicate_rate_fail_pct
            ),
            dup_pct,
            self.th.max_duplicate_rate_warn_pct,
            self.th.max_duplicate_rate_fail_pct,
            {
                "live_bronze_total": int(live_bronze["live_total"]),
                "live_bronze_unique": int(live_bronze["live_unique"]),
                "replayed_bronze_rows": int(live_bronze["replay_rows"]),
            },
        )

        self.lakehouse.append_rows("quality.dq_check_results", checks)

        passed = sum(1 for c in checks if c["status"] == "PASS")
        warned = sum(1 for c in checks if c["status"] == "WARN")
        failed = sum(1 for c in checks if c["status"] == "FAIL")
        overall = "FAIL" if failed > 0 else ("WARN" if warned > 0 else "PASS")
        self.logger.info(
            "dq.run_complete",
            f"Data quality suite finished: {overall} ({passed} pass, {warned} warn, {failed} fail)",
            pipeline_run_id=run_id,
            context={"passed": passed, "warned": warned, "failed": failed},
        )
        return {
            "run_id": run_id,
            "executed_at": executed_at.isoformat(),
            "overall_status": overall,
            "summary": {"passed": passed, "warned": warned, "failed": failed, "total": len(checks)},
            "checks": checks,
        }
