#!/usr/bin/env python3
"""Apache Spark 4.1.4 Medallion Batch Jobs (Bronze -> Silver -> Gold, SCD2, Backfill, Maintenance).

Executed inside the `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` container against
Lakekeeper Iceberg REST catalog (`fleet.*`).
"""

from __future__ import annotations

import argparse
from typing import Any

from pipelines.spark.spark_session import build_spark_session


def run_bronze_to_silver(spark: Any, target_date: str | None = None) -> None:
    """Canonical deduplication, validity filtering, quarantine routing, and point-in-time SCD2 join."""
    date_filter = f"WHERE CAST(event_time AS DATE) = DATE '{target_date}'" if target_date else ""
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW bronze_dedup AS
        SELECT * EXCEPT (rn)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id
                       ORDER BY ingest_time ASC, source_offset ASC
                   ) AS rn
            FROM fleet.bronze.telemetry_raw
            {date_filter}
        )
        WHERE rn = 1
        """
    )

    # Quarantine invalid GPS/kinematic rows rather than silently dropping them
    spark.sql(
        """
        INSERT INTO fleet.silver.telemetry_quarantine
        SELECT
            event_id,
            vehicle_id,
            event_time,
            CASE
                WHEN speed_mps < 0 OR speed_mps > 55 THEN 'IMPLAUSIBLE_SPEED'
                WHEN battery_soc_pct < 0 OR battery_soc_pct > 100 THEN 'INVALID_BATTERY_SOC'
                ELSE 'INVALID_GPS_BOUNDS'
            END AS quarantine_reason,
            current_timestamp() AS quarantined_at,
            to_json(struct(*)) AS raw_json
        FROM bronze_dedup
        WHERE speed_mps < 0 OR speed_mps > 55
           OR battery_soc_pct < 0 OR battery_soc_pct > 100
           OR (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180))
        """
    )

    # Point-in-time SCD Type 2 join on vehicle_id and event_time window
    spark.sql(
        """
        INSERT OVERWRITE fleet.silver.telemetry_clean
        SELECT
            b.event_id,
            b.vehicle_id,
            b.trip_id,
            b.event_time,
            b.ingest_time,
            DATE_FORMAT(b.event_time, 'yyyy-MM-dd') AS event_date,
            b.sequence_number,
            b.vehicle_state,
            b.region,
            b.vehicle_model,
            b.software_version,
            b.latitude,
            b.longitude,
            b.heading_deg,
            CAST(NULL AS STRING) AS h3_cell_res6,
            CAST(NULL AS STRING) AS h3_cell_res9,
            '' AS geofences_csv,
            CAST(NULL AS DOUBLE) AS route_deviation_m,
            b.speed_mps,
            b.acceleration_mps2,
            b.braking_force_pct,
            b.battery_soc_pct,
            b.battery_temp_c,
            b.motor_temp_c,
            b.odometer_km,
            b.sensor_health_score,
            b.fault_codes_csv,
            GREATEST(0.0, UNIX_TIMESTAMP(b.ingest_time) - UNIX_TIMESTAMP(b.event_time)) AS ingest_lag_seconds,
            (UNIX_TIMESTAMP(b.ingest_time) - UNIX_TIMESTAMP(b.event_time)) > 15 AS is_late_arrival,
            d.vehicle_sk
        FROM bronze_dedup b
        LEFT JOIN fleet.silver.dim_vehicle_scd2 d
          ON b.vehicle_id = d.vehicle_id
         AND b.event_time >= d.effective_from
         AND (d.effective_to IS NULL OR b.event_time < d.effective_to)
        WHERE b.speed_mps BETWEEN 0 AND 55
          AND b.battery_soc_pct BETWEEN 0 AND 100
        """
    )


def run_silver_to_gold(spark: Any) -> None:
    """Materialize Gold hourly fleet activity, battery health, and reliability KPIs."""
    spark.sql(
        """
        INSERT OVERWRITE fleet.gold.fleet_activity_hourly
        SELECT
            DATE_TRUNC('HOUR', event_time) AS hour_bucket,
            event_date,
            region,
            software_version,
            COUNT(DISTINCT vehicle_id) AS active_vehicles,
            COUNT(*) AS total_events,
            ROUND(AVG(speed_mps), 3) AS avg_speed_mps,
            ROUND(AVG(battery_soc_pct), 2) AS avg_battery_soc_pct,
            ROUND(AVG(sensor_health_score), 4) AS avg_sensor_health,
            SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END) AS late_arrival_events,
            current_timestamp() AS computed_at
        FROM fleet.silver.telemetry_clean
        GROUP BY 1, 2, 3, 4
        """
    )

    spark.sql(
        """
        INSERT OVERWRITE fleet.gold.battery_health_trends
        SELECT
            event_date,
            region,
            vehicle_model,
            software_version,
            ROUND(AVG(battery_soc_pct), 2) AS avg_battery_soc_pct,
            ROUND(MIN(battery_soc_pct), 2) AS min_battery_soc_pct,
            ROUND(AVG(battery_temp_c), 2) AS avg_battery_temp_c,
            ROUND(MAX(battery_temp_c), 2) AS max_battery_temp_c,
            ROUND(AVG(motor_temp_c), 2) AS avg_motor_temp_c,
            SUM(CASE WHEN battery_soc_pct < 15.0 THEN 1 ELSE 0 END) AS low_battery_events,
            SUM(CASE WHEN battery_temp_c >= 60.0 THEN 1 ELSE 0 END) AS thermal_alert_events
        FROM fleet.silver.telemetry_clean
        GROUP BY 1, 2, 3, 4
        """
    )


def run_iceberg_maintenance(spark: Any) -> None:
    """Compact small Parquet files, expire snapshots safely, and clean old orphan files (>=72h)."""
    for table in (
        "fleet.bronze.telemetry_raw",
        "fleet.silver.telemetry_clean",
        "fleet.gold.fleet_activity_hourly",
    ):
        spark.sql(
            f"""
            CALL fleet.system.rewrite_data_files(
                table => '{table}',
                strategy => 'binpack',
                options => map('target-file-size-bytes', '134217728')
            )
            """
        )
        spark.sql(
            f"""
            CALL fleet.system.expire_snapshots(
                table => '{table}',
                retain_last => 5
            )
            """
        )
        spark.sql(
            f"""
            CALL fleet.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '2026-11-20 00:00:00'
            )
            """
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark 4.1.4 Medallion Iceberg Pipeline.")
    parser.add_argument(
        "--stage",
        choices=["silver", "gold", "maintenance", "all"],
        default="all",
    )
    parser.add_argument("--target-date", default=None)
    args = parser.parse_args()

    spark = build_spark_session("fleet-medallion-spark-4.1")
    try:
        if args.stage in ("silver", "all"):
            run_bronze_to_silver(spark, target_date=args.target_date)
        if args.stage in ("gold", "all"):
            run_silver_to_gold(spark)
        if args.stage in ("maintenance", "all"):
            run_iceberg_maintenance(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
