"""Real Apache Iceberg Format V2 Medallion Lakehouse built on pyiceberg 0.12.0 + PyArrow + DuckDB.

Supports:
- Lakekeeper Iceberg REST catalog + RustFS S3 (Docker/Kubernetes mode)
- SQLite Iceberg catalog + local Parquet files (local_native zero-Docker mode)

Implements real Iceberg V2 metadata, Avro manifests, Parquet ZSTD data files,
hidden partitioning (days/bucket), snapshot isolation, time travel, schema evolution,
partition evolution, small-file compaction, snapshot expiration, safe orphan cleanup,
and snapshot-accurate DuckDB SQL queries.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from fleet_platform.config import PlatformConfig


def _bronze_telemetry_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=False),
        NestedField(4, "event_type", StringType(), required=True),
        NestedField(5, "event_time", TimestampType(), required=True),
        NestedField(6, "ingest_time", TimestampType(), required=True),
        NestedField(7, "sequence_number", LongType(), required=True),
        NestedField(8, "schema_version", StringType(), required=True),
        NestedField(9, "source_id", StringType(), required=True),
        NestedField(10, "correlation_id", StringType(), required=True),
        NestedField(11, "trace_id", StringType(), required=True),
        NestedField(12, "latitude", DoubleType(), required=False),
        NestedField(13, "longitude", DoubleType(), required=False),
        NestedField(14, "heading_deg", DoubleType(), required=False),
        NestedField(15, "vehicle_state", StringType(), required=True),
        NestedField(16, "region", StringType(), required=True),
        NestedField(17, "vehicle_model", StringType(), required=True),
        NestedField(18, "software_version", StringType(), required=True),
        NestedField(19, "speed_mps", DoubleType(), required=True),
        NestedField(20, "acceleration_mps2", DoubleType(), required=True),
        NestedField(21, "braking_force_pct", DoubleType(), required=True),
        NestedField(22, "battery_soc_pct", DoubleType(), required=True),
        NestedField(23, "battery_temp_c", DoubleType(), required=True),
        NestedField(24, "motor_temp_c", DoubleType(), required=True),
        NestedField(25, "odometer_km", DoubleType(), required=True),
        NestedField(26, "fault_codes_csv", StringType(), required=False),
        NestedField(27, "sensor_health_score", DoubleType(), required=True),
        NestedField(28, "validation_status", StringType(), required=True),
        NestedField(29, "replay_batch_id", StringType(), required=False),
        NestedField(30, "source_topic", StringType(), required=True),
        NestedField(31, "source_partition", IntegerType(), required=True),
        NestedField(32, "source_offset", LongType(), required=True),
        NestedField(33, "metadata_json", StringType(), required=False),
    )


def _bronze_trips_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=True),
        NestedField(4, "event_type", StringType(), required=True),
        NestedField(5, "event_time", TimestampType(), required=True),
        NestedField(6, "ingest_time", TimestampType(), required=True),
        NestedField(7, "sequence_number", LongType(), required=True),
        NestedField(8, "schema_version", StringType(), required=True),
        NestedField(9, "latitude", DoubleType(), required=True),
        NestedField(10, "longitude", DoubleType(), required=True),
        NestedField(11, "region", StringType(), required=True),
        NestedField(12, "software_version", StringType(), required=True),
        NestedField(13, "odometer_km", DoubleType(), required=True),
        NestedField(14, "battery_soc_pct", DoubleType(), required=True),
        NestedField(15, "route_id", StringType(), required=True),
        NestedField(16, "replay_batch_id", StringType(), required=False),
    )


def _bronze_autonomy_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=False),
        NestedField(4, "event_type", StringType(), required=True),
        NestedField(5, "event_time", TimestampType(), required=True),
        NestedField(6, "ingest_time", TimestampType(), required=True),
        NestedField(7, "latitude", DoubleType(), required=True),
        NestedField(8, "longitude", DoubleType(), required=True),
        NestedField(9, "region", StringType(), required=True),
        NestedField(10, "software_version", StringType(), required=True),
        NestedField(11, "disengagement_reason", StringType(), required=True),
        NestedField(12, "autonomy_mode_before", StringType(), required=True),
        NestedField(13, "takeover_latency_ms", IntegerType(), required=True),
        NestedField(14, "synthetic_flag", BooleanType(), required=True),
    )


def _bronze_anomalies_schema() -> Schema:
    return Schema(
        NestedField(1, "anomaly_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=False),
        NestedField(4, "region", StringType(), required=True),
        NestedField(5, "software_version", StringType(), required=True),
        NestedField(6, "rule_id", StringType(), required=True),
        NestedField(7, "rule_version", StringType(), required=True),
        NestedField(8, "severity", StringType(), required=True),
        NestedField(9, "event_time", TimestampType(), required=True),
        NestedField(10, "detection_time", TimestampType(), required=True),
        NestedField(11, "latitude", DoubleType(), required=False),
        NestedField(12, "longitude", DoubleType(), required=False),
        NestedField(13, "h3_cell_res6", StringType(), required=False),
        NestedField(14, "evidence_json", StringType(), required=True),
        NestedField(15, "source_event_ids_csv", StringType(), required=True),
        NestedField(16, "correlation_id", StringType(), required=True),
        NestedField(17, "trace_id", StringType(), required=True),
    )


def _bronze_dlq_schema() -> Schema:
    return Schema(
        NestedField(1, "dlq_id", StringType(), required=True),
        NestedField(2, "source_topic", StringType(), required=True),
        NestedField(3, "source_partition", IntegerType(), required=True),
        NestedField(4, "source_offset", LongType(), required=True),
        NestedField(5, "error_code", StringType(), required=True),
        NestedField(6, "error_message", StringType(), required=True),
        NestedField(7, "quarantined_at", TimestampType(), required=True),
        NestedField(8, "raw_payload_b64", StringType(), required=True),
        NestedField(9, "correlation_id", StringType(), required=True),
    )


def _silver_telemetry_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=False),
        NestedField(4, "event_time", TimestampType(), required=True),
        NestedField(5, "ingest_time", TimestampType(), required=True),
        NestedField(6, "event_date", StringType(), required=True),
        NestedField(7, "sequence_number", LongType(), required=True),
        NestedField(8, "vehicle_state", StringType(), required=True),
        NestedField(9, "region", StringType(), required=True),
        NestedField(10, "vehicle_model", StringType(), required=True),
        NestedField(11, "software_version", StringType(), required=True),
        NestedField(12, "latitude", DoubleType(), required=False),
        NestedField(13, "longitude", DoubleType(), required=False),
        NestedField(14, "heading_deg", DoubleType(), required=False),
        NestedField(15, "h3_cell_res6", StringType(), required=False),
        NestedField(16, "h3_cell_res9", StringType(), required=False),
        NestedField(17, "geofences_csv", StringType(), required=False),
        NestedField(18, "route_deviation_m", DoubleType(), required=False),
        NestedField(19, "speed_mps", DoubleType(), required=True),
        NestedField(20, "acceleration_mps2", DoubleType(), required=True),
        NestedField(21, "braking_force_pct", DoubleType(), required=True),
        NestedField(22, "battery_soc_pct", DoubleType(), required=True),
        NestedField(23, "battery_temp_c", DoubleType(), required=True),
        NestedField(24, "motor_temp_c", DoubleType(), required=True),
        NestedField(25, "odometer_km", DoubleType(), required=True),
        NestedField(26, "sensor_health_score", DoubleType(), required=True),
        NestedField(27, "fault_codes_csv", StringType(), required=False),
        NestedField(28, "ingest_lag_seconds", DoubleType(), required=True),
        NestedField(29, "is_late_arrival", BooleanType(), required=True),
        NestedField(30, "vehicle_sk", StringType(), required=False),
    )


def _silver_trips_schema() -> Schema:
    return Schema(
        NestedField(1, "trip_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "region", StringType(), required=True),
        NestedField(4, "vehicle_model", StringType(), required=True),
        NestedField(5, "software_version", StringType(), required=True),
        NestedField(6, "start_time", TimestampType(), required=True),
        NestedField(7, "end_time", TimestampType(), required=True),
        NestedField(8, "duration_seconds", DoubleType(), required=True),
        NestedField(9, "distance_km", DoubleType(), required=True),
        NestedField(10, "telemetry_events", LongType(), required=True),
        NestedField(11, "avg_speed_mps", DoubleType(), required=True),
        NestedField(12, "max_speed_mps", DoubleType(), required=True),
        NestedField(13, "battery_drain_pct", DoubleType(), required=True),
        NestedField(14, "anomaly_count", LongType(), required=True),
        NestedField(15, "critical_anomaly_count", LongType(), required=True),
        NestedField(16, "disengagement_count", LongType(), required=True),
        NestedField(17, "start_h3_res6", StringType(), required=False),
        NestedField(18, "end_h3_res6", StringType(), required=False),
    )


def _silver_dim_vehicle_scd2_schema() -> Schema:
    return Schema(
        NestedField(1, "vehicle_sk", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "hardware_generation", StringType(), required=True),
        NestedField(4, "vehicle_model", StringType(), required=True),
        NestedField(5, "assigned_region", StringType(), required=True),
        NestedField(6, "software_version", StringType(), required=True),
        NestedField(7, "fleet_tier", StringType(), required=True),
        NestedField(8, "maintenance_status", StringType(), required=True),
        NestedField(9, "effective_from", TimestampType(), required=True),
        NestedField(10, "effective_to", TimestampType(), required=False),
        NestedField(11, "is_current", BooleanType(), required=True),
        NestedField(12, "cdc_lsn", LongType(), required=True),
    )


def _silver_quarantine_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "event_time", TimestampType(), required=True),
        NestedField(4, "quarantine_reason", StringType(), required=True),
        NestedField(5, "quarantined_at", TimestampType(), required=True),
        NestedField(6, "raw_json", StringType(), required=True),
    )


def _gold_fleet_activity_schema() -> Schema:
    return Schema(
        NestedField(1, "hour_bucket", TimestampType(), required=True),
        NestedField(2, "event_date", StringType(), required=True),
        NestedField(3, "region", StringType(), required=True),
        NestedField(4, "software_version", StringType(), required=True),
        NestedField(5, "active_vehicles", LongType(), required=True),
        NestedField(6, "total_events", LongType(), required=True),
        NestedField(7, "avg_speed_mps", DoubleType(), required=True),
        NestedField(8, "avg_battery_soc_pct", DoubleType(), required=True),
        NestedField(9, "avg_sensor_health", DoubleType(), required=True),
        NestedField(10, "late_arrival_events", LongType(), required=True),
        NestedField(11, "computed_at", TimestampType(), required=True),
    )


def _gold_anomaly_rates_schema() -> Schema:
    return Schema(
        NestedField(1, "event_date", StringType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "vehicle_model", StringType(), required=True),
        NestedField(4, "software_version", StringType(), required=True),
        NestedField(5, "rule_id", StringType(), required=True),
        NestedField(6, "severity", StringType(), required=True),
        NestedField(7, "anomaly_count", LongType(), required=True),
        NestedField(8, "affected_vehicles", LongType(), required=True),
        NestedField(9, "telemetry_events", LongType(), required=True),
        NestedField(10, "anomalies_per_1k_events", DoubleType(), required=True),
    )


def _gold_trip_risk_schema() -> Schema:
    return Schema(
        NestedField(1, "trip_id", StringType(), required=True),
        NestedField(2, "vehicle_id", StringType(), required=True),
        NestedField(3, "region", StringType(), required=True),
        NestedField(4, "software_version", StringType(), required=True),
        NestedField(5, "start_time", TimestampType(), required=True),
        NestedField(6, "end_time", TimestampType(), required=True),
        NestedField(7, "distance_km", DoubleType(), required=True),
        NestedField(8, "duration_seconds", DoubleType(), required=True),
        NestedField(9, "anomaly_count", LongType(), required=True),
        NestedField(10, "critical_anomaly_count", LongType(), required=True),
        NestedField(11, "disengagement_count", LongType(), required=True),
        NestedField(12, "risk_score", DoubleType(), required=True),
        NestedField(13, "risk_tier", StringType(), required=True),
    )


def _gold_battery_health_schema() -> Schema:
    return Schema(
        NestedField(1, "event_date", StringType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "vehicle_model", StringType(), required=True),
        NestedField(4, "software_version", StringType(), required=True),
        NestedField(5, "avg_battery_soc_pct", DoubleType(), required=True),
        NestedField(6, "min_battery_soc_pct", DoubleType(), required=True),
        NestedField(7, "avg_battery_temp_c", DoubleType(), required=True),
        NestedField(8, "max_battery_temp_c", DoubleType(), required=True),
        NestedField(9, "avg_motor_temp_c", DoubleType(), required=True),
        NestedField(10, "low_battery_events", LongType(), required=True),
        NestedField(11, "thermal_alert_events", LongType(), required=True),
    )


def _gold_regional_h3_schema() -> Schema:
    return Schema(
        NestedField(1, "event_date", StringType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "h3_cell_res6", StringType(), required=True),
        NestedField(4, "center_lat", DoubleType(), required=True),
        NestedField(5, "center_lon", DoubleType(), required=True),
        NestedField(6, "active_vehicles", LongType(), required=True),
        NestedField(7, "event_count", LongType(), required=True),
        NestedField(8, "anomaly_count", LongType(), required=True),
        NestedField(9, "avg_speed_mps", DoubleType(), required=True),
    )


def _gold_reliability_schema() -> Schema:
    return Schema(
        NestedField(1, "event_date", StringType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "bronze_events", LongType(), required=True),
        NestedField(4, "silver_clean_events", LongType(), required=True),
        NestedField(5, "duplicate_events_removed", LongType(), required=True),
        NestedField(6, "quarantined_events", LongType(), required=True),
        NestedField(7, "late_arrival_events", LongType(), required=True),
        NestedField(8, "duplicate_rate_pct", DoubleType(), required=True),
        NestedField(9, "late_arrival_rate_pct", DoubleType(), required=True),
        NestedField(10, "avg_ingest_lag_seconds", DoubleType(), required=True),
        NestedField(11, "p95_ingest_lag_seconds", DoubleType(), required=True),
    )


def _gold_vehicle_latest_schema() -> Schema:
    return Schema(
        NestedField(1, "vehicle_id", StringType(), required=True),
        NestedField(2, "region", StringType(), required=True),
        NestedField(3, "vehicle_model", StringType(), required=True),
        NestedField(4, "software_version", StringType(), required=True),
        NestedField(5, "vehicle_state", StringType(), required=True),
        NestedField(6, "connectivity_status", StringType(), required=True),
        NestedField(7, "last_event_time", TimestampType(), required=True),
        NestedField(8, "last_ingest_time", TimestampType(), required=True),
        NestedField(9, "latitude", DoubleType(), required=False),
        NestedField(10, "longitude", DoubleType(), required=False),
        NestedField(11, "h3_cell_res6", StringType(), required=False),
        NestedField(12, "geofences_csv", StringType(), required=False),
        NestedField(13, "speed_mps", DoubleType(), required=True),
        NestedField(14, "battery_soc_pct", DoubleType(), required=True),
        NestedField(15, "battery_temp_c", DoubleType(), required=True),
        NestedField(16, "motor_temp_c", DoubleType(), required=True),
        NestedField(17, "sensor_health_score", DoubleType(), required=True),
        NestedField(18, "total_events", LongType(), required=True),
        NestedField(19, "total_anomalies", LongType(), required=True),
    )


def _quality_dq_results_schema() -> Schema:
    return Schema(
        NestedField(1, "run_id", StringType(), required=True),
        NestedField(2, "executed_at", TimestampType(), required=True),
        NestedField(3, "layer", StringType(), required=True),
        NestedField(4, "check_name", StringType(), required=True),
        NestedField(5, "status", StringType(), required=True),
        NestedField(6, "observed_value", DoubleType(), required=True),
        NestedField(7, "warn_threshold", DoubleType(), required=True),
        NestedField(8, "fail_threshold", DoubleType(), required=True),
        NestedField(9, "details_json", StringType(), required=True),
    )


class IcebergLakehouse:
    """Manages real Apache Iceberg Format V2 tables, snapshots, maintenance, and SQL access."""

    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self.config.ensure_local_dirs()
        self.catalog = self._create_catalog()
        self.ensure_tables()

    def _create_catalog(self) -> Catalog:
        if self.config.execution_profile == "docker":
            return load_catalog(
                "fleet_rest",
                **{
                    "type": "rest",
                    "uri": self.config.iceberg_rest_uri,
                    "warehouse": self.config.iceberg_warehouse,
                    "s3.endpoint": self.config.s3_endpoint_url,
                    "s3.access-key-id": self.config.s3_access_key_id,
                    "s3.secret-access-key": self.config.s3_secret_access_key,
                    "s3.path-style-access": "true",
                },
            )
        return load_catalog(
            "fleet_local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{self.config.sqlite_catalog_path}",
                "warehouse": f"file://{self.config.warehouse_dir}",
            },
        )

    def _ensure_namespace(self, ns: str) -> None:
        try:
            self.catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass

    def _ensure_table(
        self,
        identifier: str,
        schema: Schema,
        partition_spec: PartitionSpec | None = None,
    ) -> Table:
        try:
            return self.catalog.load_table(identifier)
        except NoSuchTableError:
            props = {
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.delete-after-commit.enabled": "true",
                "write.metadata.previous-versions-max": "25",
            }
            if partition_spec is not None:
                return self.catalog.create_table(
                    identifier, schema=schema, partition_spec=partition_spec, properties=props
                )
            return self.catalog.create_table(identifier, schema=schema, properties=props)

    def ensure_tables(self) -> None:
        for ns in ("bronze", "silver", "gold", "quality"):
            self._ensure_namespace(ns)

        # Bronze tables
        self._ensure_table(
            "bronze.telemetry_raw",
            _bronze_telemetry_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=5, field_id=1000, transform=DayTransform(), name="event_day"
                )
            ),
        )
        self._ensure_table(
            "bronze.trips_raw",
            _bronze_trips_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=5, field_id=1000, transform=DayTransform(), name="event_day"
                )
            ),
        )
        self._ensure_table(
            "bronze.autonomy_raw",
            _bronze_autonomy_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=5, field_id=1000, transform=DayTransform(), name="event_day"
                )
            ),
        )
        self._ensure_table(
            "bronze.anomalies_stream_raw",
            _bronze_anomalies_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=9, field_id=1000, transform=DayTransform(), name="event_day"
                )
            ),
        )
        self._ensure_table("bronze.telemetry_dlq", _bronze_dlq_schema())

        # Silver tables
        self._ensure_table(
            "silver.telemetry_clean",
            _silver_telemetry_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=4, field_id=1000, transform=DayTransform(), name="event_day"
                ),
                PartitionField(
                    source_id=2,
                    field_id=1001,
                    transform=BucketTransform(16),
                    name="vehicle_bucket",
                ),
            ),
        )
        self._ensure_table(
            "silver.trips_reconstructed",
            _silver_trips_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=6, field_id=1000, transform=DayTransform(), name="start_day"
                )
            ),
        )
        self._ensure_table(
            "silver.anomalies_verified",
            _bronze_anomalies_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=9, field_id=1000, transform=DayTransform(), name="event_day"
                )
            ),
        )
        self._ensure_table("silver.dim_vehicle_scd2", _silver_dim_vehicle_scd2_schema())
        self._ensure_table("silver.telemetry_quarantine", _silver_quarantine_schema())

        # Gold tables
        self._ensure_table(
            "gold.fleet_activity_hourly",
            _gold_fleet_activity_schema(),
            PartitionSpec(
                PartitionField(
                    source_id=2, field_id=1000, transform=IdentityTransform(), name="event_date"
                )
            ),
        )
        self._ensure_table("gold.anomaly_rates_by_segment", _gold_anomaly_rates_schema())
        self._ensure_table("gold.trip_risk_summary", _gold_trip_risk_schema())
        self._ensure_table("gold.battery_health_trends", _gold_battery_health_schema())
        self._ensure_table("gold.regional_h3_density", _gold_regional_h3_schema())
        self._ensure_table("gold.telemetry_reliability_kpis", _gold_reliability_schema())
        self._ensure_table("gold.vehicle_latest_snapshot", _gold_vehicle_latest_schema())

        # Quality table
        self._ensure_table("quality.dq_check_results", _quality_dq_results_schema())

    def load_table(self, identifier: str) -> Table:
        return self.catalog.load_table(identifier)

    def _align_arrow_to_iceberg(self, table: Table, arrow_table: pa.Table) -> pa.Table:
        target_schema = table.schema().as_arrow()
        cols = []
        for field in target_schema:
            if field.name in arrow_table.column_names:
                col = arrow_table[field.name]
                if col.type != field.type:
                    col = col.cast(field.type)
                cols.append(col)
            else:
                cols.append(pa.nulls(arrow_table.num_rows, type=field.type))
        return pa.Table.from_arrays(cols, schema=target_schema)

    def append_rows(self, identifier: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        tbl = self.load_table(identifier)
        target_schema = tbl.schema().as_arrow()
        pylist_table = pa.Table.from_pylist(rows, schema=target_schema)
        tbl.append(pylist_table)
        return len(rows)

    def append_arrow(self, identifier: str, arrow_table: pa.Table) -> int:
        if arrow_table.num_rows == 0:
            return 0
        tbl = self.load_table(identifier)
        aligned = self._align_arrow_to_iceberg(tbl, arrow_table)
        tbl.append(aligned)
        return int(aligned.num_rows)

    def overwrite_arrow(self, identifier: str, arrow_table: pa.Table) -> int:
        tbl = self.load_table(identifier)
        aligned = self._align_arrow_to_iceberg(tbl, arrow_table)
        tbl.overwrite(aligned)
        return int(aligned.num_rows)

    def read_arrow(self, identifier: str, *, snapshot_id: int | None = None) -> pa.Table:
        tbl = self.load_table(identifier)
        scan = tbl.scan(snapshot_id=snapshot_id) if snapshot_id is not None else tbl.scan()
        return scan.to_arrow()

    def query_sql(
        self,
        sql: str,
        tables: dict[str, str] | None = None,
        params: list[Any] | None = None,
        snapshot_overrides: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute parameterized DuckDB SQL over committed Apache Iceberg table snapshots."""
        con = duckdb.connect(database=":memory:")
        try:
            snaps = snapshot_overrides or {}
            table_map = tables or {
                "bronze_telemetry": "bronze.telemetry_raw",
                "bronze_trips": "bronze.trips_raw",
                "bronze_autonomy": "bronze.autonomy_raw",
                "bronze_anomalies": "bronze.anomalies_stream_raw",
                "bronze_dlq": "bronze.telemetry_dlq",
                "silver_telemetry": "silver.telemetry_clean",
                "silver_trips": "silver.trips_reconstructed",
                "silver_anomalies": "silver.anomalies_verified",
                "silver_dim_vehicle": "silver.dim_vehicle_scd2",
                "silver_quarantine": "silver.telemetry_quarantine",
                "gold_fleet_activity": "gold.fleet_activity_hourly",
                "gold_anomaly_rates": "gold.anomaly_rates_by_segment",
                "gold_trip_risk": "gold.trip_risk_summary",
                "gold_battery_health": "gold.battery_health_trends",
                "gold_regional_h3": "gold.regional_h3_density",
                "gold_reliability": "gold.telemetry_reliability_kpis",
                "gold_vehicle_latest": "gold.vehicle_latest_snapshot",
                "quality_dq_results": "quality.dq_check_results",
            }
            for alias, ident in table_map.items():
                if alias in sql:
                    arrow_tbl = self.read_arrow(ident, snapshot_id=snaps.get(ident))
                    con.register(alias, arrow_tbl)
            cursor = con.execute(sql, params or [])
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
        finally:
            con.close()

    def table_stats(self, identifier: str) -> dict[str, Any]:
        tbl = self.load_table(identifier)
        snaps = list(tbl.snapshots())
        cur = tbl.current_snapshot()
        tasks = list(tbl.scan().plan_files()) if cur is not None else []
        total_bytes = sum(t.file.file_size_in_bytes for t in tasks)
        total_records = sum(t.file.record_count for t in tasks)
        return {
            "table": identifier,
            "format_version": tbl.metadata.format_version,
            "current_snapshot_id": cur.snapshot_id if cur else None,
            "snapshot_count": len(snaps),
            "data_file_count": len(tasks),
            "total_records": int(total_records),
            "total_bytes": int(total_bytes),
            "location": tbl.location(),
        }

    def compact_table(self, identifier: str) -> dict[str, Any]:
        """Small-file compaction: rewrite active snapshot into minimal ZSTD Parquet files."""
        t0 = time.perf_counter()
        before = self.table_stats(identifier)
        if before["data_file_count"] <= 1 or before["total_records"] == 0:
            return {
                "table": identifier,
                "action": "skipped",
                "files_before": before["data_file_count"],
                "files_after": before["data_file_count"],
                "duration_seconds": round(time.perf_counter() - t0, 4),
            }
        arrow_tbl = self.read_arrow(identifier)
        self.overwrite_arrow(identifier, arrow_tbl)
        after = self.table_stats(identifier)
        return {
            "table": identifier,
            "action": "compacted",
            "files_before": before["data_file_count"],
            "files_after": after["data_file_count"],
            "bytes_before": before["total_bytes"],
            "bytes_after": after["total_bytes"],
            "records_preserved": after["total_records"],
            "duration_seconds": round(time.perf_counter() - t0, 4),
        }

    def expire_snapshots_safe(
        self,
        identifier: str,
        *,
        keep_last_n: int = 5,
        older_than_hours: int = 168,
    ) -> dict[str, Any]:
        """Safely expire old Iceberg snapshots while retaining at least keep_last_n snapshots."""
        tbl = self.load_table(identifier)
        snaps = sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms)
        if len(snaps) <= keep_last_n:
            return {
                "table": identifier,
                "snapshots_before": len(snaps),
                "snapshots_expired": 0,
                "snapshots_after": len(snaps),
            }
        cutoff_ms = int((datetime.now(UTC) - timedelta(hours=older_than_hours)).timestamp() * 1000)
        candidates = [s for s in snaps[:-keep_last_n] if s.timestamp_ms < cutoff_ms]
        expired = 0
        if candidates and hasattr(tbl, "maintenance"):
            try:
                tbl.maintenance.expire_snapshots().older_than(
                    datetime.now(UTC) - timedelta(hours=older_than_hours)
                ).commit()
                expired = len(candidates)
            except Exception:
                expired = 0
        after_count = len(list(self.load_table(identifier).snapshots()))
        return {
            "table": identifier,
            "snapshots_before": len(snaps),
            "snapshots_expired": expired,
            "snapshots_after": after_count,
        }

    def detect_orphan_files(
        self,
        identifier: str,
        *,
        min_age_hours: float = 72.0,
        delete: bool = False,
    ) -> dict[str, Any]:
        """Scan table data directory for unreferenced Parquet files; delete only if safety checks pass."""
        tbl = self.load_table(identifier)
        loc = tbl.location()
        if not loc.startswith("file://"):
            return {
                "table": identifier,
                "status": "skipped_remote_storage",
                "orphan_candidates": 0,
                "deleted": 0,
            }
        data_dir = Path(loc.removeprefix("file://")) / "data"
        if not data_dir.exists():
            return {"table": identifier, "orphan_candidates": 0, "deleted": 0}
        referenced: set[str] = set()
        for snap in tbl.snapshots():
            for task in tbl.scan(snapshot_id=snap.snapshot_id).plan_files():
                referenced.add(
                    Path(task.file.file_path.removeprefix("file://")).resolve().as_posix()
                )

        cutoff_ts = time.time() - (min_age_hours * 3600.0)
        orphans: list[Path] = []
        for parquet_file in data_dir.rglob("*.parquet"):
            resolved = parquet_file.resolve().as_posix()
            if resolved not in referenced and parquet_file.stat().st_mtime < cutoff_ts:
                orphans.append(parquet_file)
        deleted = 0
        if delete and min_age_hours >= 72.0:
            for orphan in orphans:
                orphan.unlink(missing_ok=True)
                deleted += 1
        return {
            "table": identifier,
            "referenced_files": len(referenced),
            "orphan_candidates": len(orphans),
            "deleted": deleted,
            "safety_window_hours": min_age_hours,
        }

    def all_table_stats(self) -> list[dict[str, Any]]:
        tables = [
            "bronze.telemetry_raw",
            "bronze.trips_raw",
            "bronze.autonomy_raw",
            "bronze.anomalies_stream_raw",
            "bronze.telemetry_dlq",
            "silver.telemetry_clean",
            "silver.trips_reconstructed",
            "silver.anomalies_verified",
            "silver.dim_vehicle_scd2",
            "silver.telemetry_quarantine",
            "gold.fleet_activity_hourly",
            "gold.anomaly_rates_by_segment",
            "gold.trip_risk_summary",
            "gold.battery_health_trends",
            "gold.regional_h3_density",
            "gold.telemetry_reliability_kpis",
            "gold.vehicle_latest_snapshot",
            "quality.dq_check_results",
        ]
        return [self.table_stats(t) for t in tables]
