"""Centralized Prometheus metric definitions across all platform layers."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class PlatformMetrics:
    """Thread-safe Prometheus metric registry for simulator, streaming, ingestion, lakehouse, and API."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        # Producer / Simulator
        self.events_produced_total = Counter(
            "fleet_events_produced_total",
            "Total synthetic telemetry events produced",
            ["topic", "event_type"],
            registry=self.registry,
        )
        self.producer_bytes_total = Counter(
            "fleet_producer_bytes_total",
            "Total serialized Avro bytes produced",
            ["topic"],
            registry=self.registry,
        )
        self.producer_errors_total = Counter(
            "fleet_producer_errors_total",
            "Total producer send failures or retries",
            ["topic", "reason"],
            registry=self.registry,
        )

        # Ingestion & DLQ
        self.events_ingested_bronze_total = Counter(
            "fleet_bronze_events_ingested_total",
            "Total events committed to Iceberg Bronze tables",
            ["table"],
            registry=self.registry,
        )
        self.dlq_messages_total = Counter(
            "fleet_dlq_messages_total",
            "Total invalid or poison messages quarantined to DLQ",
            ["topic", "error_code"],
            registry=self.registry,
        )
        self.consumer_lag_records = Gauge(
            "fleet_consumer_lag_records",
            "Current consumer lag in records per consumer group and topic",
            ["consumer_group", "topic"],
            registry=self.registry,
        )

        # Streaming (Flink / Stateful Engine)
        self.stream_events_processed_total = Counter(
            "fleet_stream_events_processed_total",
            "Events processed by the stateful stream engine",
            ["status"],
            registry=self.registry,
        )
        self.stream_anomalies_detected_total = Counter(
            "fleet_anomalies_detected_total",
            "Anomalies emitted by streaming rule engine",
            ["rule_id", "severity"],
            registry=self.registry,
        )
        self.stream_watermark_lag_seconds = Gauge(
            "fleet_stream_watermark_lag_seconds",
            "Difference between wall clock and current event-time watermark",
            registry=self.registry,
        )
        self.stream_state_keys = Gauge(
            "fleet_stream_state_keys",
            "Active keyed state entries held by streaming engine",
            ["state_store"],
            registry=self.registry,
        )
        self.stream_checkpoint_duration_seconds = Histogram(
            "fleet_stream_checkpoint_duration_seconds",
            "Checkpoint serialization and flush latency",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            registry=self.registry,
        )
        self.stream_e2e_latency_seconds = Histogram(
            "fleet_e2e_processing_latency_seconds",
            "End-to-end event processing latency from event_time to detection_time",
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0),
            registry=self.registry,
        )

        # Lakehouse & Batch
        self.batch_job_duration_seconds = Histogram(
            "fleet_batch_job_duration_seconds",
            "Batch curation or maintenance job runtime in seconds",
            ["job_name"],
            registry=self.registry,
        )
        self.iceberg_table_files = Gauge(
            "fleet_iceberg_table_data_files",
            "Number of active Parquet data files in an Iceberg table",
            ["table"],
            registry=self.registry,
        )
        self.iceberg_table_snapshots = Gauge(
            "fleet_iceberg_table_snapshots",
            "Number of retained snapshots in an Iceberg table",
            ["table"],
            registry=self.registry,
        )
        self.data_freshness_seconds = Gauge(
            "fleet_data_freshness_seconds",
            "Seconds elapsed since the latest event_time committed to Gold",
            ["dataset"],
            registry=self.registry,
        )

        # Data Quality
        self.dq_checks_total = Counter(
            "fleet_dq_checks_total",
            "Data quality check outcomes",
            ["check_name", "status"],
            registry=self.registry,
        )

        # Serving API
        self.api_requests_total = Counter(
            "fleet_api_requests_total",
            "HTTP requests served by FastAPI",
            ["endpoint", "method", "status_code", "role"],
            registry=self.registry,
        )
        self.api_request_duration_seconds = Histogram(
            "fleet_api_request_duration_seconds",
            "API request latency in seconds",
            ["endpoint"],
            buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
            registry=self.registry,
        )

    def render_prometheus(self) -> bytes:
        return generate_latest(self.registry)


_DEFAULT_METRICS: PlatformMetrics | None = None


def get_metrics() -> PlatformMetrics:
    global _DEFAULT_METRICS
    if _DEFAULT_METRICS is None:
        _DEFAULT_METRICS = PlatformMetrics()
    return _DEFAULT_METRICS
