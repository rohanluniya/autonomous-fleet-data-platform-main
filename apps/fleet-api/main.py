"""Autonomous Fleet Telemetry & Driving Intelligence FastAPI Serving Layer.

Exposes all operational, analytical, geospatial, data-quality, and platform telemetry
endpoints with pagination, bounded queries, short-TTL response caching, RBAC spatial
privacy enforcement, Prometheus metrics, OpenTelemetry spans, and static dashboard hosting.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import psutil
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_API_DIR = Path(__file__).resolve().parent
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from security import Principal, apply_location_privacy, resolve_principal

from fleet_platform.config import PlatformConfig, load_config
from fleet_platform.geospatial.spatial import (
    METRO_REGIONS,
    export_geofences_geojson,
    h3_cell_boundary_geojson,
)
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import (
    OpenLineageEmitter,
    get_logger,
    get_metrics,
    get_tracer,
    new_trace_context,
)
from fleet_platform.transport.factory import create_event_bus


class _TTLCache:
    def __init__(self, ttl_seconds: float = 5.0) -> None:
        self.ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}

    def get_or_set(self, key: str, loader: Callable[[], Any]) -> Any:
        now = time.monotonic()
        hit = self._store.get(key)
        if hit is not None and (now - hit[0]) < self.ttl:
            return hit[1]
        val = loader()
        self._store[key] = (now, val)
        return val

    def clear(self) -> None:
        self._store.clear()


def _serialize_val(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.replace(tzinfo=UTC).isoformat() if obj.tzinfo is None else obj.isoformat()
    if isinstance(obj, list):
        return [_serialize_val(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _serialize_val(v) for k, v in obj.items()}
    return obj


def _serialize_dt(obj: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], _serialize_val(obj))


def create_app(config: PlatformConfig | None = None) -> FastAPI:
    cfg = config or load_config()
    app = FastAPI(
        title="Autonomous Fleet Telemetry & Driving Intelligence API",
        version="0.1.0",
        description=(
            "Open-source operational & analytical API backed by Apache Iceberg lakehouse tables. "
            "All vehicle identities and telemetry are synthetic."
        ),
    )
    app.state.config = cfg
    app.state.lakehouse = IcebergLakehouse(cfg)
    app.state.bus = create_event_bus(cfg)
    app.state.lineage = OpenLineageEmitter(
        namespace=cfg.openlineage_namespace,
        marquez_url=cfg.openlineage_url,
        local_store_dir=cfg.lineage_dir,
    )
    app.state.cache = _TTLCache(ttl_seconds=5.0)
    app.state.logger = get_logger("fleet-api", "http", environment=cfg.env)
    app.state.metrics = get_metrics()
    app.state.tracer = get_tracer("fleet-api")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    dashboard_dir = _ROOT / "apps" / "fleet-dashboard"
    if (dashboard_dir / "assets").exists():
        app.mount("/assets", StaticFiles(directory=str(dashboard_dir / "assets")), name="assets")

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next: Any) -> Response:
        t0 = time.perf_counter()
        trace_id, corr_id = new_trace_context()
        request.state.trace_id = trace_id
        request.state.correlation_id = corr_id
        with app.state.tracer.start_as_current_span(f"{request.method} {request.url.path}"):
            response: Response = await call_next(request)
        dur = time.perf_counter() - t0
        endpoint = request.url.path
        app.state.metrics.api_requests_total.labels(
            endpoint=endpoint,
            method=request.method,
            status_code=str(response.status_code),
            role=getattr(request.state, "role", "anonymous"),
        ).inc()
        app.state.metrics.api_request_duration_seconds.labels(endpoint=endpoint).observe(dur)
        response.headers["X-Trace-Id"] = trace_id
        response.headers["X-Correlation-Id"] = corr_id
        response.headers["X-Simulated-Data"] = "true"
        return response

    def _lh() -> IcebergLakehouse:
        return cast(IcebergLakehouse, app.state.lakehouse)

    @app.get("/", include_in_schema=False)
    def serve_dashboard() -> Response:
        index_file = dashboard_dir / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return PlainTextResponse("Autonomous Fleet API is running. See /docs.")

    @app.get("/health")
    def health() -> dict[str, Any]:
        stats = _lh().table_stats("gold.vehicle_latest_snapshot")
        latest_row = _lh().query_sql(
            "SELECT MAX(last_event_time) AS latest_ev, MAX(last_ingest_time) AS latest_ing FROM gold_vehicle_latest"
        )[0]
        return _serialize_dt(
            {
                "status": "ok",
                "service": "fleet-api",
                "environment": cfg.env,
                "execution_profile": cfg.execution_profile,
                "transport_backend": app.state.bus.backend_name,
                "sql_engine": "duckdb-2.0.0-over-iceberg"
                if cfg.execution_profile == "local_native"
                else "trino-484",
                "synthetic_data": True,
                "disclaimer": "All fleet identities, telemetry, and autonomy disengagement events are synthetic.",
                "gold_vehicles_indexed": stats["total_records"],
                "latest_event_time_utc": latest_row["latest_ev"],
                "latest_ingest_time_utc": latest_row["latest_ing"],
                "server_time_utc": datetime.now(UTC).isoformat(),
            }
        )

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> Response:
        return Response(
            content=app.state.metrics.render_prometheus(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/api/map/basemap.geojson")
    def self_hosted_basemap() -> dict[str, Any]:
        """Self-hosted synthetic vector basemap (metro bounds, geofences, routes) — zero external tile calls."""
        base = export_geofences_geojson()
        for reg in METRO_REGIONS.values():
            min_lat, min_lon, max_lat, max_lon = reg.bbox
            base["features"].append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "metro_boundary",
                        "region": reg.region_id,
                        "name": reg.display_name,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [min_lon, min_lat],
                                [max_lon, min_lat],
                                [max_lon, max_lat],
                                [min_lon, max_lat],
                                [min_lon, min_lat],
                            ]
                        ],
                    },
                }
            )
        return base

    @app.get("/fleet/summary")
    def fleet_summary(
        request: Request,
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role

        def _load() -> dict[str, Any]:
            lh = _lh()
            overview = lh.query_sql(
                """
                SELECT
                    COUNT(*) AS total_vehicles,
                    SUM(CASE WHEN vehicle_state = 'driving' THEN 1 ELSE 0 END) AS driving_vehicles,
                    SUM(CASE WHEN vehicle_state = 'parked' THEN 1 ELSE 0 END) AS parked_vehicles,
                    SUM(CASE WHEN vehicle_state = 'charging' THEN 1 ELSE 0 END) AS charging_vehicles,
                    SUM(CASE WHEN vehicle_state = 'offline' THEN 1 ELSE 0 END) AS offline_vehicles,
                    SUM(CASE WHEN vehicle_state = 'maintenance' THEN 1 ELSE 0 END) AS maintenance_vehicles,
                    SUM(CASE WHEN connectivity_status = 'TELEMETRY_GAP' THEN 1 ELSE 0 END) AS telemetry_gap_vehicles,
                    ROUND(AVG(battery_soc_pct), 2) AS avg_battery_soc_pct,
                    ROUND(AVG(sensor_health_score), 4) AS avg_sensor_health,
                    MAX(last_event_time) AS latest_event_time,
                    MAX(last_ingest_time) AS latest_processing_time
                FROM gold_vehicle_latest
                """
            )[0]
            by_sw = lh.query_sql(
                """
                SELECT
                    a.software_version,
                    SUM(a.active_vehicles) AS active_vehicles,
                    SUM(a.total_events) AS total_events,
                    ROUND(AVG(a.avg_speed_mps), 2) AS avg_speed_mps,
                    ROUND(AVG(a.avg_battery_soc_pct), 2) AS avg_battery_soc_pct,
                    COALESCE((SELECT SUM(r.anomaly_count) FROM gold_anomaly_rates r WHERE r.software_version = a.software_version), 0) AS total_anomalies
                FROM gold_fleet_activity a
                GROUP BY a.software_version
                ORDER BY a.software_version
                """
            )
            for row in by_sw:
                ev_cnt = max(1, int(row["total_events"] or 0))
                row["anomalies_per_1k_events"] = round(
                    (int(row["total_anomalies"] or 0) / ev_cnt) * 1000.0, 3
                )
            by_rule = lh.query_sql(
                """
                SELECT rule_id, severity, SUM(anomaly_count) AS count, SUM(affected_vehicles) AS vehicles
                FROM gold_anomaly_rates
                GROUP BY rule_id, severity
                ORDER BY count DESC
                """
            )
            reliability = lh.query_sql("SELECT * FROM gold_reliability ORDER BY region")
            battery = lh.query_sql(
                """
                SELECT region, vehicle_model, software_version,
                       avg_battery_soc_pct, min_battery_soc_pct,
                       avg_battery_temp_c, max_battery_temp_c,
                       thermal_alert_events
                FROM gold_battery_health
                ORDER BY region, vehicle_model, software_version
                """
            )
            timeline = lh.query_sql(
                """
                SELECT
                    DATE_TRUNC('minute', event_time) AS minute_bucket,
                    COUNT(*) AS anomaly_count,
                    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
                    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_count
                FROM silver_anomalies
                GROUP BY 1
                ORDER BY 1
                """
            )
            trip_tiers = lh.query_sql(
                """
                SELECT risk_tier, COUNT(*) AS trip_count, ROUND(AVG(risk_score), 2) AS avg_risk_score
                FROM gold_trip_risk
                GROUP BY risk_tier
                ORDER BY avg_risk_score DESC
                """
            )
            return {
                "synthetic_data": True,
                "execution_profile": cfg.execution_profile,
                "role": principal.role,
                "overview": overview,
                "software_version_comparison": by_sw,
                "anomalies_by_rule": by_rule,
                "anomaly_timeline": timeline,
                "trip_risk_distribution": trip_tiers,
                "reliability_by_region": reliability,
                "battery_health": battery,
                "queried_at_utc": datetime.now(UTC).isoformat(),
            }

        return _serialize_dt(app.state.cache.get_or_set(f"summary:{principal.role}", _load))

    @app.get("/fleet/vehicles")
    def list_vehicles(
        request: Request,
        region: str | None = Query(default=None),
        state: str | None = Query(default=None),
        connectivity_status: str | None = Query(default=None),
        software_version: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        clauses = ["1=1"]
        params: list[Any] = []
        if region:
            clauses.append("region = ?")
            params.append(region)
        if state:
            clauses.append("vehicle_state = ?")
            params.append(state)
        if connectivity_status:
            clauses.append("connectivity_status = ?")
            params.append(connectivity_status)
        if software_version:
            clauses.append("software_version = ?")
            params.append(software_version)
        where_sql = " AND ".join(clauses)

        total = _lh().query_sql(
            f"SELECT COUNT(*) AS cnt FROM gold_vehicle_latest WHERE {where_sql}",
            params=params,
        )[0]["cnt"]
        rows = _lh().query_sql(
            f"""
            SELECT * FROM gold_vehicle_latest
            WHERE {where_sql}
            ORDER BY total_anomalies DESC, vehicle_id ASC
            LIMIT ? OFFSET ?
            """,
            params=[*params, limit, offset],
        )
        secured_rows = [
            apply_location_privacy(
                r,
                principal,
                coarsen_res=cfg.public_coarsen_h3_res,
                correlation_id=request.state.correlation_id,
            )
            for r in rows
        ]
        return _serialize_dt(
            {
                "synthetic_data": True,
                "role": principal.role,
                "location_policy": (
                    "raw_gps_authorized"
                    if principal.can_view_raw_gps
                    else f"coarsened_h3_res{cfg.public_coarsen_h3_res}"
                ),
                "total": int(total),
                "limit": limit,
                "offset": offset,
                "items": secured_rows,
            }
        )

    @app.get("/fleet/vehicles/{vehicle_id}")
    def get_vehicle_detail(
        vehicle_id: str,
        request: Request,
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        rows = _lh().query_sql(
            "SELECT * FROM gold_vehicle_latest WHERE vehicle_id = ?",
            params=[vehicle_id],
        )
        if not rows:
            raise HTTPException(status_code=404, detail=f"Vehicle '{vehicle_id}' not found")
        vehicle = apply_location_privacy(
            rows[0],
            principal,
            coarsen_res=cfg.public_coarsen_h3_res,
            correlation_id=request.state.correlation_id,
        )
        scd2_history = _lh().query_sql(
            "SELECT * FROM silver_dim_vehicle WHERE vehicle_id = ? ORDER BY effective_from DESC",
            params=[vehicle_id],
        )
        anomalies = _lh().query_sql(
            "SELECT * FROM silver_anomalies WHERE vehicle_id = ? ORDER BY event_time DESC LIMIT 50",
            params=[vehicle_id],
        )
        trips = _lh().query_sql(
            "SELECT * FROM gold_trip_risk WHERE vehicle_id = ? ORDER BY start_time DESC LIMIT 20",
            params=[vehicle_id],
        )
        return _serialize_dt(
            {
                "synthetic_data": True,
                "vehicle": vehicle,
                "scd2_dimension_history": scd2_history,
                "recent_anomalies": anomalies,
                "recent_trips": trips,
            }
        )

    @app.get("/fleet/anomalies")
    def list_anomalies(
        request: Request,
        severity: str | None = Query(default=None),
        rule_id: str | None = Query(default=None),
        region: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        clauses = ["1=1"]
        params: list[Any] = []
        for col, val in (
            ("severity", severity),
            ("rule_id", rule_id),
            ("region", region),
            ("vehicle_id", vehicle_id),
        ):
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        where_sql = " AND ".join(clauses)
        total = _lh().query_sql(
            f"SELECT COUNT(*) AS cnt FROM silver_anomalies WHERE {where_sql}", params=params
        )[0]["cnt"]
        rows = _lh().query_sql(
            f"""
            SELECT * FROM silver_anomalies
            WHERE {where_sql}
            ORDER BY event_time DESC
            LIMIT ? OFFSET ?
            """,
            params=[*params, limit, offset],
        )
        parsed = []
        for r in rows:
            item = apply_location_privacy(
                r,
                principal,
                coarsen_res=cfg.public_coarsen_h3_res,
                correlation_id=request.state.correlation_id,
            )
            try:
                item["evidence"] = json.loads(item.get("evidence_json") or "{}")
            except Exception:
                item["evidence"] = {}
            item["source_event_ids"] = [
                s for s in (item.get("source_event_ids_csv") or "").split(",") if s
            ]
            parsed.append(item)
        return _serialize_dt(
            {
                "synthetic_data": True,
                "total": int(total),
                "limit": limit,
                "offset": offset,
                "items": parsed,
            }
        )

    @app.get("/fleet/trips")
    def list_trips(
        request: Request,
        risk_tier: str | None = Query(default=None),
        region: str | None = Query(default=None),
        software_version: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        clauses = ["1=1"]
        params: list[Any] = []
        for col, val in (
            ("risk_tier", risk_tier),
            ("region", region),
            ("software_version", software_version),
        ):
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        where_sql = " AND ".join(clauses)
        total = _lh().query_sql(
            f"SELECT COUNT(*) AS cnt FROM gold_trip_risk WHERE {where_sql}", params=params
        )[0]["cnt"]
        rows = _lh().query_sql(
            f"""
            SELECT * FROM gold_trip_risk
            WHERE {where_sql}
            ORDER BY risk_score DESC, anomaly_count DESC
            LIMIT ? OFFSET ?
            """,
            params=[*params, limit, offset],
        )
        return _serialize_dt(
            {
                "synthetic_data": True,
                "total": int(total),
                "limit": limit,
                "offset": offset,
                "items": rows,
            }
        )

    @app.get("/fleet/regions")
    def regional_intelligence(
        request: Request,
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        lh = _lh()
        regional_summary = lh.query_sql(
            """
            SELECT
                region,
                COUNT(*) AS total_vehicles,
                SUM(CASE WHEN vehicle_state = 'driving' THEN 1 ELSE 0 END) AS driving_vehicles,
                SUM(CASE WHEN connectivity_status = 'TELEMETRY_GAP' THEN 1 ELSE 0 END) AS telemetry_gap_vehicles,
                SUM(total_events) AS total_events,
                SUM(total_anomalies) AS total_anomalies,
                ROUND(AVG(speed_mps), 2) AS avg_speed_mps,
                ROUND(AVG(battery_soc_pct), 2) AS avg_battery_soc_pct
            FROM gold_vehicle_latest
            GROUP BY region
            ORDER BY region
            """
        )
        h3_cells = lh.query_sql(
            """
            SELECT region, h3_cell_res6, center_lat, center_lon,
                   SUM(active_vehicles) AS active_vehicles,
                   SUM(event_count) AS event_count,
                   SUM(anomaly_count) AS anomaly_count,
                   ROUND(AVG(avg_speed_mps), 2) AS avg_speed_mps
            FROM gold_regional_h3
            GROUP BY region, h3_cell_res6, center_lat, center_lon
            ORDER BY anomaly_count DESC, active_vehicles DESC
            """
        )
        h3_features = []
        for c in h3_cells:
            ring = h3_cell_boundary_geojson(c["h3_cell_res6"])
            h3_features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "region": c["region"],
                        "h3_cell_res6": c["h3_cell_res6"],
                        "active_vehicles": int(c["active_vehicles"]),
                        "event_count": int(c["event_count"]),
                        "anomaly_count": int(c["anomaly_count"]),
                        "avg_speed_mps": float(c["avg_speed_mps"]),
                        "center_lat": float(c["center_lat"]),
                        "center_lon": float(c["center_lon"]),
                    },
                    "geometry": {"type": "Polygon", "coordinates": [ring]},
                }
            )
        return _serialize_dt(
            {
                "synthetic_data": True,
                "privacy_note": "Regional map polygons are aggregated at H3 resolution 6 (~36 km^2).",
                "regions": regional_summary,
                "h3_geojson": {"type": "FeatureCollection", "features": h3_features},
            }
        )

    @app.get("/fleet/data-quality")
    def data_quality_status(
        request: Request,
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        lh = _lh()
        latest_run = lh.query_sql(
            "SELECT run_id, MAX(executed_at) AS executed_at FROM quality_dq_results GROUP BY run_id ORDER BY executed_at DESC LIMIT 1"
        )
        if not latest_run:
            return {"status": "NO_RUNS", "checks": [], "dlq_count": 0, "quarantine_count": 0}
        run_id = latest_run[0]["run_id"]
        checks = lh.query_sql(
            "SELECT * FROM quality_dq_results WHERE run_id = ? ORDER BY layer, check_name",
            params=[run_id],
        )
        for c in checks:
            try:
                c["details"] = json.loads(c.get("details_json") or "{}")
            except Exception:
                c["details"] = {}
        dlq_cnt = lh.query_sql("SELECT COUNT(*) AS cnt FROM bronze_dlq")[0]["cnt"]
        quar_cnt = lh.query_sql("SELECT COUNT(*) AS cnt FROM silver_quarantine")[0]["cnt"]
        passed = sum(1 for c in checks if c["status"] == "PASS")
        warned = sum(1 for c in checks if c["status"] == "WARN")
        failed = sum(1 for c in checks if c["status"] == "FAIL")
        return _serialize_dt(
            {
                "run_id": run_id,
                "executed_at": latest_run[0]["executed_at"],
                "overall_status": "FAIL" if failed else ("WARN" if warned else "PASS"),
                "summary": {
                    "passed": passed,
                    "warned": warned,
                    "failed": failed,
                    "total": len(checks),
                },
                "dlq_count": int(dlq_cnt),
                "quarantine_count": int(quar_cnt),
                "checks": checks,
            }
        )

    @app.get("/platform/metrics")
    def platform_metrics_endpoint(
        request: Request,
        principal: Principal = Depends(resolve_principal),
    ) -> dict[str, Any]:
        request.state.role = principal.role
        lh = _lh()
        bus = app.state.bus
        mem = psutil.virtual_memory()
        topic_stats = bus.topic_stats()
        lags = {
            "bronze_ingestor": bus.get_consumer_lag("fleet.telemetry.raw", "fleet-bronze-ingestor")[
                "total_lag"
            ],
            "stream_processor": bus.get_consumer_lag(
                "fleet.telemetry.raw", "fleet-flink-stream-processor"
            )["total_lag"],
        }
        lineage_events = app.state.lineage.read_local_events(limit=25)
        return _serialize_dt(
            {
                "execution_profile": cfg.execution_profile,
                "transport_backend": bus.backend_name,
                "host_resources": {
                    "cpu_percent": psutil.cpu_percent(interval=0.05),
                    "memory_used_percent": mem.percent,
                    "memory_total_gib": round(mem.total / (1024**3), 2),
                },
                "consumer_lag_records": lags,
                "topics": topic_stats,
                "iceberg_tables": lh.all_table_stats(),
                "recent_openlineage_runs": [
                    {
                        "event_type": e["eventType"],
                        "event_time": e["eventTime"],
                        "job_name": e["job"]["name"],
                        "run_id": e["run"]["runId"],
                        "inputs": [i["name"] for i in e.get("inputs", [])],
                        "outputs": [o["name"] for o in e.get("outputs", [])],
                    }
                    for e in lineage_events[-12:]
                ],
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    cfg = load_config()
    uvicorn.run(app, host=cfg.api_host, port=cfg.api_port)
