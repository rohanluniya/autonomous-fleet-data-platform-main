"""Medallion Batch Lakehouse Curation (Bronze -> Silver -> Gold), Idempotent Backfills,
and Iceberg Maintenance.

Executes directly on real Apache Iceberg Format V2 tables using PyIceberg + PyArrow + DuckDB
in `local_native` mode, maintaining exact semantic parity with the PySpark 4.1.4 jobs in
`pipelines/spark/`.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from fleet_platform.config import PlatformConfig
from fleet_platform.geospatial.spatial import (
    h3_cell_center,
    haversine_km,
    match_geofences,
    route_deviation_meters,
    to_h3_cell,
    validate_gps,
)
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import OpenLineageEmitter, get_logger, get_metrics


def _is_valid_telemetry_row(row: dict[str, Any]) -> tuple[bool, str]:
    lat = row.get("latitude")
    lon = row.get("longitude")
    if lat is not None and lon is not None and not validate_gps(float(lat), float(lon)):
        return False, "INVALID_GPS_BOUNDS"
    speed = float(row["speed_mps"])
    if not (0.0 <= speed <= 55.0):
        return False, "IMPLAUSIBLE_SPEED"
    accel = float(row["acceleration_mps2"])
    if not (-15.0 <= accel <= 12.0):
        return False, "IMPLAUSIBLE_ACCELERATION"
    soc = float(row["battery_soc_pct"])
    if not (0.0 <= soc <= 100.0):
        return False, "INVALID_BATTERY_SOC"
    health = float(row["sensor_health_score"])
    if not (0.0 <= health <= 1.0):
        return False, "INVALID_SENSOR_HEALTH"
    return True, "OK"


class MedallionBatchPipeline:
    """Bronze -> Silver -> Gold curation, reconciliation, backfill, and Iceberg maintenance."""

    def __init__(
        self,
        config: PlatformConfig,
        lakehouse: IcebergLakehouse,
        lineage: OpenLineageEmitter | None = None,
    ) -> None:
        self.config = config
        self.lakehouse = lakehouse
        self.lineage = lineage or OpenLineageEmitter(
            namespace=config.openlineage_namespace,
            marquez_url=config.openlineage_url,
            local_store_dir=config.lineage_dir,
        )
        self.logger = get_logger("batch-curation", "medallion_pipeline", environment=config.env)
        self.metrics = get_metrics()

    def _lookup_vehicle_sk(
        self,
        dim_index: dict[str, list[dict[str, Any]]],
        vehicle_id: str,
        event_time: datetime,
    ) -> str | None:
        versions = dim_index.get(vehicle_id)
        if not versions:
            return None
        for v in versions:
            eff_from = v["effective_from"]
            eff_to = v["effective_to"]
            if eff_from <= event_time and (eff_to is None or event_time < eff_to):
                return str(v["vehicle_sk"])
        return str(versions[-1]["vehicle_sk"])

    def run_bronze_to_silver(self, *, target_date: str | None = None) -> dict[str, Any]:
        """Deduplicate Bronze telemetry/anomalies, enrich with H3/geofences/SCD2,
        quarantine invalid records, and reconstruct trips.
        """
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        self.lineage.emit(
            event_type="START",
            job_name="bronze_to_silver_curation",
            run_id=run_id,
            inputs=[
                {"name": "bronze.telemetry_raw"},
                {"name": "bronze.trips_raw"},
                {"name": "bronze.autonomy_raw"},
                {"name": "bronze.anomalies_stream_raw"},
                {"name": "silver.dim_vehicle_scd2"},
            ],
            outputs=[
                {"name": "silver.telemetry_clean"},
                {"name": "silver.trips_reconstructed"},
                {"name": "silver.anomalies_verified"},
                {"name": "silver.telemetry_quarantine"},
            ],
        )

        # Build point-in-time SCD2 lookup index
        dim_rows = self.lakehouse.read_arrow("silver.dim_vehicle_scd2").to_pylist()
        dim_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for d in dim_rows:
            dim_index[str(d["vehicle_id"])].append(d)
        for vlist in dim_index.values():
            vlist.sort(key=lambda r: r["effective_from"])

        # 1. Canonical deduplication of bronze.telemetry_raw by event_id (earliest ingest_time wins)
        raw_tel = self.lakehouse.read_arrow("bronze.telemetry_raw").to_pylist()
        if target_date is not None:
            raw_tel = [r for r in raw_tel if r["event_time"].strftime("%Y-%m-%d") == target_date]

        dedup_map: dict[str, dict[str, Any]] = {}
        for row in raw_tel:
            eid = str(row["event_id"])
            prev = dedup_map.get(eid)
            if prev is None or row["ingest_time"] < prev["ingest_time"]:
                dedup_map[eid] = row
        duplicates_removed = len(raw_tel) - len(dedup_map)

        clean_rows: list[dict[str, Any]] = []
        quarantine_rows: list[dict[str, Any]] = []
        now_naive = datetime.now(UTC).replace(tzinfo=None)

        for row in dedup_map.values():
            ok, reason = _is_valid_telemetry_row(row)
            if not ok:
                quarantine_rows.append(
                    {
                        "event_id": str(row["event_id"]),
                        "vehicle_id": str(row["vehicle_id"]),
                        "event_time": row["event_time"],
                        "quarantine_reason": reason,
                        "quarantined_at": now_naive,
                        "raw_json": json.dumps(row, default=str),
                    }
                )
                continue

            lat = row.get("latitude")
            lon = row.get("longitude")
            h3_6 = (
                to_h3_cell(lat, lon, resolution=6) if lat is not None and lon is not None else None
            )
            h3_9 = (
                to_h3_cell(lat, lon, resolution=9) if lat is not None and lon is not None else None
            )
            fences = (
                match_geofences(str(row["region"]), float(lat), float(lon))
                if lat is not None and lon is not None
                else []
            )
            meta = {}
            if row.get("metadata_json"):
                try:
                    meta = json.loads(row["metadata_json"])
                except Exception:
                    meta = {}
            route_id = meta.get("route_id", "")
            dev_m = (
                route_deviation_meters(str(row["region"]), route_id, float(lat), float(lon))
                if route_id and lat is not None and lon is not None
                else None
            )
            ingest_lag_s = max(0.0, (row["ingest_time"] - row["event_time"]).total_seconds())
            # Mark events with delayed flush, ingest lag > 15s, or clock skew > 25s as late arrivals
            is_late = bool(
                meta.get("delayed_flush") == "true"
                or abs(int(meta.get("clock_skew_s", "0"))) > 25
                or ingest_lag_s > 15.0
            )
            vsk = self._lookup_vehicle_sk(dim_index, str(row["vehicle_id"]), row["event_time"])

            clean_rows.append(
                {
                    "event_id": str(row["event_id"]),
                    "vehicle_id": str(row["vehicle_id"]),
                    "trip_id": row.get("trip_id"),
                    "event_time": row["event_time"],
                    "ingest_time": row["ingest_time"],
                    "event_date": row["event_time"].strftime("%Y-%m-%d"),
                    "sequence_number": int(row["sequence_number"]),
                    "vehicle_state": str(row["vehicle_state"]),
                    "region": str(row["region"]),
                    "vehicle_model": str(row["vehicle_model"]),
                    "software_version": str(row["software_version"]),
                    "latitude": lat,
                    "longitude": lon,
                    "heading_deg": row.get("heading_deg"),
                    "h3_cell_res6": h3_6,
                    "h3_cell_res9": h3_9,
                    "geofences_csv": ",".join(fences),
                    "route_deviation_m": round(dev_m, 2) if dev_m is not None else None,
                    "speed_mps": float(row["speed_mps"]),
                    "acceleration_mps2": float(row["acceleration_mps2"]),
                    "braking_force_pct": float(row["braking_force_pct"]),
                    "battery_soc_pct": float(row["battery_soc_pct"]),
                    "battery_temp_c": float(row["battery_temp_c"]),
                    "motor_temp_c": float(row["motor_temp_c"]),
                    "odometer_km": float(row["odometer_km"]),
                    "sensor_health_score": float(row["sensor_health_score"]),
                    "fault_codes_csv": row.get("fault_codes_csv") or "",
                    "ingest_lag_seconds": round(ingest_lag_s, 3),
                    "is_late_arrival": is_late,
                    "vehicle_sk": vsk,
                }
            )

        # Merge cleanly if target_date backfill is scoped to one day
        if target_date is not None:
            existing_silver = [
                r
                for r in self.lakehouse.read_arrow("silver.telemetry_clean").to_pylist()
                if r["event_date"] != target_date
            ]
            clean_rows = existing_silver + clean_rows

        tbl_clean = self.lakehouse.load_table("silver.telemetry_clean")
        self.lakehouse.overwrite_arrow(
            "silver.telemetry_clean",
            pa.Table.from_pylist(clean_rows, schema=tbl_clean.schema().as_arrow()),
        )

        tbl_quar = self.lakehouse.load_table("silver.telemetry_quarantine")
        self.lakehouse.overwrite_arrow(
            "silver.telemetry_quarantine",
            pa.Table.from_pylist(quarantine_rows, schema=tbl_quar.schema().as_arrow()),
        )

        # 2. Deduplicate streaming anomalies into silver.anomalies_verified
        raw_anoms = self.lakehouse.read_arrow("bronze.anomalies_stream_raw").to_pylist()
        anom_dedup: dict[str, dict[str, Any]] = {}
        for a in raw_anoms:
            aid = str(a["anomaly_id"])
            if aid not in anom_dedup or a["detection_time"] < anom_dedup[aid]["detection_time"]:
                anom_dedup[aid] = a
        verified_anoms = list(anom_dedup.values())
        tbl_anom = self.lakehouse.load_table("silver.anomalies_verified")
        self.lakehouse.overwrite_arrow(
            "silver.anomalies_verified",
            pa.Table.from_pylist(verified_anoms, schema=tbl_anom.schema().as_arrow()),
        )

        # 3. Trip Reconstruction into silver.trips_reconstructed
        anom_by_trip: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for a in verified_anoms:
            if a.get("trip_id"):
                anom_by_trip[str(a["trip_id"])].append(a)

        raw_auto = self.lakehouse.read_arrow("bronze.autonomy_raw").to_pylist()
        diseng_by_trip: dict[str, int] = defaultdict(int)
        seen_auto: set[str] = set()
        for au in raw_auto:
            if au["event_id"] in seen_auto:
                continue
            seen_auto.add(au["event_id"])
            if au.get("trip_id"):
                diseng_by_trip[str(au["trip_id"])] += 1

        tel_by_trip: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in clean_rows:
            if r.get("trip_id"):
                tel_by_trip[str(r["trip_id"])].append(r)

        reconstructed_trips: list[dict[str, Any]] = []
        for tid, rows in tel_by_trip.items():
            rows.sort(key=lambda x: x["event_time"])
            first, last = rows[0], rows[-1]
            dist_km = 0.0
            for prev_r, cur_r in zip(rows, rows[1:], strict=False):
                if prev_r.get("latitude") is not None and cur_r.get("latitude") is not None:
                    dist_km += haversine_km(
                        float(prev_r["latitude"]),
                        float(prev_r["longitude"]),
                        float(cur_r["latitude"]),
                        float(cur_r["longitude"]),
                    )
            odom_delta = max(0.0, float(last["odometer_km"]) - float(first["odometer_km"]))
            effective_dist = max(dist_km, odom_delta)
            duration_s = max(0.0, (last["event_time"] - first["event_time"]).total_seconds())
            speeds = [float(r["speed_mps"]) for r in rows]
            trip_anoms = anom_by_trip.get(tid, [])
            crit_cnt = sum(1 for a in trip_anoms if a["severity"] == "CRITICAL")
            reconstructed_trips.append(
                {
                    "trip_id": tid,
                    "vehicle_id": str(first["vehicle_id"]),
                    "region": str(first["region"]),
                    "vehicle_model": str(first["vehicle_model"]),
                    "software_version": str(last["software_version"]),
                    "start_time": first["event_time"],
                    "end_time": last["event_time"],
                    "duration_seconds": round(duration_s, 2),
                    "distance_km": round(effective_dist, 4),
                    "telemetry_events": len(rows),
                    "avg_speed_mps": round(sum(speeds) / len(speeds), 3),
                    "max_speed_mps": round(max(speeds), 3),
                    "battery_drain_pct": round(
                        max(0.0, float(first["battery_soc_pct"]) - float(last["battery_soc_pct"])),
                        2,
                    ),
                    "anomaly_count": len(trip_anoms),
                    "critical_anomaly_count": crit_cnt,
                    "disengagement_count": diseng_by_trip.get(tid, 0),
                    "start_h3_res6": first.get("h3_cell_res6"),
                    "end_h3_res6": last.get("h3_cell_res6"),
                }
            )

        tbl_trips = self.lakehouse.load_table("silver.trips_reconstructed")
        self.lakehouse.overwrite_arrow(
            "silver.trips_reconstructed",
            pa.Table.from_pylist(reconstructed_trips, schema=tbl_trips.schema().as_arrow()),
        )

        dur = round(time.perf_counter() - t0, 4)
        self.metrics.batch_job_duration_seconds.labels(job_name="bronze_to_silver").observe(dur)
        self.lineage.emit(
            event_type="COMPLETE",
            job_name="bronze_to_silver_curation",
            run_id=run_id,
            inputs=[{"name": "bronze.telemetry_raw"}, {"name": "silver.dim_vehicle_scd2"}],
            outputs=[
                {"name": "silver.telemetry_clean", "row_count": len(clean_rows)},
                {"name": "silver.trips_reconstructed", "row_count": len(reconstructed_trips)},
                {"name": "silver.anomalies_verified", "row_count": len(verified_anoms)},
                {"name": "silver.telemetry_quarantine", "row_count": len(quarantine_rows)},
            ],
        )
        return {
            "run_id": run_id,
            "target_date": target_date,
            "bronze_rows_read": len(raw_tel),
            "duplicates_removed": duplicates_removed,
            "silver_clean_rows": len(clean_rows),
            "quarantined_rows": len(quarantine_rows),
            "trips_reconstructed": len(reconstructed_trips),
            "anomalies_verified": len(verified_anoms),
            "duration_seconds": dur,
        }

    def run_silver_to_gold(self) -> dict[str, Any]:
        """Compute all curated Gold analytical tables atomically from Silver tables."""
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        now_naive = datetime.now(UTC).replace(tzinfo=None)
        self.lineage.emit(
            event_type="START",
            job_name="silver_to_gold_aggregates",
            run_id=run_id,
            inputs=[
                {"name": "silver.telemetry_clean"},
                {"name": "silver.trips_reconstructed"},
                {"name": "silver.anomalies_verified"},
                {"name": "bronze.telemetry_raw"},
                {"name": "silver.telemetry_quarantine"},
            ],
            outputs=[
                {"name": "gold.fleet_activity_hourly"},
                {"name": "gold.anomaly_rates_by_segment"},
                {"name": "gold.trip_risk_summary"},
                {"name": "gold.battery_health_trends"},
                {"name": "gold.regional_h3_density"},
                {"name": "gold.telemetry_reliability_kpis"},
                {"name": "gold.vehicle_latest_snapshot"},
            ],
        )

        clean_rows = self.lakehouse.read_arrow("silver.telemetry_clean").to_pylist()
        trips_rows = self.lakehouse.read_arrow("silver.trips_reconstructed").to_pylist()
        anoms_rows = self.lakehouse.read_arrow("silver.anomalies_verified").to_pylist()
        bronze_rows = self.lakehouse.read_arrow("bronze.telemetry_raw").to_pylist()
        quar_rows = self.lakehouse.read_arrow("silver.telemetry_quarantine").to_pylist()

        # 1. gold.fleet_activity_hourly
        hourly_groups: dict[tuple[datetime, str, str, str], list[dict[str, Any]]] = defaultdict(
            list
        )
        for r in clean_rows:
            hb = r["event_time"].replace(minute=0, second=0, microsecond=0)
            hourly_groups[(hb, r["event_date"], r["region"], r["software_version"])].append(r)

        activity_rows = []
        for (hb, edate, reg, sw), grp in sorted(hourly_groups.items()):
            n = len(grp)
            activity_rows.append(
                {
                    "hour_bucket": hb,
                    "event_date": edate,
                    "region": reg,
                    "software_version": sw,
                    "active_vehicles": len({x["vehicle_id"] for x in grp}),
                    "total_events": n,
                    "avg_speed_mps": round(sum(x["speed_mps"] for x in grp) / n, 3),
                    "avg_battery_soc_pct": round(sum(x["battery_soc_pct"] for x in grp) / n, 2),
                    "avg_sensor_health": round(sum(x["sensor_health_score"] for x in grp) / n, 4),
                    "late_arrival_events": sum(1 for x in grp if x["is_late_arrival"]),
                    "computed_at": now_naive,
                }
            )
        self._overwrite_pylist("gold.fleet_activity_hourly", activity_rows)

        # 2. gold.anomaly_rates_by_segment
        tel_seg_counts: dict[tuple[str, str, str, str], int] = defaultdict(int)
        veh_model_lookup: dict[str, str] = {}
        for r in clean_rows:
            tel_seg_counts[
                (r["event_date"], r["region"], r["vehicle_model"], r["software_version"])
            ] += 1
            veh_model_lookup[r["vehicle_id"]] = r["vehicle_model"]

        anom_groups: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(
            list
        )
        for a in anoms_rows:
            edate = a["event_time"].strftime("%Y-%m-%d")
            vmodel = veh_model_lookup.get(a["vehicle_id"], "UNKNOWN")
            key = (edate, a["region"], vmodel, a["software_version"], a["rule_id"], a["severity"])
            anom_groups[key].append(a)

        anomaly_rate_rows = []
        for (edate, reg, vmodel, sw, rule_id, sev), grp in sorted(anom_groups.items()):
            seg_events = max(1, tel_seg_counts.get((edate, reg, vmodel, sw), len(grp)))
            anomaly_rate_rows.append(
                {
                    "event_date": edate,
                    "region": reg,
                    "vehicle_model": vmodel,
                    "software_version": sw,
                    "rule_id": rule_id,
                    "severity": sev,
                    "anomaly_count": len(grp),
                    "affected_vehicles": len({x["vehicle_id"] for x in grp}),
                    "telemetry_events": seg_events,
                    "anomalies_per_1k_events": round((len(grp) / seg_events) * 1000.0, 3),
                }
            )
        self._overwrite_pylist("gold.anomaly_rates_by_segment", anomaly_rate_rows)

        # 3. gold.trip_risk_summary
        trip_risk_rows = []
        for tr in trips_rows:
            score = round(
                tr["anomaly_count"] * 12.0
                + tr["critical_anomaly_count"] * 25.0
                + tr["disengagement_count"] * 18.0,
                2,
            )
            if score >= 60:
                tier = "HIGH"
            elif score >= 30:
                tier = "ELEVATED"
            elif score > 0:
                tier = "MODERATE"
            else:
                tier = "LOW"
            trip_risk_rows.append(
                {
                    "trip_id": tr["trip_id"],
                    "vehicle_id": tr["vehicle_id"],
                    "region": tr["region"],
                    "software_version": tr["software_version"],
                    "start_time": tr["start_time"],
                    "end_time": tr["end_time"],
                    "distance_km": tr["distance_km"],
                    "duration_seconds": tr["duration_seconds"],
                    "anomaly_count": tr["anomaly_count"],
                    "critical_anomaly_count": tr["critical_anomaly_count"],
                    "disengagement_count": tr["disengagement_count"],
                    "risk_score": score,
                    "risk_tier": tier,
                }
            )
        self._overwrite_pylist("gold.trip_risk_summary", trip_risk_rows)

        # 4. gold.battery_health_trends
        batt_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in clean_rows:
            batt_groups[
                (r["event_date"], r["region"], r["vehicle_model"], r["software_version"])
            ].append(r)

        batt_rows = []
        for (edate, reg, vmodel, sw), grp in sorted(batt_groups.items()):
            n = len(grp)
            batt_rows.append(
                {
                    "event_date": edate,
                    "region": reg,
                    "vehicle_model": vmodel,
                    "software_version": sw,
                    "avg_battery_soc_pct": round(sum(x["battery_soc_pct"] for x in grp) / n, 2),
                    "min_battery_soc_pct": round(min(x["battery_soc_pct"] for x in grp), 2),
                    "avg_battery_temp_c": round(sum(x["battery_temp_c"] for x in grp) / n, 2),
                    "max_battery_temp_c": round(max(x["battery_temp_c"] for x in grp), 2),
                    "avg_motor_temp_c": round(sum(x["motor_temp_c"] for x in grp) / n, 2),
                    "low_battery_events": sum(1 for x in grp if x["battery_soc_pct"] < 15.0),
                    "thermal_alert_events": sum(1 for x in grp if x["battery_temp_c"] >= 60.0),
                }
            )
        self._overwrite_pylist("gold.battery_health_trends", batt_rows)

        # 5. gold.regional_h3_density
        anom_by_h3: dict[tuple[str, str, str], int] = defaultdict(int)
        for a in anoms_rows:
            if a.get("h3_cell_res6"):
                anom_by_h3[
                    (a["event_time"].strftime("%Y-%m-%d"), a["region"], a["h3_cell_res6"])
                ] += 1

        h3_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in clean_rows:
            if r.get("h3_cell_res6"):
                h3_groups[(r["event_date"], r["region"], r["h3_cell_res6"])].append(r)

        h3_rows = []
        for (edate, reg, cell), grp in sorted(h3_groups.items()):
            clat, clon = h3_cell_center(cell)
            h3_rows.append(
                {
                    "event_date": edate,
                    "region": reg,
                    "h3_cell_res6": cell,
                    "center_lat": round(clat, 5),
                    "center_lon": round(clon, 5),
                    "active_vehicles": len({x["vehicle_id"] for x in grp}),
                    "event_count": len(grp),
                    "anomaly_count": anom_by_h3.get((edate, reg, cell), 0),
                    "avg_speed_mps": round(sum(x["speed_mps"] for x in grp) / len(grp), 3),
                }
            )
        self._overwrite_pylist("gold.regional_h3_density", h3_rows)

        # 6. gold.telemetry_reliability_kpis
        bronze_by_dr: dict[tuple[str, str], int] = defaultdict(int)
        for b in bronze_rows:
            bronze_by_dr[(b["event_time"].strftime("%Y-%m-%d"), b["region"])] += 1
        quar_by_d: dict[str, int] = defaultdict(int)
        for q in quar_rows:
            quar_by_d[q["event_time"].strftime("%Y-%m-%d")] += 1

        clean_by_dr: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in clean_rows:
            clean_by_dr[(r["event_date"], r["region"])].append(r)

        rel_rows = []
        for (edate, reg), grp in sorted(clean_by_dr.items()):
            b_cnt = bronze_by_dr.get((edate, reg), len(grp))
            c_cnt = len(grp)
            q_cnt = quar_by_d.get(edate, 0)
            dup_cnt = max(0, b_cnt - c_cnt)
            late_cnt = sum(1 for x in grp if x["is_late_arrival"])
            lags = sorted(float(x["ingest_lag_seconds"]) for x in grp)
            p95_idx = min(len(lags) - 1, int(math.ceil(0.95 * len(lags))) - 1) if lags else 0
            rel_rows.append(
                {
                    "event_date": edate,
                    "region": reg,
                    "bronze_events": b_cnt,
                    "silver_clean_events": c_cnt,
                    "duplicate_events_removed": dup_cnt,
                    "quarantined_events": q_cnt,
                    "late_arrival_events": late_cnt,
                    "duplicate_rate_pct": round((dup_cnt / max(1, b_cnt)) * 100.0, 3),
                    "late_arrival_rate_pct": round((late_cnt / max(1, c_cnt)) * 100.0, 3),
                    "avg_ingest_lag_seconds": round(sum(lags) / max(1, len(lags)), 3),
                    "p95_ingest_lag_seconds": round(lags[p95_idx], 3) if lags else 0.0,
                }
            )
        self._overwrite_pylist("gold.telemetry_reliability_kpis", rel_rows)

        # 7. gold.vehicle_latest_snapshot
        anom_cnt_by_veh: dict[str, int] = defaultdict(int)
        for a in anoms_rows:
            anom_cnt_by_veh[a["vehicle_id"]] += 1

        by_veh: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in clean_rows:
            by_veh[r["vehicle_id"]].append(r)

        max_ev_time = max((r["event_time"] for r in clean_rows), default=now_naive)
        latest_rows = []
        for vid, vrows in sorted(by_veh.items()):
            vrows.sort(key=lambda x: x["event_time"])
            last = vrows[-1]
            gap_s = (max_ev_time - last["event_time"]).total_seconds()
            if last["vehicle_state"] == "offline":
                conn = "OFFLINE"
            elif last["vehicle_state"] == "driving" and gap_s >= 45.0:
                conn = "TELEMETRY_GAP"
            else:
                conn = "ONLINE"
            latest_rows.append(
                {
                    "vehicle_id": vid,
                    "region": last["region"],
                    "vehicle_model": last["vehicle_model"],
                    "software_version": last["software_version"],
                    "vehicle_state": last["vehicle_state"],
                    "connectivity_status": conn,
                    "last_event_time": last["event_time"],
                    "last_ingest_time": last["ingest_time"],
                    "latitude": last.get("latitude"),
                    "longitude": last.get("longitude"),
                    "h3_cell_res6": last.get("h3_cell_res6"),
                    "geofences_csv": last.get("geofences_csv") or "",
                    "speed_mps": last["speed_mps"],
                    "battery_soc_pct": last["battery_soc_pct"],
                    "battery_temp_c": last["battery_temp_c"],
                    "motor_temp_c": last["motor_temp_c"],
                    "sensor_health_score": last["sensor_health_score"],
                    "total_events": len(vrows),
                    "total_anomalies": anom_cnt_by_veh.get(vid, 0),
                }
            )
        self._overwrite_pylist("gold.vehicle_latest_snapshot", latest_rows)

        dur = round(time.perf_counter() - t0, 4)
        self.metrics.batch_job_duration_seconds.labels(job_name="silver_to_gold").observe(dur)
        self.metrics.data_freshness_seconds.labels(dataset="gold.fleet_activity_hourly").set(
            max(0.0, (now_naive - max_ev_time).total_seconds())
        )
        self.lineage.emit(
            event_type="COMPLETE",
            job_name="silver_to_gold_aggregates",
            run_id=run_id,
            inputs=[{"name": "silver.telemetry_clean"}, {"name": "silver.trips_reconstructed"}],
            outputs=[
                {"name": "gold.fleet_activity_hourly", "row_count": len(activity_rows)},
                {"name": "gold.anomaly_rates_by_segment", "row_count": len(anomaly_rate_rows)},
                {"name": "gold.trip_risk_summary", "row_count": len(trip_risk_rows)},
                {"name": "gold.regional_h3_density", "row_count": len(h3_rows)},
                {"name": "gold.vehicle_latest_snapshot", "row_count": len(latest_rows)},
            ],
        )
        return {
            "run_id": run_id,
            "duration_seconds": dur,
            "gold_fleet_activity_rows": len(activity_rows),
            "gold_anomaly_rates_rows": len(anomaly_rate_rows),
            "gold_trip_risk_rows": len(trip_risk_rows),
            "gold_battery_health_rows": len(batt_rows),
            "gold_regional_h3_rows": len(h3_rows),
            "gold_reliability_rows": len(rel_rows),
            "gold_vehicle_latest_rows": len(latest_rows),
        }

    def run_backfill(self, target_date: str) -> dict[str, Any]:
        """Idempotent date-scoped backfill across Bronze -> Silver -> Gold."""
        s = self.run_bronze_to_silver(target_date=target_date)
        g = self.run_silver_to_gold()
        return {"backfill_date": target_date, "silver": s, "gold": g}

    def run_maintenance(self) -> dict[str, Any]:
        """Run Iceberg compaction, safe snapshot expiration, and orphan-file audit."""
        target_tables = [
            "bronze.telemetry_raw",
            "silver.telemetry_clean",
            "silver.anomalies_verified",
            "gold.fleet_activity_hourly",
            "gold.vehicle_latest_snapshot",
        ]
        compactions = [self.lakehouse.compact_table(t) for t in target_tables]
        expirations = [
            self.lakehouse.expire_snapshots_safe(t, keep_last_n=5) for t in target_tables
        ]
        orphans = [
            self.lakehouse.detect_orphan_files(t, min_age_hours=72.0, delete=False)
            for t in target_tables
        ]
        for t in target_tables:
            st = self.lakehouse.table_stats(t)
            self.metrics.iceberg_table_files.labels(table=t).set(st["data_file_count"])
            self.metrics.iceberg_table_snapshots.labels(table=t).set(st["snapshot_count"])
        return {
            "compactions": compactions,
            "expirations": expirations,
            "orphan_audits": orphans,
        }

    def _overwrite_pylist(self, identifier: str, rows: list[dict[str, Any]]) -> None:
        tbl = self.lakehouse.load_table(identifier)
        arrow_tbl = pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow())
        self.lakehouse.overwrite_arrow(identifier, arrow_tbl)
