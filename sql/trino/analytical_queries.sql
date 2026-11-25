-- Trino 484 Analytical SQL Queries over Apache Iceberg Lakehouse (Catalog: iceberg -> Lakekeeper REST)

-- 1. Inspect Iceberg snapshots and demonstrate time-travel
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.bronze."telemetry_raw$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- Time-travel query example (replace <SNAPSHOT_ID> with snapshot_id from query above):
-- SELECT COUNT(*) AS rows_at_snapshot FROM iceberg.bronze.telemetry_raw FOR VERSION AS OF <SNAPSHOT_ID>;

-- 2. Fleet Activity & Software Version Comparison
SELECT
    event_date,
    region,
    software_version,
    SUM(active_vehicles) AS vehicle_hour_count,
    SUM(total_events) AS total_events,
    ROUND(AVG(avg_speed_mps), 2) AS mean_speed_mps,
    ROUND(AVG(avg_battery_soc_pct), 2) AS mean_battery_soc_pct,
    ROUND(AVG(avg_sensor_health), 4) AS mean_sensor_health,
    SUM(late_arrival_events) AS late_arrival_events
FROM iceberg.gold.fleet_activity_hourly
GROUP BY event_date, region, software_version
ORDER BY event_date DESC, region, software_version;

-- 3. Anomaly Rate Normalized per 1,000 Events by Region, Vehicle Model, and Software Version
SELECT
    event_date,
    region,
    vehicle_model,
    software_version,
    rule_id,
    severity,
    anomaly_count,
    affected_vehicles,
    anomalies_per_1k_events
FROM iceberg.gold.anomaly_rates_by_segment
ORDER BY anomalies_per_1k_events DESC, anomaly_count DESC
LIMIT 50;

-- 4. Highest-Risk Trips Requiring Operational Review
SELECT
    trip_id,
    vehicle_id,
    region,
    software_version,
    distance_km,
    duration_seconds,
    anomaly_count,
    critical_anomaly_count,
    disengagement_count,
    risk_score,
    risk_tier
FROM iceberg.gold.trip_risk_summary
WHERE risk_tier IN ('HIGH', 'ELEVATED')
ORDER BY risk_score DESC
LIMIT 25;

-- 5. Coarsened Regional H3 Anomaly Hotspots (Privacy-Safe Resolution 6)
SELECT
    region,
    h3_cell_res6,
    center_lat,
    center_lon,
    SUM(active_vehicles) AS active_vehicles,
    SUM(event_count) AS event_count,
    SUM(anomaly_count) AS anomaly_count,
    ROUND(1000.0 * SUM(anomaly_count) / GREATEST(1, SUM(event_count)), 3) AS anomalies_per_1k
FROM iceberg.gold.regional_h3_density
GROUP BY region, h3_cell_res6, center_lat, center_lon
ORDER BY anomaly_count DESC;
