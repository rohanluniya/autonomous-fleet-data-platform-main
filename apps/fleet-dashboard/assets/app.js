/* Autonomous Fleet Intelligence Dashboard Controller
 * Uses MapLibre GL JS with a self-hosted offline GeoJSON basemap (zero external tile dependencies)
 * and Chart.js for operational analytics. Includes graceful Canvas fallback if WebGL is unavailable.
 */

const REGION_VIEWS = {
  sf_bay: { center: [-122.415, 37.774], zoom: 11.4 },
  phoenix_metro: { center: [-112.025, 33.445], zoom: 10.6 },
  austin_metro: { center: [-97.743, 30.275], zoom: 11.2 },
};

let activeRegion = "sf_bay";
let mapInstance = null;
let cachedBasemap = null;
let cachedRegions = null;
const charts = {};

function fmtTime(iso) {
  if (!iso) return "--";
  return iso.replace("T", " ").replace("+00:00", " UTC").slice(0, 23);
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

function initTabs() {
  document.querySelectorAll(".region-tabs .tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".region-tabs .tab-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      activeRegion = btn.dataset.region;
      focusRegion(activeRegion);
    });
  });

  document.querySelectorAll(".view-tabs .tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".view-tabs .tab-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      const target = btn.dataset.view;
      document.querySelectorAll(".table-view").forEach((v) => v.classList.remove("active"));
      document.getElementById(`view-${target}`).classList.add("active");
    });
  });

  document.getElementById("btn-refresh").addEventListener("click", () => loadDashboard());
}

function focusRegion(regionId) {
  const view = REGION_VIEWS[regionId] || REGION_VIEWS.sf_bay;
  if (mapInstance && typeof mapInstance.flyTo === "function") {
    mapInstance.flyTo({ center: view.center, zoom: view.zoom, duration: 500 });
  } else if (cachedBasemap && cachedRegions) {
    renderSvgFallbackMap(regionId, cachedBasemap, cachedRegions.h3_geojson);
  }
}

function initOrUpdateMap(basemapGeojson, h3Geojson) {
  const container = document.getElementById("fleet-map");
  if (typeof maplibregl === "undefined") {
    renderSvgFallbackMap(activeRegion, basemapGeojson, h3Geojson);
    return;
  }

  try {
    if (!mapInstance) {
      const view = REGION_VIEWS[activeRegion];
      mapInstance = new maplibregl.Map({
        container: "fleet-map",
        center: view.center,
        zoom: view.zoom,
        attributionControl: false,
        style: {
          version: 8,
          sources: {
            basemap: { type: "geojson", data: basemapGeojson },
            h3cells: { type: "geojson", data: h3Geojson },
          },
          layers: [
            {
              id: "bg",
              type: "background",
              paint: { "background-color": "#091122" },
            },
            {
              id: "metro-bounds",
              type: "fill",
              source: "basemap",
              filter: ["==", ["get", "feature_type"], "metro_boundary"],
              paint: { "fill-color": "#111c33", "fill-outline-color": "#263a63" },
            },
            {
              id: "geofences-fill",
              type: "fill",
              source: "basemap",
              filter: ["==", ["get", "feature_type"], "geofence"],
              paint: {
                "fill-color": "#34d399",
                "fill-opacity": 0.14,
                "fill-outline-color": "#34d399",
              },
            },
            {
              id: "h3-density-fill",
              type: "fill",
              source: "h3cells",
              paint: {
                "fill-color": [
                  "case",
                  [">=", ["get", "anomaly_count"], 15], "#ef4444",
                  [">=", ["get", "anomaly_count"], 5], "#f59e0b",
                  [">=", ["get", "active_vehicles"], 100], "#0ea5e9",
                  "#1d4ed8",
                ],
                "fill-opacity": 0.52,
                "fill-outline-color": "#93c5fd",
              },
            },
            {
              id: "routes-line",
              type: "line",
              source: "basemap",
              filter: ["==", ["get", "feature_type"], "route"],
              paint: { "line-color": "#fbbf24", "line-width": 2.4, "line-opacity": 0.85 },
            },
          ],
        },
      });

      mapInstance.on("error", () => {
        // Fallback if WebGL unavailable (e.g. software-only headless environment)
        if (!container.dataset.fallback) {
          container.dataset.fallback = "true";
          mapInstance = null;
          renderSvgFallbackMap(activeRegion, basemapGeojson, h3Geojson);
        }
      });

      mapInstance.on("click", "h3-density-fill", (e) => {
        const props = e.features[0].properties;
        new maplibregl.Popup()
          .setLngLat(e.lngLat)
          .setHTML(
            `<div style="color:#0b1120;font-size:12px;">
              <strong>H3 Res-6: ${props.h3_cell_res6}</strong><br>
              Region: ${props.region}<br>
              Active Vehicles: ${props.active_vehicles}<br>
              Events: ${props.event_count} | Anomalies: <b>${props.anomaly_count}</b><br>
              Avg Speed: ${props.avg_speed_mps} m/s
            </div>`
          )
          .addTo(mapInstance);
      });
    } else {
      const h3Src = mapInstance.getSource("h3cells");
      if (h3Src) h3Src.setData(h3Geojson);
    }
  } catch (_err) {
    mapInstance = null;
    renderSvgFallbackMap(activeRegion, basemapGeojson, h3Geojson);
  }
}

function renderSvgFallbackMap(regionId, basemapGeojson, h3Geojson) {
  const container = document.getElementById("fleet-map");
  const w = container.clientWidth || 720;
  const h = 410;
  const regionFeatures = basemapGeojson.features.filter((f) => f.properties.region === regionId);
  const h3Features = h3Geojson.features.filter((f) => f.properties.region === regionId);

  let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
  const scanCoords = (coords) => {
    if (typeof coords[0] === "number") {
      minLon = Math.min(minLon, coords[0]);
      maxLon = Math.max(maxLon, coords[0]);
      minLat = Math.min(minLat, coords[1]);
      maxLat = Math.max(maxLat, coords[1]);
    } else {
      coords.forEach(scanCoords);
    }
  };
  [...regionFeatures, ...h3Features].forEach((f) => scanCoords(f.geometry.coordinates));
  if (!isFinite(minLon)) return;

  const pad = 28;
  const proj = ([lon, lat]) => {
    const x = pad + ((lon - minLon) / Math.max(1e-6, maxLon - minLon)) * (w - 2 * pad);
    const y = h - pad - ((lat - minLat) / Math.max(1e-6, maxLat - minLat)) * (h - 2 * pad);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  };

  let svg = `<svg width="100%" height="${h}" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg" style="background:#091122">`;
  // Subtle coordinate grid
  for (let i = 1; i < 6; i++) {
    const gx = (w / 6) * i;
    const gy = (h / 6) * i;
    svg += `<line x1="${gx}" y1="0" x2="${gx}" y2="${h}" stroke="#172440" stroke-width="1"/>`;
    svg += `<line x1="0" y1="${gy}" x2="${w}" y2="${gy}" stroke="#172440" stroke-width="1"/>`;
  }
  // Geofences
  regionFeatures
    .filter((f) => f.properties.feature_type === "geofence")
    .forEach((f) => {
      const pts = f.geometry.coordinates[0].map(proj).join(" ");
      svg += `<polygon points="${pts}" fill="rgba(52,211,153,0.16)" stroke="#34d399" stroke-width="1.5"/>`;
    });
  // H3 Cells
  h3Features.forEach((f) => {
    const p = f.properties;
    const fill =
      p.anomaly_count >= 15
        ? "rgba(239,68,68,0.58)"
        : p.anomaly_count >= 5
          ? "rgba(245,158,11,0.52)"
          : "rgba(14,165,233,0.45)";
    const pts = f.geometry.coordinates[0].map(proj).join(" ");
    const center = proj([p.center_lon, p.center_lat]).split(",");
    svg += `<polygon points="${pts}" fill="${fill}" stroke="#93c5fd" stroke-width="1.2"/>`;
    svg += `<text x="${center[0]}" y="${center[1]}" fill="#f8fafc" font-size="11" font-family="monospace" text-anchor="middle">${p.active_vehicles}v / ${p.anomaly_count}a</text>`;
  });
  // Routes
  regionFeatures
    .filter((f) => f.properties.feature_type === "route")
    .forEach((f) => {
      const pts = f.geometry.coordinates.map(proj).join(" ");
      svg += `<polyline points="${pts}" fill="none" stroke="#fbbf24" stroke-width="2.5" stroke-dasharray="6,3"/>`;
    });
  svg += `<text x="14" y="22" fill="#93a6c9" font-size="11" font-family="monospace">Region: ${regionId} · H3 Res-6 Coarsened Vector Layer (Self-Hosted GeoJSON)</text>`;
  svg += "</svg>";
  container.innerHTML = svg;
}

function upsertChart(id, config) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  if (ctx && typeof Chart !== "undefined") {
    charts[id] = new Chart(ctx, config);
  }
}

function renderCharts(summary) {
  const sw = summary.software_version_comparison || [];
  upsertChart("chart-sw-version", {
    type: "bar",
    data: {
      labels: sw.map((r) => r.software_version),
      datasets: [
        {
          label: "Anomalies / 1k Events",
          data: sw.map((r) => r.anomalies_per_1k_events),
          backgroundColor: "#f87171",
          borderRadius: 6,
        },
        {
          label: "Avg Speed (m/s)",
          data: sw.map((r) => r.avg_speed_mps),
          backgroundColor: "#38bdf8",
          borderRadius: 6,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#cbd5e1", font: { size: 11 } } } },
      scales: {
        x: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" } },
        y: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" } },
      },
    },
  });

  const tl = summary.anomaly_timeline || [];
  upsertChart("chart-anomaly-timeline", {
    type: "line",
    data: {
      labels: tl.map((r) => fmtTime(r.minute_bucket).slice(11, 19)),
      datasets: [
        {
          label: "Total Anomalies",
          data: tl.map((r) => r.anomaly_count),
          borderColor: "#38bdf8",
          backgroundColor: "rgba(56,189,248,0.16)",
          fill: true,
          tension: 0.3,
        },
        {
          label: "Critical",
          data: tl.map((r) => r.critical_count),
          borderColor: "#f87171",
          tension: 0.3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#cbd5e1", font: { size: 11 } } } },
      scales: {
        x: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" } },
        y: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" }, beginAtZero: true },
      },
    },
  });

  const rel = summary.reliability_by_region || [];
  upsertChart("chart-reliability", {
    type: "bar",
    data: {
      labels: rel.map((r) => r.region),
      datasets: [
        {
          label: "Duplicate Rate (%)",
          data: rel.map((r) => r.duplicate_rate_pct),
          backgroundColor: "#fbbf24",
          borderRadius: 6,
        },
        {
          label: "Late Arrival Rate (%)",
          data: rel.map((r) => r.late_arrival_rate_pct),
          backgroundColor: "#a78bfa",
          borderRadius: 6,
        },
        {
          label: "p95 Ingest Lag (s)",
          data: rel.map((r) => r.p95_ingest_lag_seconds),
          backgroundColor: "#34d399",
          borderRadius: 6,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#cbd5e1", font: { size: 11 } } } },
      scales: {
        x: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" } },
        y: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" }, beginAtZero: true },
      },
    },
  });

  const rules = summary.anomalies_by_rule || [];
  upsertChart("chart-rules", {
    type: "bar",
    data: {
      labels: rules.map((r) => r.rule_id.replace("RULE_", "")),
      datasets: [
        {
          label: "Detections",
          data: rules.map((r) => r.count),
          backgroundColor: rules.map((r) =>
            r.severity === "CRITICAL" ? "#f87171" : r.severity === "HIGH" ? "#fbbf24" : "#38bdf8"
          ),
          borderRadius: 6,
        },
      ],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#93a6c9" }, grid: { color: "#1e2c4a" }, beginAtZero: true },
        y: { ticks: { color: "#cbd5e1", font: { size: 11 } }, grid: { display: false } },
      },
    },
  });
}

function renderTables(anomaliesRes, tripsRes, dqRes, metricsRes) {
  const anomBody = document.getElementById("tbody-anomalies");
  anomBody.innerHTML = (anomaliesRes.items || [])
    .slice(0, 15)
    .map(
      (a) => `
      <tr>
        <td class="mono">${a.vehicle_id}</td>
        <td class="mono">${a.rule_id} <span class="pill">v${a.rule_version}</span></td>
        <td><span class="sev-pill sev-${a.severity}">${a.severity}</span></td>
        <td>${a.region} · ${a.software_version}</td>
        <td class="mono">${fmtTime(a.event_time)}</td>
        <td class="mono">${fmtTime(a.detection_time)}</td>
        <td class="mono">${JSON.stringify(a.evidence || {}).slice(0, 95)}</td>
      </tr>`
    )
    .join("");

  const tripBody = document.getElementById("tbody-trips");
  tripBody.innerHTML = (tripsRes.items || [])
    .slice(0, 15)
    .map(
      (t) => `
      <tr>
        <td class="mono">${t.trip_id}</td>
        <td class="mono">${t.vehicle_id}</td>
        <td>${t.region} · ${t.software_version}</td>
        <td>${Number(t.distance_km).toFixed(2)}</td>
        <td>${Number(t.duration_seconds).toFixed(0)}</td>
        <td>${t.anomaly_count} (${t.critical_anomaly_count})</td>
        <td>${t.disengagement_count}</td>
        <td><strong>${t.risk_score}</strong> <span class="sev-pill sev-${t.risk_tier === "HIGH" ? "HIGH-TIER" : t.risk_tier}">${t.risk_tier}</span></td>
      </tr>`
    )
    .join("");

  const dqBody = document.getElementById("tbody-dq");
  dqBody.innerHTML = (dqRes.checks || [])
    .map(
      (c) => `
      <tr>
        <td>${c.layer}</td>
        <td class="mono">${c.check_name}</td>
        <td><span class="sev-pill sev-${c.status}">${c.status}</span></td>
        <td class="mono">${c.observed_value}</td>
        <td class="mono">${c.warn_threshold} / ${c.fail_threshold}</td>
        <td class="mono">${JSON.stringify(c.details || {}).slice(0, 90)}</td>
      </tr>`
    )
    .join("");

  const iceBody = document.getElementById("tbody-iceberg");
  iceBody.innerHTML = (metricsRes.iceberg_tables || [])
    .filter((t) => t.total_records > 0)
    .map(
      (t) => `
      <tr>
        <td class="mono">${t.table}</td>
        <td>${t.snapshot_count}</td>
        <td>${t.data_file_count}</td>
        <td>${t.total_records.toLocaleString()}</td>
        <td>${(t.total_bytes / 1024).toFixed(1)}</td>
      </tr>`
    )
    .join("");

  const lag = metricsRes.consumer_lag_records || {};
  const res = metricsRes.host_resources || {};
  document.getElementById("lag-summary").textContent =
    `Backend: ${metricsRes.transport_backend} | Bronze Lag: ${lag.bronze_ingestor ?? 0} | Stream Lag: ${lag.stream_processor ?? 0} | CPU: ${res.cpu_percent ?? 0}% | RAM: ${res.memory_used_percent ?? 0}% of ${res.memory_total_gib ?? 0} GiB`;

  const linBody = document.getElementById("tbody-lineage");
  linBody.innerHTML = (metricsRes.recent_openlineage_runs || [])
    .slice(-8)
    .reverse()
    .map(
      (l) => `
      <tr>
        <td><span class="sev-pill sev-${l.event_type === "COMPLETE" ? "PASS" : "MEDIUM"}">${l.event_type}</span></td>
        <td class="mono">${l.job_name}</td>
        <td class="mono">${(l.inputs || []).slice(0, 2).join(", ")} -> ${(l.outputs || []).slice(0, 2).join(", ")}</td>
      </tr>`
    )
    .join("");
}

async function loadDashboard() {
  try {
    const [health, summary, regions, basemap, anomalies, trips, dq, plat] = await Promise.all([
      fetchJson("/health"),
      fetchJson("/fleet/summary"),
      fetchJson("/fleet/regions"),
      fetchJson("/api/map/basemap.geojson"),
      fetchJson("/fleet/anomalies?limit=25"),
      fetchJson("/fleet/trips?limit=25"),
      fetchJson("/fleet/data-quality"),
      fetchJson("/platform/metrics"),
    ]);

    cachedBasemap = basemap;
    cachedRegions = regions;

    document.getElementById("badge-profile").textContent = `Profile: ${health.execution_profile}`;
    document.getElementById("meta-event-time").textContent = fmtTime(health.latest_event_time_utc);
    document.getElementById("meta-ingest-time").textContent = fmtTime(health.latest_ingest_time_utc);
    document.getElementById("meta-sql-engine").textContent = health.sql_engine;
    document.getElementById("meta-transport").textContent = health.transport_backend;

    const ov = summary.overview || {};
    document.getElementById("kpi-total-vehicles").textContent = (ov.total_vehicles ?? 0).toLocaleString();
    document.getElementById("kpi-state-breakdown").textContent =
      `Driving ${ov.driving_vehicles ?? 0} · Parked ${ov.parked_vehicles ?? 0} · Charging ${ov.charging_vehicles ?? 0}`;

    const gapsAndOff = (ov.telemetry_gap_vehicles ?? 0) + (ov.offline_vehicles ?? 0);
    document.getElementById("kpi-gaps-offline").textContent = gapsAndOff.toLocaleString();
    document.getElementById("kpi-gaps-sub").textContent =
      `Offline ${ov.offline_vehicles ?? 0} · Telemetry Gaps ${ov.telemetry_gap_vehicles ?? 0}`;

    const totalAnoms = (summary.anomalies_by_rule || []).reduce((s, r) => s + Number(r.count || 0), 0);
    const critAnoms = (summary.anomalies_by_rule || [])
      .filter((r) => r.severity === "CRITICAL")
      .reduce((s, r) => s + Number(r.count || 0), 0);
    document.getElementById("kpi-anomalies").textContent = totalAnoms.toLocaleString();
    document.getElementById("kpi-anomalies-sub").textContent = `${critAnoms} critical · ${(summary.anomalies_by_rule || []).length} rules active`;

    const totalTrips = (summary.trip_risk_distribution || []).reduce((s, r) => s + Number(r.trip_count || 0), 0);
    const highTrips = (summary.trip_risk_distribution || [])
      .filter((r) => r.risk_tier === "HIGH" || r.risk_tier === "ELEVATED")
      .reduce((s, r) => s + Number(r.trip_count || 0), 0);
    document.getElementById("kpi-trips").textContent = totalTrips.toLocaleString();
    document.getElementById("kpi-trips-sub").textContent = `${highTrips} elevated/high-risk trips`;

    document.getElementById("kpi-health").textContent =
      `${ov.avg_battery_soc_pct ?? "--"}% / ${((ov.avg_sensor_health ?? 0) * 100).toFixed(1)}%`;
    document.getElementById("kpi-health-sub").textContent = "Avg battery SoC / sensor perception score";

    const dqs = dq.summary || {};
    document.getElementById("kpi-dq").textContent = `${dq.overall_status || "--"} (${dqs.passed ?? 0}/${dqs.total ?? 0})`;
    document.getElementById("kpi-dq-sub").textContent = `DLQ ${dq.dlq_count ?? 0} · Quarantine ${dq.quarantine_count ?? 0}`;

    initOrUpdateMap(basemap, regions.h3_geojson);
    renderCharts(summary);
    renderTables(anomalies, trips, dq, plat);
  } catch (err) {
    console.error("Failed to load fleet dashboard:", err);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  loadDashboard();
});
