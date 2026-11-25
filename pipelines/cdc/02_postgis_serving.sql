-- PostGIS 3.6 Spatial Serving Tables & GiST Indexes (SRID 4326 / WGS84)
-- Serves coarsened H3 fleet density, metro geofences, and geographic anomaly hotspots.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS fleet_spatial;

CREATE TABLE IF NOT EXISTS fleet_spatial.metro_geofences (
    geofence_id         VARCHAR(64) PRIMARY KEY,
    region              VARCHAR(32) NOT NULL,
    zone_type           VARCHAR(32) NOT NULL,
    geom                GEOMETRY(Polygon, 4326) NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metro_geofences_geom
    ON fleet_spatial.metro_geofences USING GIST (geom);

CREATE TABLE IF NOT EXISTS fleet_spatial.h3_fleet_density (
    event_date          DATE NOT NULL,
    region              VARCHAR(32) NOT NULL,
    h3_cell_res6        VARCHAR(24) NOT NULL,
    centroid            GEOMETRY(Point, 4326) NOT NULL,
    active_vehicles     INTEGER NOT NULL,
    event_count         BIGINT NOT NULL,
    anomaly_count       INTEGER NOT NULL,
    avg_speed_mps       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (event_date, region, h3_cell_res6)
);

CREATE INDEX IF NOT EXISTS idx_h3_density_centroid
    ON fleet_spatial.h3_fleet_density USING GIST (centroid);

CREATE TABLE IF NOT EXISTS fleet_spatial.anomaly_hotspots (
    anomaly_id          VARCHAR(64) PRIMARY KEY,
    vehicle_id          VARCHAR(32) NOT NULL,
    region              VARCHAR(32) NOT NULL,
    rule_id             VARCHAR(64) NOT NULL,
    severity            VARCHAR(16) NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL,
    h3_cell_res6        VARCHAR(24) NOT NULL,
    coarsened_point     GEOMETRY(Point, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomaly_hotspots_geom
    ON fleet_spatial.anomaly_hotspots USING GIST (coarsened_point);

-- Seed synthetic San Francisco, Phoenix, and Austin metro geofences
INSERT INTO fleet_spatial.metro_geofences (geofence_id, region, zone_type, geom)
VALUES
    ('sf_downtown_core', 'sf_bay', 'urban_core',
     ST_GeomFromText('POLYGON((-122.425 37.772,-122.388 37.772,-122.388 37.798,-122.425 37.798,-122.425 37.772))', 4326)),
    ('sf_depot_dogpatch', 'sf_bay', 'charging_depot',
     ST_GeomFromText('POLYGON((-122.398 37.752,-122.380 37.752,-122.380 37.768,-122.398 37.768,-122.398 37.752))', 4326)),
    ('phx_downtown', 'phoenix_metro', 'urban_core',
     ST_GeomFromText('POLYGON((-112.090 33.435,-112.055 33.435,-112.055 33.468,-112.090 33.468,-112.090 33.435))', 4326)),
    ('atx_downtown', 'austin_metro', 'urban_core',
     ST_GeomFromText('POLYGON((-97.760 30.258,-97.730 30.258,-97.730 30.282,-97.760 30.282,-97.760 30.258))', 4326))
ON CONFLICT (geofence_id) DO NOTHING;
