-- Operational PostgreSQL 17 Schema for Autonomous Fleet Vehicle Registry & CDC
-- Configured with REPLICA IDENTITY FULL and logical publication for Debezium 3.7.1.Final.

CREATE SCHEMA IF NOT EXISTS fleet_ops;

CREATE TABLE IF NOT EXISTS fleet_ops.vehicle_registry (
    vehicle_id          VARCHAR(32) PRIMARY KEY,
    hardware_generation VARCHAR(32) NOT NULL,
    vehicle_model       VARCHAR(64) NOT NULL,
    assigned_region     VARCHAR(32) NOT NULL,
    software_version    VARCHAR(32) NOT NULL,
    fleet_tier          VARCHAR(32) NOT NULL DEFAULT 'commercial_metro',
    maintenance_status  VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    commissioned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fleet_ops.firmware_deployments (
    deployment_id       BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(32) NOT NULL REFERENCES fleet_ops.vehicle_registry(vehicle_id),
    from_version        VARCHAR(32) NOT NULL,
    to_version          VARCHAR(32) NOT NULL,
    deployed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rollout_ring        VARCHAR(32) NOT NULL DEFAULT 'canary'
);

CREATE TABLE IF NOT EXISTS fleet_ops.maintenance_events (
    maintenance_id      BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(32) NOT NULL REFERENCES fleet_ops.vehicle_registry(vehicle_id),
    event_type          VARCHAR(64) NOT NULL,
    notes               TEXT,
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at           TIMESTAMPTZ
);

ALTER TABLE fleet_ops.vehicle_registry REPLICA IDENTITY FULL;
ALTER TABLE fleet_ops.firmware_deployments REPLICA IDENTITY FULL;
ALTER TABLE fleet_ops.maintenance_events REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'fleet_cdc_pub') THEN
        CREATE PUBLICATION fleet_cdc_pub FOR TABLE
            fleet_ops.vehicle_registry,
            fleet_ops.firmware_deployments,
            fleet_ops.maintenance_events;
    END IF;
END $$;
