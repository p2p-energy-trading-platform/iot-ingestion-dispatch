-- +goose Up

-- The iot_data schema and the timescaledb extension are already provisioned
-- by gridx-infra's bootstrap script (init-scripts/timescaledb/01-init-timescaledb.sql),
-- which creates the schema AUTHORIZATION IOT_SERVICE_USER and enables the
-- extension ahead of any service migration running. Confirmed directly:
-- IOT_SERVICE_USER has schema-level ownership only, by design, and does NOT
-- have database-level CREATE privilege — so CREATE SCHEMA / CREATE EXTENSION
-- statements here would fail with "permission denied for database" even
-- with IF NOT EXISTS guards. Intentionally omitted rather than worked around.

-- ============================================================================
-- Registry tables (small, low-churn, updated via heartbeat device-discovery)
-- ============================================================================

-- grids: one row per configured zone. Lat/lon is simulator-internal config,
-- never published on the wire, so this table must be seeded/loaded out of
-- band (grid_loader.go) rather than derived from Kafka records.
CREATE TABLE iot_data.grids (
    grid_id      TEXT PRIMARY KEY,
    lat          DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon          DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    display_name TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- houses: discovered from heartbeat messages, not meter readings.
CREATE TABLE iot_data.houses (
    house_id       TEXT PRIMARY KEY,
    grid_id        TEXT NOT NULL REFERENCES iot_data.grids (grid_id),
    device_class   TEXT NOT NULL CHECK (device_class IN ('consumer', 'residential_prosumer', 'commercial')),
    rated_solar_kw DOUBLE PRECISION CHECK (rated_solar_kw IS NULL OR rated_solar_kw >= 0),
    status         TEXT NOT NULL DEFAULT 'online' CHECK (status IN ('online', 'offline')),
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_houses_grid_id ON iot_data.houses (grid_id);

-- flexible_assets: batteries / EVs reported in a house's heartbeat payload.
CREATE TABLE iot_data.flexible_assets (
    asset_id         TEXT PRIMARY KEY,
    house_id         TEXT NOT NULL REFERENCES iot_data.houses (house_id),
    asset_type       TEXT NOT NULL CHECK (asset_type IN ('bess', 'ev')),
    capacity_kwh     DOUBLE PRECISION NOT NULL CHECK (capacity_kwh > 0),
    max_charge_kw    DOUBLE PRECISION NOT NULL CHECK (max_charge_kw >= 0),
    max_discharge_kw DOUBLE PRECISION NOT NULL CHECK (max_discharge_kw >= 0),
    v2g_capable      BOOLEAN,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_v2g_only_on_ev CHECK (v2g_capable IS NULL OR asset_type = 'ev')
);

CREATE INDEX idx_flexible_assets_house_id ON iot_data.flexible_assets (house_id);

-- ============================================================================
-- Telemetry tables (high volume, hypertables, partitioned on "time")
-- Deliberately NOT foreign-keyed to grids/houses/flexible_assets: telemetry
-- must keep ingesting even if a device's heartbeat hasn't been processed yet,
-- since meter and heartbeat topics are consumed independently.
-- ============================================================================

-- meter_readings: one row per meter tick (~5s cadence).
CREATE TABLE iot_data.meter_readings (
    "time"                 TIMESTAMPTZ NOT NULL,
    meter_id               TEXT NOT NULL,
    house_id               TEXT NOT NULL,
    grid_id                TEXT NOT NULL,
    device_class           TEXT NOT NULL CHECK (device_class IN ('consumer', 'residential_prosumer', 'commercial')),
    schema_version          TEXT NOT NULL,
    seq                    BIGINT NOT NULL CHECK (seq >= 0),
    solar_kw               DOUBLE PRECISION NOT NULL CHECK (solar_kw >= 0),
    consumption_kw          DOUBLE PRECISION NOT NULL CHECK (consumption_kw >= 0),
    net_kw                 DOUBLE PRECISION NOT NULL,
    weather_irradiance_wm2  DOUBLE PRECISION CHECK (weather_irradiance_wm2 IS NULL OR weather_irradiance_wm2 >= 0),
    cloud_cover_pct         DOUBLE PRECISION CHECK (cloud_cover_pct IS NULL OR cloud_cover_pct BETWEEN 0 AND 100),
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("time", meter_id, seq)
);

SELECT create_hypertable('iot_data.meter_readings', 'time');

CREATE INDEX idx_meter_readings_house_time ON iot_data.meter_readings (house_id, "time" DESC);
CREATE INDEX idx_meter_readings_grid_time ON iot_data.meter_readings (grid_id, "time" DESC);

SELECT add_retention_policy('iot_data.meter_readings', INTERVAL '6 months');

-- storage_asset_readings: one row per flexible asset per meter tick
-- (a single meter_readings row can fan out into zero or more of these).
CREATE TABLE iot_data.storage_asset_readings (
    "time"       TIMESTAMPTZ NOT NULL,
    meter_id     TEXT NOT NULL,
    house_id     TEXT NOT NULL,
    asset_id     TEXT NOT NULL,
    asset_type   TEXT NOT NULL CHECK (asset_type IN ('bess', 'ev')),
    soc_pct      DOUBLE PRECISION NOT NULL CHECK (soc_pct BETWEEN 0 AND 100),
    power_kw     DOUBLE PRECISION NOT NULL,
    capacity_kwh DOUBLE PRECISION NOT NULL CHECK (capacity_kwh > 0),
    plugged_in   BOOLEAN,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("time", meter_id, asset_id),
    CONSTRAINT chk_plugged_in_only_on_ev CHECK (plugged_in IS NULL OR asset_type = 'ev')
);

SELECT create_hypertable('iot_data.storage_asset_readings', 'time');

CREATE INDEX idx_storage_readings_house_time ON iot_data.storage_asset_readings (house_id, "time" DESC);
CREATE INDEX idx_storage_readings_asset_time ON iot_data.storage_asset_readings (asset_id, "time" DESC);

SELECT add_retention_policy('iot_data.storage_asset_readings', INTERVAL '6 months');

-- +goose Down

SELECT remove_retention_policy('iot_data.storage_asset_readings', if_exists => true);
DROP TABLE IF EXISTS iot_data.storage_asset_readings;

SELECT remove_retention_policy('iot_data.meter_readings', if_exists => true);
DROP TABLE IF EXISTS iot_data.meter_readings;

DROP TABLE IF EXISTS iot_data.flexible_assets;
DROP TABLE IF EXISTS iot_data.houses;
DROP TABLE IF EXISTS iot_data.grids;

-- Schema itself is left in place: it's provisioned by gridx-infra's bootstrap
-- script and owned by IOT_SERVICE_USER, not by this migration.
