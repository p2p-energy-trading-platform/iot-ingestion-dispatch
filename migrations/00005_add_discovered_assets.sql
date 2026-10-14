-- +goose Up

-- discovered_assets: assets observed in heartbeat payloads that have no
-- authoritative registration. An observation is not a registration: nothing
-- here grants ownership, trading, dispatch, settlement eligibility or user
-- consent. Only iot_data.flexible_assets, written by the registration flow,
-- is authoritative.
CREATE TABLE iot_data.discovered_assets (
    asset_id          TEXT PRIMARY KEY,
    observed_house_id TEXT NOT NULL REFERENCES iot_data.houses (house_id),
    asset_type        TEXT NOT NULL CHECK (asset_type IN ('bess', 'ev')),
    capacity_kwh      DOUBLE PRECISION NOT NULL CHECK (capacity_kwh > 0),
    max_charge_kw     DOUBLE PRECISION NOT NULL CHECK (max_charge_kw >= 0),
    max_discharge_kw  DOUBLE PRECISION NOT NULL CHECK (max_discharge_kw >= 0),
    v2g_capable       BOOLEAN,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_discovered_v2g_only_on_ev CHECK (v2g_capable IS NULL OR asset_type = 'ev')
);

CREATE INDEX idx_discovered_assets_house_id ON iot_data.discovered_assets (observed_house_id);

-- +goose Down

DROP TABLE IF EXISTS iot_data.discovered_assets;
