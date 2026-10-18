-- +goose Up

-- Identity checks that belong to the accepted wire contract
-- (03-timescaledb-warm-storage.md). The decoder is the primary validation
-- layer; these are the final guard.
ALTER TABLE iot_data.meter_readings
    ADD CONSTRAINT chk_meter_readings_ids_nonempty
    CHECK (meter_id <> '' AND house_id <> '' AND grid_id <> '' AND schema_version <> ''),
    ADD CONSTRAINT chk_meter_readings_finite
    CHECK (solar_kw < 'Infinity' AND consumption_kw < 'Infinity'
           AND net_kw > '-Infinity' AND net_kw < 'Infinity'
           AND net_kw = net_kw AND solar_kw = solar_kw AND consumption_kw = consumption_kw);

ALTER TABLE iot_data.storage_asset_readings
    ADD CONSTRAINT chk_storage_asset_readings_ids_nonempty
    CHECK (meter_id <> '' AND house_id <> '' AND asset_id <> ''),
    ADD CONSTRAINT chk_storage_asset_readings_finite
    CHECK (power_kw > '-Infinity' AND power_kw < 'Infinity' AND power_kw = power_kw
           AND soc_pct = soc_pct AND capacity_kwh < 'Infinity');

ALTER TABLE iot_data.houses
    ADD CONSTRAINT chk_houses_ids_nonempty
    CHECK (house_id <> '' AND (meter_id IS NULL OR meter_id <> ''));

ALTER TABLE iot_data.flexible_assets
    ADD CONSTRAINT chk_flexible_assets_id_nonempty CHECK (asset_id <> '');

ALTER TABLE iot_data.discovered_assets
    ADD CONSTRAINT chk_discovered_assets_id_nonempty CHECK (asset_id <> '');

-- A house's meter is its identity binding; it must be unique once set.
CREATE UNIQUE INDEX IF NOT EXISTS uq_houses_meter_id
    ON iot_data.houses (meter_id) WHERE meter_id IS NOT NULL;

-- Columnstore / compression policy. The API changed across TimescaleDB
-- releases (add_compression_policy -> add_columnstore_policy), so pick what
-- the running server provides. Chunks older than 7 days are compressed; the
-- 6 month retention policy from 00002 is unchanged. If neither API exists the
-- migration fails rather than silently skipping a documented storage control.
-- +goose StatementBegin
DO $$
DECLARE
    has_columnstore boolean := EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'add_columnstore_policy');
    has_compression boolean := EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'add_compression_policy');
BEGIN
    ALTER TABLE iot_data.meter_readings SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'house_id',
        timescaledb.compress_orderby = '"time" DESC'
    );
    ALTER TABLE iot_data.storage_asset_readings SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'asset_id',
        timescaledb.compress_orderby = '"time" DESC'
    );

    IF has_columnstore THEN
        -- add_columnstore_policy is a procedure in current releases, so it is CALLed.
        CALL add_columnstore_policy('iot_data.meter_readings', after => INTERVAL '7 days', if_not_exists => true);
        CALL add_columnstore_policy('iot_data.storage_asset_readings', after => INTERVAL '7 days', if_not_exists => true);
    ELSIF has_compression THEN
        PERFORM add_compression_policy('iot_data.meter_readings', INTERVAL '7 days', if_not_exists => true);
        PERFORM add_compression_policy('iot_data.storage_asset_readings', INTERVAL '7 days', if_not_exists => true);
    ELSE
        RAISE EXCEPTION 'TimescaleDB provides neither add_columnstore_policy nor add_compression_policy';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- Forward-recovery policy: compressed chunks are not decompressed here (that
-- can be very expensive), so only the policies and constraints are removed.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'remove_columnstore_policy') THEN
        CALL remove_columnstore_policy('iot_data.meter_readings', if_exists => true);
        CALL remove_columnstore_policy('iot_data.storage_asset_readings', if_exists => true);
    ELSIF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'remove_compression_policy') THEN
        PERFORM remove_compression_policy('iot_data.meter_readings', if_exists => true);
        PERFORM remove_compression_policy('iot_data.storage_asset_readings', if_exists => true);
    END IF;
END
$$;
-- +goose StatementEnd

DROP INDEX IF EXISTS iot_data.uq_houses_meter_id;
ALTER TABLE iot_data.discovered_assets DROP CONSTRAINT IF EXISTS chk_discovered_assets_id_nonempty;
ALTER TABLE iot_data.flexible_assets DROP CONSTRAINT IF EXISTS chk_flexible_assets_id_nonempty;
ALTER TABLE iot_data.houses DROP CONSTRAINT IF EXISTS chk_houses_ids_nonempty;
ALTER TABLE iot_data.storage_asset_readings
    DROP CONSTRAINT IF EXISTS chk_storage_asset_readings_finite,
    DROP CONSTRAINT IF EXISTS chk_storage_asset_readings_ids_nonempty;
ALTER TABLE iot_data.meter_readings
    DROP CONSTRAINT IF EXISTS chk_meter_readings_finite,
    DROP CONSTRAINT IF EXISTS chk_meter_readings_ids_nonempty;
