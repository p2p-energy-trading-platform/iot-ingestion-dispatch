-- +goose Up

ALTER TABLE iot_data.houses
    ADD COLUMN IF NOT EXISTS meter_id TEXT;

-- +goose Down

ALTER TABLE iot_data.houses
    DROP COLUMN IF EXISTS meter_id;
