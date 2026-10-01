-- +goose Up

-- Liveness is derived from how recent a house's heartbeat is, so the column
-- carries that name. Asset last_seen_at is unchanged.
ALTER TABLE iot_data.houses RENAME COLUMN last_seen_at TO last_heartbeat_at;

-- +goose Down

ALTER TABLE iot_data.houses RENAME COLUMN last_heartbeat_at TO last_seen_at;