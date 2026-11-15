-- +goose Up
ALTER TABLE iot_data.meter_registrations
    ADD COLUMN request_id TEXT;

CREATE UNIQUE INDEX idx_meter_registrations_request_id
    ON iot_data.meter_registrations (request_id)
    WHERE request_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS iot_data.idx_meter_registrations_request_id;

ALTER TABLE iot_data.meter_registrations
    DROP COLUMN IF EXISTS request_id;
