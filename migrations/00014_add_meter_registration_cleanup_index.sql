-- +goose Up
CREATE INDEX IF NOT EXISTS idx_meter_registrations_cleanup
    ON iot_data.meter_registrations (updated_at, id)
    WHERE approval_status IN (
        'DEWA_APPROVAL_PENDING',
        'REJECTED',
        'EXPIRED'
    );

-- +goose Down
DROP INDEX IF EXISTS iot_data.idx_meter_registrations_cleanup;    