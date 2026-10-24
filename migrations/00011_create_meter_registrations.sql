
-- +goose Up
CREATE TABLE IF NOT EXISTS iot_data.meter_registrations (
    registration_id TEXT PRIMARY KEY,
    meter_id TEXT NOT NULL,
    user_reference TEXT NOT NULL,
    approval_status TEXT NOT NULL DEFAULT 'DEWA_APPROVAL_PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT meter_registrations_approval_status_check
        CHECK (
            approval_status IN (
                'DEWA_APPROVAL_PENDING',
                'APPROVED',
                'REJECTED',
                'TIMED_OUT'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_meter_registrations_meter_id
    ON iot_data.meter_registrations (meter_id);

-- +goose Down
DROP TABLE IF EXISTS iot_data.meter_registrations;
