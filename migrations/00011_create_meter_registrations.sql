
-- +goose Up
CREATE TYPE iot_data.meter_approval_status AS ENUM('DEWA_APPROVAL_PENDING', 'REJECTED', 'APPROVED', 'EXPIRED');
CREATE TYPE iot_data.meter_owner_role AS ENUM('PRIMARY_OWNER', 'VIEWER', 'OPERATOR');

CREATE TABLE IF NOT EXISTS iot_data.meter_registrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    meter_id TEXT NOT NULL,
    registration_id TEXT UNIQUE NOT NULL,
    approval_status  iot_data.meter_approval_status NOT NULL DEFAULT 'DEWA_APPROVAL_PENDING',
    role iot_data.meter_owner_role NOT NULL DEFAULT 'PRIMARY_OWNER',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_primary_owner_per_meter UNIQUE (meter_id, role)
);

CREATE INDEX IF NOT EXISTS idx_meter_registrations_meter_id
    ON iot_data.meter_registrations (meter_id);

-- +goose Down
DROP TABLE IF EXISTS iot_data.meter_registrations;
DROP TYPE IF EXISTS iot_data.meter_owner_role;
DROP TYPE IF EXISTS iot_data.meter_approval_status;
