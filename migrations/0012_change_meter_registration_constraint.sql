-- +goose Up
ALTER TABLE iot_data.meter_registrations 
DROP CONSTRAINT IF EXISTS unique_primary_owner_per_meter;

CREATE UNIQUE INDEX unique_approved_primary_owner 
ON iot_data.meter_registrations (meter_id) 
WHERE role = 'PRIMARY_OWNER' AND approval_status = 'APPROVED';

-- +goose Down
DROP INDEX IF EXISTS iot_data.unique_approved_primary_owner;