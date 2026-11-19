-- +goose Up
-- An account has one smart meter, and a house is identified by its meter: a
-- user may hold at most one pending or approved primary-owner registration.
-- Rejected and expired registrations do not count, so the user can claim again.
-- Fails if an account already has two active registrations; resolve those first.
CREATE UNIQUE INDEX unique_active_primary_owner_per_user
ON iot_data.meter_registrations (user_id)
WHERE role = 'PRIMARY_OWNER' AND approval_status IN ('DEWA_APPROVAL_PENDING', 'APPROVED');

-- +goose Down
DROP INDEX IF EXISTS iot_data.unique_active_primary_owner_per_user;
