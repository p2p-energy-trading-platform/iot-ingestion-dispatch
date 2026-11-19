-- +goose Up
-- An account has one smart meter, and a house is identified by its meter: a
-- user may hold at most one approved primary-owner registration. Pending
-- claims are not ownership (anyone can type a meter number), so pending,
-- rejected and expired registrations are not limited.
-- Fails if an account already has two approved registrations; resolve those first.
CREATE UNIQUE INDEX unique_approved_primary_owner_per_user
ON iot_data.meter_registrations (user_id)
WHERE role = 'PRIMARY_OWNER' AND approval_status = 'APPROVED';

-- +goose Down
DROP INDEX IF EXISTS iot_data.unique_approved_primary_owner_per_user;
