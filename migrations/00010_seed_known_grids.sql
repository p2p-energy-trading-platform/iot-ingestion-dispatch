-- +goose Up

-- 05-startup-registry.md: provision the simulator's grids before their
-- telemetry is enabled. Idempotent, so it is safe on databases where the
-- grids were already inserted by hand or by migrations/seed/00003.
-- Coordinates are taken from the simulator's config/grids.yaml.
INSERT INTO iot_data.grids (grid_id, lat, lon) VALUES
    ('grid01', 6.9271, 79.8612),
    ('grid02', 9.6615, 80.0255),
    ('grid03', 7.8731, 80.6550)
ON CONFLICT (grid_id) DO NOTHING;

-- +goose Down

-- Provisioning data is deliberately kept on rollback: houses may already
-- reference these grids.
SELECT 1;
