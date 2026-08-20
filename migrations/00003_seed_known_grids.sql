-- +goose Up

-- Coordinates confirmed directly against the simulator's actual
-- config/grids.yaml (all three grids, lat/lon match exactly).
--
-- Per 05-startup-registry.md: provisioning must land here BEFORE that
-- grid's telemetry publisher is enabled. An unprovisioned grid_id is
-- rejected at admission (failure_stage = 'grid_validation'), not
-- auto-created.
INSERT INTO iot_data.grids (grid_id, lat, lon) VALUES
    ('grid01', 6.9271, 79.8612),
    ('grid02', 9.6615, 80.0255),
    ('grid03', 7.8731, 80.6550)
ON CONFLICT (grid_id) DO NOTHING;

-- +goose Down

DELETE FROM iot_data.grids WHERE grid_id IN ('grid01', 'grid02', 'grid03');