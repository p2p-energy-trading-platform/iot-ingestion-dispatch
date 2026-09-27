-- Seed data for known grids, intentionally kept OUT of migrations/ per
-- review feedback: seed data should not live alongside schema migrations.
-- This is a temporary location, not (currently) run through goose - the
-- file previously lived at migrations/00003_seed_known_grids.sql and was
-- already applied there as goose version 3 on some environments; keeping
-- it goose-formatted here would risk a future real migration also
-- claiming version 3 in the same shared goose_db_version tracking table.
--
-- Run manually against the target database:
--   psql "$POSTGRES_URL" -f migrations/seed/00003_seed_known_grids.sql
--
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
