-- +goose Up

-- A panic inside a topic handler is contained and recorded like any other
-- permanent failure, under its own stage so it can be alerted on separately.
ALTER TABLE iot_data.ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_failure_stage_check;

ALTER TABLE iot_data.ingestion_failures
    ADD CONSTRAINT ingestion_failures_failure_stage_check
    CHECK (failure_stage IN ('decode', 'grid_validation', 'redis_write', 'timescale_write', 'handler_panic'));

-- +goose Down

-- Rows with the new stage cannot exist under the older constraint, so they
-- are removed before it is restored.
DELETE FROM iot_data.ingestion_failures WHERE failure_stage = 'handler_panic';

ALTER TABLE iot_data.ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_failure_stage_check;

ALTER TABLE iot_data.ingestion_failures
    ADD CONSTRAINT ingestion_failures_failure_stage_check
    CHECK (failure_stage IN ('decode', 'grid_validation', 'redis_write', 'timescale_write'));
