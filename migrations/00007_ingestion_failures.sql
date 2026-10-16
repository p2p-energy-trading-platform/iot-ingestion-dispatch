-- +goose Up

CREATE TABLE iot_data.ingestion_failures (
    id              BIGSERIAL PRIMARY KEY,
    kafka_topic     TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    kafka_key       BYTEA,
    raw_payload     BYTEA NOT NULL,
    payload_sha256  TEXT NOT NULL,
    failure_stage   TEXT NOT NULL
                    CHECK (failure_stage IN ('decode', 'grid_validation', 'redis_write', 'timescale_write')),
    error_reason    TEXT NOT NULL,
    attempt_count   INT NOT NULL CHECK (attempt_count >= 1),
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolution      TEXT NOT NULL DEFAULT 'unresolved'
                    CHECK (resolution IN ('unresolved', 'replayed', 'discarded')),
    resolved_at     TIMESTAMPTZ,
    resolved_by     TEXT,
    UNIQUE (kafka_topic, kafka_partition, kafka_offset, failure_stage)
);

CREATE INDEX idx_ingestion_failures_unresolved
    ON iot_data.ingestion_failures (last_failed_at DESC)
    WHERE resolution = 'unresolved';

-- +goose Down

DROP INDEX IF EXISTS iot_data.idx_ingestion_failures_unresolved;
DROP TABLE IF EXISTS iot_data.ingestion_failures;
