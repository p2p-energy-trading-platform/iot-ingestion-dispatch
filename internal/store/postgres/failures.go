package postgres

import (
	"context"
	"fmt"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
)

const upsertIngestionFailureSQL = `
INSERT INTO iot_data.ingestion_failures (
    kafka_topic, kafka_partition, kafka_offset, kafka_key,
    raw_payload, payload_sha256, failure_stage, error_reason, attempt_count
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset, failure_stage)
DO UPDATE SET
    kafka_key      = EXCLUDED.kafka_key,
    raw_payload    = EXCLUDED.raw_payload,
    payload_sha256 = EXCLUDED.payload_sha256,
    error_reason   = EXCLUDED.error_reason,
    attempt_count  = EXCLUDED.attempt_count,
    last_failed_at = now()`

var _ ingestion.FailureRecorder = (*Store)(nil)

// RecordFailure persists an exhausted or permanent ingestion failure.
// Repeat processing of the same Kafka coordinates and stage upserts
// attempt metadata instead of inserting duplicate rows.
func (store *Store) RecordFailure(ctx context.Context, rec ingestion.IngestionFailure) error {
	if rec.Attempts < 1 {
		rec.Attempts = 1
	}
	// raw_payload is NOT NULL, but a nil slice is sent as NULL. A record with
	// no value (for example a tombstone) must still be recordable, or it would
	// block its partition forever.
	if rec.RawPayload == nil {
		rec.RawPayload = []byte{}
	}

	_, err := store.pool.Exec(ctx, upsertIngestionFailureSQL,
		rec.Topic,
		rec.Partition,
		rec.Offset,
		rec.Key,
		rec.RawPayload,
		rec.PayloadSHA256,
		rec.Stage,
		rec.Reason,
		rec.Attempts,
	)
	if err != nil {
		return fmt.Errorf("record ingestion failure %s/%d@%d stage %s: %w",
			rec.Topic, rec.Partition, rec.Offset, rec.Stage, err)
	}
	return nil
}
