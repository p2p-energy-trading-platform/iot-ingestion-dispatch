package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
)

func TestRecordFailureInsertAndUpsert(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()

	rec := ingestion.IngestionFailure{
		Topic:         "iot.meter-readings",
		Partition:     0,
		Offset:        time.Now().UnixNano(),
		Key:           []byte("grid01-house0042"),
		RawPayload:    []byte(`{"bad":true}`),
		PayloadSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Stage:         ingestion.StageDecode,
		Reason:        "validation: field \"payload\": invalid JSON",
		Attempts:      1,
	}

	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx,
			`DELETE FROM iot_data.ingestion_failures WHERE kafka_topic = $1 AND kafka_partition = $2 AND kafka_offset = $3`,
			rec.Topic, rec.Partition, rec.Offset)
	})

	if err := store.RecordFailure(ctx, rec); err != nil {
		t.Fatal(err)
	}

	rec.Attempts = 3
	rec.Reason = "still invalid"
	if err := store.RecordFailure(ctx, rec); err != nil {
		t.Fatal(err)
	}

	var n, attempts int
	var reason, resolution string
	if err := store.pool.QueryRow(ctx, `
		SELECT count(*), max(attempt_count), max(error_reason), max(resolution)
		FROM iot_data.ingestion_failures
		WHERE kafka_topic = $1 AND kafka_partition = $2 AND kafka_offset = $3 AND failure_stage = $4`,
		rec.Topic, rec.Partition, rec.Offset, rec.Stage,
	).Scan(&n, &attempts, &reason, &resolution); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want 1 (upsert, not duplicate)", n)
	}
	if attempts != 3 || reason != "still invalid" || resolution != "unresolved" {
		t.Fatalf("attempt_count=%d reason=%q resolution=%q", attempts, reason, resolution)
	}
}

func TestRecordFailureStoresKafkaCoordinates(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()

	rec := ingestion.IngestionFailure{
		Topic:         "iot.heartbeats",
		Partition:     2,
		Offset:        time.Now().UnixNano(),
		Key:           []byte("k"),
		RawPayload:    []byte(`{}`),
		PayloadSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Stage:         ingestion.StageGridValidation,
		Reason:        fmt.Sprintf("%s: %q", "admission: unknown grid_id", "nope"),
		Attempts:      1,
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx,
			`DELETE FROM iot_data.ingestion_failures WHERE kafka_offset = $1`, rec.Offset)
	})

	if err := store.RecordFailure(ctx, rec); err != nil {
		t.Fatal(err)
	}

	var topic, stage, hash string
	var partition int32
	if err := store.pool.QueryRow(ctx, `
		SELECT kafka_topic, kafka_partition, failure_stage, payload_sha256
		FROM iot_data.ingestion_failures WHERE kafka_offset = $1`, rec.Offset,
	).Scan(&topic, &partition, &stage, &hash); err != nil {
		t.Fatal(err)
	}
	if topic != rec.Topic || partition != rec.Partition || stage != rec.Stage || hash != rec.PayloadSHA256 {
		t.Fatalf("stored %+v/%d %s %s", topic, partition, stage, hash)
	}
}
