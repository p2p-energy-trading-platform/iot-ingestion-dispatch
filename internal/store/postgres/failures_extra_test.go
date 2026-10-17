package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
)

func failureTestTopic(t *testing.T, store *Store, label string) string {
	t.Helper()
	topic := fmt.Sprintf("test.failures.%s.%d", label, time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DELETE FROM iot_data.ingestion_failures WHERE kafka_topic = $1`, topic)
	})
	return topic
}

func TestRecordFailureAcceptsNilKeyAndPayload(t *testing.T) {
	store := integrationStore(t)
	topic := failureTestTopic(t, store, "nil")

	err := store.RecordFailure(context.Background(), ingestion.IngestionFailure{
		Topic: topic, Partition: 0, Offset: 7,
		Key: nil, RawPayload: nil,
		PayloadSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Stage:         ingestion.StageDecode,
		Reason:        "empty payload",
		Attempts:      1,
	})
	if err != nil {
		t.Fatalf("a record with no key and no value must still be recordable: %v", err)
	}

	var n, length int
	if err := store.pool.QueryRow(context.Background(),
		`SELECT count(*), coalesce(max(length(raw_payload)), -1) FROM iot_data.ingestion_failures WHERE kafka_topic = $1`,
		topic).Scan(&n, &length); err != nil {
		t.Fatal(err)
	}
	if n != 1 || length != 0 {
		t.Fatalf("rows = %d, payload length = %d, want 1 and 0", n, length)
	}
}

func TestRecordFailureReplayUpdatesInsteadOfDuplicating(t *testing.T) {
	store := integrationStore(t)
	topic := failureTestTopic(t, store, "replay")
	ctx := context.Background()

	rec := ingestion.IngestionFailure{
		Topic: topic, Partition: 2, Offset: 41,
		Key: []byte("k"), RawPayload: []byte("{}"),
		PayloadSHA256: "abc", Stage: ingestion.StageTimescaleWrite,
		Reason: "db down", Attempts: 1,
	}
	if err := store.RecordFailure(ctx, rec); err != nil {
		t.Fatal(err)
	}
	rec.Attempts = 3
	if err := store.RecordFailure(ctx, rec); err != nil {
		t.Fatal(err)
	}

	var n, attempts int
	var first, last time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*), max(attempt_count), min(first_failed_at), max(last_failed_at)
		   FROM iot_data.ingestion_failures WHERE kafka_topic = $1`, topic).Scan(&n, &attempts, &first, &last); err != nil {
		t.Fatal(err)
	}
	if n != 1 || attempts != 3 {
		t.Fatalf("rows = %d, attempts = %d, want 1 and 3", n, attempts)
	}
	if last.Before(first) {
		t.Fatalf("last_failed_at %v is before first_failed_at %v", last, first)
	}
}

func TestApplyHeartbeatUnknownHouseIsHouseIdentityError(t *testing.T) {
	store := integrationStore(t)

	hb := domain.Heartbeat{
		SchemaVersion: "1.0",
		GridID:        "gridnotthere",
		HouseID:       "gridnotthere-house0001",
		MeterID:       "meter-gridnotthere-house0001",
		Status:        "online",
		DeviceClass:   domain.DeviceClassConsumer,
	}
	err := store.ApplyHeartbeat(context.Background(), hb, time.Now().UTC())
	if !errors.Is(err, domain.ErrHouseIdentity) {
		t.Fatalf("error = %v, want ErrHouseIdentity", err)
	}
}
