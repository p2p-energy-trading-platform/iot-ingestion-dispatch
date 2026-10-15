package postgres

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
)

func TestRecordFailureAcceptsHandlerPanicStageAndSanitizesReason(t *testing.T) {
	store := integrationStore(t)
	topic := failureTestTopic(t, store, "sanitize")
	ctx := context.Background()

	// NUL bytes, invalid UTF-8 and a huge message: none may make the write fail.
	reason := "bad\x00value \xff\xfe " + strings.Repeat("é", 5000)

	err := store.RecordFailure(ctx, ingestion.IngestionFailure{
		Topic: topic, Partition: 0, Offset: 1,
		RawPayload: []byte("{}"), PayloadSHA256: "abc",
		Stage: ingestion.StageHandlerPanic, Reason: reason, Attempts: 1,
	})
	if err != nil {
		t.Fatalf("a handler_panic failure with a hostile reason must be recordable: %v", err)
	}

	var stored string
	if err := store.pool.QueryRow(ctx,
		`SELECT error_reason FROM iot_data.ingestion_failures WHERE kafka_topic = $1`, topic).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(stored) || strings.ContainsRune(stored, 0) {
		t.Fatal("stored reason is not clean UTF-8 text")
	}
	if n := utf8.RuneCountInString(stored); n > maxReasonRunes+1 {
		t.Fatalf("stored reason has %d runes, want at most %d", n, maxReasonRunes+1)
	}
}
