package ingestion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FailureRecorder is the application port for durable ingestion-failure
// records. The partition worker must not commit a failed Kafka offset
// until this call succeeds.
type FailureRecorder interface {
	RecordFailure(ctx context.Context, rec IngestionFailure) error
}

// IngestionFailure is one exhausted or permanent handler failure, keyed
// by Kafka coordinates and failure stage so replays upsert rather than
// multiply rows.
type IngestionFailure struct {
	Topic         string
	Partition     int32
	Offset        int64
	Key           []byte
	RawPayload    []byte
	PayloadSHA256 string
	Stage         string
	Reason        string
	Attempts      int
}

func newIngestionFailure(rec *kgo.Record, handlerErr error, attempts int) IngestionFailure {
	return IngestionFailure{
		Topic:         rec.Topic,
		Partition:     rec.Partition,
		Offset:        rec.Offset,
		Key:           rec.Key,
		RawPayload:    rec.Value,
		PayloadSHA256: payloadSHA256(rec.Value),
		Stage:         failureStage(handlerErr),
		Reason:        handlerErr.Error(),
		Attempts:      attempts,
	}
}

func payloadSHA256(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
