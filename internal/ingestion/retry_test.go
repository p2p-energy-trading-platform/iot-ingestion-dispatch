package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

func testRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxTries:        3,
		InitialInterval: time.Millisecond,
		MaxInterval:     time.Millisecond,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
		stage     string
	}{
		{
			name:      "validation",
			err:       fmt.Errorf("decode meter reading: %w", domain.NewValidationError("payload", "invalid JSON")),
			permanent: true,
			stage:     StageDecode,
		},
		{
			name:      "unknown grid",
			err:       fmt.Errorf("%w: %q", admission.ErrUnknownGrid, "grid99"),
			permanent: true,
			stage:     StageGridValidation,
		},
		{
			name:      "integrity conflict",
			err:       fmt.Errorf("persist meter reading m seq 1: %w", domain.ErrIntegrityConflict),
			permanent: true,
			stage:     StageTimescaleWrite,
		},
		{
			name:      "timescale blip",
			err:       errors.New("persist meter reading m seq 1: connection reset"),
			permanent: false,
			stage:     StageTimescaleWrite,
		},
		{
			name:      "redis timeout",
			err:       errors.New("update latest state for m: redis: i/o timeout"),
			permanent: false,
			stage:     StageRedisWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPermanent(tt.err); got != tt.permanent {
				t.Fatalf("isPermanent = %v, want %v", got, tt.permanent)
			}
			if got := failureStage(tt.err); got != tt.stage {
				t.Fatalf("failureStage = %q, want %q", got, tt.stage)
			}
		})
	}
}

func TestRetryHandlePermanentDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	permanent := domain.NewValidationError("grid_id", "required")

	attempts, err := retryHandle(context.Background(), testRetryPolicy(), discardLogger(), func() error {
		calls.Add(1)
		return permanent
	})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
	if attempts != 1 || calls.Load() != 1 {
		t.Fatalf("attempts=%d calls=%d, want 1 (permanent must not burn retries)", attempts, calls.Load())
	}
}

func TestRetryHandleTransientThenSuccess(t *testing.T) {
	var calls atomic.Int32
	attempts, err := retryHandle(context.Background(), testRetryPolicy(), discardLogger(), func() error {
		n := calls.Add(1)
		if n < 2 {
			return errors.New("redis: timeout")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRetryHandleExhaustsTransientBudget(t *testing.T) {
	boom := errors.New("timescale: connection refused")
	var calls atomic.Int32
	attempts, err := retryHandle(context.Background(), testRetryPolicy(), discardLogger(), func() error {
		calls.Add(1)
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3", attempts, calls.Load())
	}
}

func TestRetryHandleStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	_, err := retryHandle(ctx, testRetryPolicy(), discardLogger(), func() error {
		if calls.Add(1) == 1 {
			cancel()
		}
		return errors.New("redis: timeout")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls.Load() > 2 {
		t.Fatalf("calls = %d, cancel should stop further retries", calls.Load())
	}
}

func TestPayloadSHA256IsStable(t *testing.T) {
	got := payloadSHA256([]byte(`{"meter_id":"m"}`))
	again := payloadSHA256([]byte(`{"meter_id":"m"}`))
	if got != again || len(got) != 64 {
		t.Fatalf("hash = %q (len %d), not a stable sha256 hex", got, len(got))
	}
	if payloadSHA256([]byte(`other`)) == got {
		t.Fatal("different payloads hashed equal")
	}
}
