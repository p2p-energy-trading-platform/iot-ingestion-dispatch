package ingestion

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

func TestHouseIdentityErrorIsPermanentAndRecordedAsProvisioning(t *testing.T) {
	err := fmt.Errorf("persist heartbeat for h: %w: house %q is not registered", domain.ErrHouseIdentity, "h")

	if !isPermanent(err) {
		t.Fatal("a house identity failure must be permanent")
	}
	if got := failureStage(err); got != StageGridValidation {
		t.Fatalf("failureStage = %q, want %q", got, StageGridValidation)
	}
}

func TestHouseIdentityFailureIsNotRetriedAndIsRecorded(t *testing.T) {
	var calls atomic.Int64
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			calls.Add(1)
			return fmt.Errorf("persist heartbeat: %w: house %q is not registered", domain.ErrHouseIdentity, "h")
		}
		return nil
	})

	ffEnqueue(t, rig, 0, 3)
	ffEventually(t, func() bool { return len(rig.committer.committed()) == 3 }, "all three records committed")

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times for a permanent identity failure, want 1", calls.Load())
	}
	failures := rig.recorder.recorded()
	if len(failures) != 1 || failures[0].Offset != 1 || failures[0].Attempts != 1 || failures[0].Stage != StageGridValidation {
		t.Fatalf("unexpected failures: %+v", failures)
	}
	ffAssertNoFatal(t, rig)
}
