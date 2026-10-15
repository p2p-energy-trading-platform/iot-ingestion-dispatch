package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const ffTopic = "test.failure.flow"

type ffHandler struct {
	fn func(ctx context.Context, offset int64) error
}

func (h ffHandler) HandleRecord(ctx context.Context, _, value []byte) error {
	off, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return err
	}
	return h.fn(ctx, off)
}

type ffCommitter struct {
	mu      sync.Mutex
	offsets []int64
}

func (c *ffCommitter) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rs {
		c.offsets = append(c.offsets, r.Offset)
	}
	return nil
}

func (c *ffCommitter) committed() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.offsets)
}

type ffRecorder struct {
	mu       sync.Mutex
	failures []IngestionFailure
	err      error
}

func (r *ffRecorder) RecordFailure(_ context.Context, f IngestionFailure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.failures = append(r.failures, f)
	return nil
}

func (r *ffRecorder) recorded() []IngestionFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.failures)
}

type ffRig struct {
	worker    *partitionWorker
	committer *ffCommitter
	recorder  *ffRecorder
	fatal     chan error
}

func newFFRig(t *testing.T, policy RetryPolicy, registerHandler bool, fn func(ctx context.Context, offset int64) error) *ffRig {
	t.Helper()

	router := NewRouter()
	if registerHandler {
		router.Register(ffTopic, ffHandler{fn: fn})
	}

	rig := &ffRig{
		committer: &ffCommitter{},
		recorder:  &ffRecorder{},
		fatal:     make(chan error, 4),
	}
	rig.worker = newPartitionWorker(context.Background(),
		topicPartition{topic: ffTopic, partition: 0},
		workerDeps{
			router:    router,
			committer: rig.committer,
			recorder:  rig.recorder,
			retry:     policy,
			logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			slots:     make(chan struct{}, 2),
			onFatal: func(err error) {
				select {
				case rig.fatal <- err:
				default:
				}
			},
			queueDepth: 2,
		})
	rig.worker.start()
	t.Cleanup(func() { rig.worker.stop(true) })
	return rig
}

func ffRecords(from, count int64) []*kgo.Record {
	recs := make([]*kgo.Record, 0, count)
	for i := int64(0); i < count; i++ {
		off := from + i
		recs = append(recs, &kgo.Record{
			Topic: ffTopic, Partition: 0, Offset: off,
			Key: []byte("k"), Value: []byte(strconv.FormatInt(off, 10)),
		})
	}
	return recs
}

func ffEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func ffWaitFatal(t *testing.T, rig *ffRig) error {
	t.Helper()
	select {
	case err := <-rig.fatal:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fatal error")
		return nil
	}
}

func ffAssertNoFatal(t *testing.T, rig *ffRig) {
	t.Helper()
	select {
	case err := <-rig.fatal:
		t.Fatalf("unexpected fatal error: %v", err)
	default:
	}
}

func ffEnqueue(t *testing.T, rig *ffRig, from, count int64) {
	t.Helper()
	if err := rig.worker.enqueue(context.Background(), ffRecords(from, count)); err != nil {
		t.Fatal(err)
	}
}

// A retry would wait 5s, so these tests also prove "no retry" by finishing fast.
var ffSlowPolicy = RetryPolicy{MaxTries: 3, InitialInterval: 5 * time.Second, MaxInterval: 5 * time.Second}

var ffFastPolicy = RetryPolicy{MaxTries: 3, InitialInterval: 5 * time.Millisecond, MaxInterval: 10 * time.Millisecond}

func TestFailureFlowPermanentErrorIsRecordedOnceAndCommitted(t *testing.T) {
	var calls atomic.Int64
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			calls.Add(1)
			return domain.NewValidationError("seq", "bad")
		}
		return nil
	})

	ffEnqueue(t, rig, 0, 3)
	ffEventually(t, func() bool { return len(rig.committer.committed()) == 3 }, "all three records committed")

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0, 1, 2}) {
		t.Fatalf("committed %v, want [0 1 2]", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times for a permanent error, want 1", calls.Load())
	}

	failures := rig.recorder.recorded()
	if len(failures) != 1 {
		t.Fatalf("recorded %d failures, want 1", len(failures))
	}
	f := failures[0]
	if f.Offset != 1 || f.Attempts != 1 || f.Stage != StageDecode || len(f.PayloadSHA256) != 64 {
		t.Fatalf("unexpected failure record: %+v", f)
	}
	ffAssertNoFatal(t, rig)
}

func TestFailureFlowTransientErrorRetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int64
	rig := newFFRig(t, ffFastPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 && calls.Add(1) < 3 {
			return errors.New("db hiccup")
		}
		return nil
	})

	ffEnqueue(t, rig, 0, 3)
	ffEventually(t, func() bool { return len(rig.committer.committed()) == 3 }, "all three records committed")

	if calls.Load() != 3 {
		t.Fatalf("handler ran %d times, want 3", calls.Load())
	}
	if got := rig.recorder.recorded(); len(got) != 0 {
		t.Fatalf("recorded %d failures for a record that eventually succeeded", len(got))
	}
	ffAssertNoFatal(t, rig)
}

func TestFailureFlowExhaustedRetriesAreRecordedAndCommitted(t *testing.T) {
	var calls atomic.Int64
	rig := newFFRig(t, ffFastPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			calls.Add(1)
			return errors.New("db down")
		}
		return nil
	})

	ffEnqueue(t, rig, 0, 3)
	ffEventually(t, func() bool { return len(rig.committer.committed()) == 3 }, "all three records committed")

	if calls.Load() != 3 {
		t.Fatalf("handler ran %d times, want exactly MaxTries (3)", calls.Load())
	}
	failures := rig.recorder.recorded()
	if len(failures) != 1 || failures[0].Attempts != 3 || failures[0].Stage != StageTimescaleWrite {
		t.Fatalf("unexpected failures: %+v", failures)
	}
	ffAssertNoFatal(t, rig)
}

func TestFailureFlowRecordingFailureLeavesOffsetUncommitted(t *testing.T) {
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			return domain.NewValidationError("seq", "bad")
		}
		return nil
	})
	rig.recorder.err = errors.New("failure table unavailable")

	ffEnqueue(t, rig, 0, 3)

	err := ffWaitFatal(t, rig)
	if !strings.Contains(err.Error(), "failure table unavailable") {
		t.Fatalf("fatal error %q does not mention the recorder error", err)
	}
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("fatal error %q lost the original handler error", err)
	}
	select {
	case <-rig.worker.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after a recording failure")
	}

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("committed %v, want only [0] (the failed record and later ones stay uncommitted)", got)
	}
}

func TestFailureFlowMissingHandlerIsFatalAndNotRetriedOrRecorded(t *testing.T) {
	rig := newFFRig(t, ffSlowPolicy, false, nil)

	ffEnqueue(t, rig, 0, 2)

	err := ffWaitFatal(t, rig)
	if !errors.Is(err, ErrNoHandler) {
		t.Fatalf("fatal error = %v, want ErrNoHandler", err)
	}
	if got := rig.recorder.recorded(); len(got) != 0 {
		t.Fatalf("a wiring bug was recorded as a data failure: %+v", got)
	}
	if got := rig.committer.committed(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing", got)
	}
}

func TestFailureFlowStopInterruptsBackoffAndLeavesRecordUncommitted(t *testing.T) {
	firstAttempt := make(chan struct{}, 1)
	rig := newFFRig(t, ffSlowPolicy, true, func(context.Context, int64) error {
		select {
		case firstAttempt <- struct{}{}:
		default:
		}
		return errors.New("db down")
	})

	ffEnqueue(t, rig, 0, 2)
	select {
	case <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}

	stopped := make(chan struct{})
	go func() {
		rig.worker.stop(false)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not interrupt the backoff wait")
	}

	if got := rig.recorder.recorded(); len(got) != 0 {
		t.Fatalf("an interrupted record was recorded as a failure: %+v", got)
	}
	if got := rig.committer.committed(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing", got)
	}
	ffAssertNoFatal(t, rig)
}

func TestFailureClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
		stage     string
	}{
		{"validation", domain.NewValidationError("f", "r"), true, StageDecode},
		{"unknown grid", fmt.Errorf("%w: %q", admission.ErrUnknownGrid, "grid9"), true, StageGridValidation},
		{"grid not provisioned", fmt.Errorf("%w: grid gone", domain.ErrGridNotProvisioned), true, StageGridValidation},
		{"integrity conflict", fmt.Errorf("persist: %w", domain.ErrIntegrityConflict), true, StageTimescaleWrite},
		{"no handler", fmt.Errorf("%w: %q", ErrNoHandler, "t"), true, StageTimescaleWrite},
		{"plain error", errors.New("connection reset"), false, StageTimescaleWrite},
		{"redis latest", fmt.Errorf("update latest state for m: %w", errors.New("timeout")), false, StageRedisWrite},
		{"redis heartbeat", fmt.Errorf("update heartbeat state for h: %w", errors.New("timeout")), false, StageRedisWrite},
		{"redis prefix", errors.New("redis: set_latest_if_newer failed"), false, StageRedisWrite},
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

func TestRetryPolicyWithDefaultsIsAlwaysBounded(t *testing.T) {
	tests := []struct {
		name string
		in   RetryPolicy
		want RetryPolicy
	}{
		{"zero value", RetryPolicy{}, DefaultRetryPolicy()},
		{"negative tries", RetryPolicy{MaxTries: -1}, DefaultRetryPolicy()},
		{"too many tries", RetryPolicy{MaxTries: 1000}, RetryPolicy{MaxTries: maxRetryTries, InitialInterval: defaultRetryInitialInterval, MaxInterval: defaultRetryMaxInterval}},
		{"max below initial", RetryPolicy{MaxTries: 2, InitialInterval: time.Second, MaxInterval: time.Millisecond}, RetryPolicy{MaxTries: 2, InitialInterval: time.Second, MaxInterval: time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.withDefaults(); got != tt.want {
				t.Fatalf("withDefaults = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNewConsumerRequiresRecorder(t *testing.T) {
	if _, err := NewConsumer(Config{}, NewRouter(), nil, nil); err == nil {
		t.Fatal("expected an error for a nil failure recorder")
	}
}
