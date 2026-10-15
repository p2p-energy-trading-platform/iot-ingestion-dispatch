package ingestion

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

func dsWaitDone(t *testing.T, w *partitionWorker) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func dsWaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

type dsPanicRecorder struct{}

func (dsPanicRecorder) RecordFailure(context.Context, IngestionFailure) error {
	panic("recorder exploded")
}

// dsStore stands in for an idempotent store: writing the same offset twice
// leaves the same state.
type dsStore struct {
	mu    sync.Mutex
	state map[int64]bool
	calls int
}

func newDSStore() *dsStore { return &dsStore{state: map[int64]bool{}} }

func (s *dsStore) put(off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.state[off] = true
}

func (s *dsStore) snapshot() (distinct, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state), s.calls
}

// ---- panic containment -------------------------------------------------

func TestDeliveryHandlerPanicIsContainedRecordedAndCommitted(t *testing.T) {
	panics := map[string]func(){
		"string value":  func() { panic("boom") },
		"error value":   func() { panic(errors.New("boom error")) },
		"runtime error": func() { s := make([]int, 0); n := len(s) + 3; _ = s[n] },
	}
	for name, doPanic := range panics {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
				if off == 1 {
					calls.Add(1)
					doPanic()
				}
				return nil
			})

			ffEnqueue(t, rig, 0, 3)
			ffEventually(t, func() bool { return len(rig.committer.committed()) == 3 }, "all three records committed")

			if got := rig.committer.committed(); !slices.Equal(got, []int64{0, 1, 2}) {
				t.Fatalf("committed %v, want [0 1 2]", got)
			}
			if calls.Load() != 1 {
				t.Fatalf("handler ran %d times for a panic, want 1 (a panic is permanent)", calls.Load())
			}

			failures := rig.recorder.recorded()
			if len(failures) != 1 {
				t.Fatalf("recorded %d failures, want 1", len(failures))
			}
			f := failures[0]
			if f.Offset != 1 || f.Attempts != 1 || f.Stage != StageHandlerPanic || !strings.Contains(f.Reason, "handler panic") {
				t.Fatalf("unexpected failure record: %+v", f)
			}
			ffAssertNoFatal(t, rig)
		})
	}
}

func TestDeliveryPanicWithFailingRecorderIsFatalAndLeavesOffsetUncommitted(t *testing.T) {
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			panic("boom")
		}
		return nil
	})
	rig.recorder.err = errors.New("failure table unavailable")

	ffEnqueue(t, rig, 0, 3)

	err := ffWaitFatal(t, rig)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("fatal error = %v, want it to wrap ErrHandlerPanic", err)
	}
	if !strings.Contains(err.Error(), "failure table unavailable") {
		t.Fatalf("fatal error %q does not mention the recorder error", err)
	}
	dsWaitDone(t, rig.worker)

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("committed %v, want only [0]", got)
	}
}

func TestDeliveryWorkerLoopPanicIsContained(t *testing.T) {
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			return domain.NewValidationError("seq", "bad")
		}
		return nil
	})
	rig.worker.deps.recorder = dsPanicRecorder{}

	ffEnqueue(t, rig, 0, 3)

	err := ffWaitFatal(t, rig)
	if !errors.Is(err, ErrWorkerPanic) {
		t.Fatalf("fatal error = %v, want ErrWorkerPanic", err)
	}
	dsWaitDone(t, rig.worker)

	if got := rig.committer.committed(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing (the batch in progress is redelivered)", got)
	}
}

func TestDeliveryPanicClassification(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &HandlerPanicError{Topic: "t", Partition: 1, Offset: 2, Value: "boom"})

	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatal("HandlerPanicError must unwrap to ErrHandlerPanic")
	}
	if !isPermanent(err) {
		t.Fatal("a handler panic must be permanent")
	}
	if got := failureStage(err); got != StageHandlerPanic {
		t.Fatalf("failureStage = %q, want %q", got, StageHandlerPanic)
	}
}

func TestHandlerPanicErrorIsBoundedAndValidUTF8(t *testing.T) {
	e := &HandlerPanicError{Topic: "t", Value: strings.Repeat("é", 5000) + "\xff\x00"}

	msg := e.Error()
	if !utf8.ValidString(msg) {
		t.Fatal("panic message is not valid UTF-8")
	}
	if strings.ContainsRune(msg, 0) {
		t.Fatal("panic message contains a NUL byte")
	}
	if n := utf8.RuneCountInString(msg); n > 700 {
		t.Fatalf("panic message has %d runes, want it bounded", n)
	}
}

// ---- contiguous commits --------------------------------------------------

func TestDeliveryMiddleBatchFailureCommitsContiguousPrefixOnly(t *testing.T) {
	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 4 {
			return domain.NewValidationError("seq", "bad")
		}
		return nil
	})
	rig.recorder.err = errors.New("failure table unavailable")

	ffEnqueue(t, rig, 0, 3)
	ffEnqueue(t, rig, 3, 3)

	_ = ffWaitFatal(t, rig)
	dsWaitDone(t, rig.worker)

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0, 1, 2, 3}) {
		t.Fatalf("committed %v, want [0 1 2 3]: the failed record and everything after it stay uncommitted", got)
	}
}

func TestDeliveryNothingCommittedPastFirstUnfinishedRecord(t *testing.T) {
	for k := int64(0); k < 6; k++ {
		t.Run(fmt.Sprintf("fails at %d", k), func(t *testing.T) {
			rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
				if off == k {
					return domain.NewValidationError("seq", "bad")
				}
				return nil
			})
			rig.recorder.err = errors.New("failure table unavailable")

			ffEnqueue(t, rig, 0, 6)
			_ = ffWaitFatal(t, rig)
			dsWaitDone(t, rig.worker)

			want := make([]int64, 0, k)
			for i := int64(0); i < k; i++ {
				want = append(want, i)
			}
			if got := rig.committer.committed(); !slices.Equal(got, want) {
				t.Fatalf("committed %v, want %v", got, want)
			}
		})
	}
}

// ---- bounded drain ---------------------------------------------------------

func TestDeliveryDrainWaitsForRunningRecord(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }

	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			close(started)
			<-release
		}
		return nil
	})
	t.Cleanup(releaseOnce)

	ffEnqueue(t, rig, 0, 4)
	dsWaitSignal(t, started)

	result := make(chan bool, 1)
	go func() { result <- rig.worker.stop(false) }()

	select {
	case <-result:
		t.Fatal("stop returned while a record was still running")
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce()
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("stop reported failure although the record finished")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not return after the record finished")
	}

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0, 1}) {
		t.Fatalf("committed %v, want [0 1]", got)
	}
}

func TestDeliveryDrainTimeoutCancelsCooperativeHandler(t *testing.T) {
	started := make(chan struct{})
	rig := newFFRig(t, ffSlowPolicy, true, func(ctx context.Context, off int64) error {
		if off == 1 {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	rig.worker.deps.drainTimeout = 50 * time.Millisecond

	ffEnqueue(t, rig, 0, 4)
	dsWaitSignal(t, started)

	begin := time.Now()
	if !rig.worker.stop(false) {
		t.Fatal("stop reported failure for a handler that honours cancellation")
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("stop took %v, want it bounded by the drain timeout", elapsed)
	}

	if got := rig.committer.committed(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("committed %v, want [0] (the interrupted record is not finished)", got)
	}
	if got := rig.recorder.recorded(); len(got) != 0 {
		t.Fatalf("an interrupted record was recorded as a failure: %+v", got)
	}
	ffAssertNoFatal(t, rig)
}

func TestDeliveryStuckHandlerIsAbandonedAndNeverCommits(t *testing.T) {
	oldGrace := abortGrace
	abortGrace = 30 * time.Millisecond
	t.Cleanup(func() { abortGrace = oldGrace })

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }

	rig := newFFRig(t, ffSlowPolicy, true, func(_ context.Context, off int64) error {
		if off == 1 {
			close(started)
			<-release // ignores cancellation on purpose
		}
		return nil
	})
	t.Cleanup(releaseOnce)
	rig.worker.deps.drainTimeout = 30 * time.Millisecond

	ffEnqueue(t, rig, 0, 4)
	dsWaitSignal(t, started)

	if rig.worker.stop(false) {
		t.Fatal("stop reported success for a handler that ignores cancellation")
	}

	// The handler finally returns after the worker was abandoned. Records 0
	// and 1 did finish, but an abandoned worker must not commit anything: the
	// partition may belong to someone else by now.
	releaseOnce()
	dsWaitDone(t, rig.worker)

	if got := rig.committer.committed(); len(got) != 0 {
		t.Fatalf("an abandoned worker committed %v, want nothing", got)
	}
	if got := rig.recorder.recorded(); len(got) != 0 {
		t.Fatalf("recorded %+v, want nothing", got)
	}
	ffAssertNoFatal(t, rig)
}

func TestDeliveryStoppedWorkerRejectsNewWorkEvenWithQueueRoom(t *testing.T) {
	rig := newFFRig(t, ffFastPolicy, true, func(context.Context, int64) error { return nil })

	if !rig.worker.stop(false) {
		t.Fatal("stop failed on an idle worker")
	}

	err := rig.worker.enqueue(context.Background(), ffRecords(0, 1))
	if !errors.Is(err, errWorkerStopped) {
		t.Fatalf("enqueue error = %v, want errWorkerStopped", err)
	}
}

// ---- replay after restart -----------------------------------------------------

func TestDeliveryReplayAfterRestartIsIdempotent(t *testing.T) {
	store := newDSStore()
	started := make(chan struct{})

	// "Process" A is cut off while handling record 3.
	a := newFFRig(t, ffFastPolicy, true, func(ctx context.Context, off int64) error {
		if off == 3 {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		store.put(off)
		return nil
	})
	ffEnqueue(t, a, 0, 6)
	dsWaitSignal(t, started)
	a.worker.stop(true)

	if got := a.committer.committed(); !slices.Equal(got, []int64{0, 1, 2}) {
		t.Fatalf("first run committed %v, want [0 1 2]", got)
	}

	// "Process" B restarts and is redelivered everything, as if the commit of
	// 0..2 had been lost as well.
	b := newFFRig(t, ffFastPolicy, true, func(_ context.Context, off int64) error {
		store.put(off)
		return nil
	})
	ffEnqueue(t, b, 0, 6)
	ffEventually(t, func() bool { return len(b.committer.committed()) == 6 }, "replay committed")

	if got := b.committer.committed(); !slices.Equal(got, []int64{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("replay committed %v, want [0 1 2 3 4 5]", got)
	}
	distinct, calls := store.snapshot()
	if distinct != 6 {
		t.Fatalf("store holds %d distinct records, want 6", distinct)
	}
	if calls <= 6 {
		t.Fatalf("store saw %d writes, want more than 6 (the replay must have re-written records)", calls)
	}
	if got := a.recorder.recorded(); len(got) != 0 {
		t.Fatalf("first run recorded failures: %+v", got)
	}
	if got := b.recorder.recorded(); len(got) != 0 {
		t.Fatalf("replay recorded failures: %+v", got)
	}
	ffAssertNoFatal(t, a)
	ffAssertNoFatal(t, b)
}
