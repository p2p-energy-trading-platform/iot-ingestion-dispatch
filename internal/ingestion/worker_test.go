package ingestion

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const workerTestTopic = "test.topic"

// ---- helpers ---------------------------------------------------------

type workerTestHandler func(ctx context.Context, offset int64) error

func (h workerTestHandler) HandleRecord(ctx context.Context, _, value []byte) error {
	offset, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return err
	}
	return h(ctx, offset)
}

type workerTestCommitter struct {
	mu    sync.Mutex
	calls [][]int64
	err   error
}

func (c *workerTestCommitter) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	offsets := make([]int64, len(rs))
	for i, r := range rs {
		offsets[i] = r.Offset
	}
	c.calls = append(c.calls, offsets)
	return c.err
}

func (c *workerTestCommitter) offsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var all []int64
	for _, call := range c.calls {
		all = append(all, call...)
	}
	return all
}

func (c *workerTestCommitter) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

type workerTestLog struct {
	mu      sync.Mutex
	offsets []int64
}

func (l *workerTestLog) add(o int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.offsets = append(l.offsets, o)
}

func (l *workerTestLog) snapshot() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.offsets)
}

type workerTestEnv struct {
	router    *Router
	committer *workerTestCommitter
	slots     chan struct{}
	fatal     chan error
}

func newWorkerTestEnv(slotCount int, h workerTestHandler) *workerTestEnv {
	router := NewRouter()
	if h != nil {
		router.Register(workerTestTopic, h)
	}
	return &workerTestEnv{
		router:    router,
		committer: &workerTestCommitter{},
		slots:     make(chan struct{}, slotCount),
		fatal:     make(chan error, 8),
	}
}

func (e *workerTestEnv) startWorker(t *testing.T, partition int32, queueDepth int) *partitionWorker {
	t.Helper()
	w := newPartitionWorker(context.Background(),
		topicPartition{topic: workerTestTopic, partition: partition},
		workerDeps{
			router:    e.router,
			committer: e.committer,
			logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			slots:     e.slots,
			onFatal: func(err error) {
				select {
				case e.fatal <- err:
				default:
				}
			},
			queueDepth: queueDepth,
		})
	w.start()
	t.Cleanup(func() { w.stop(true) })
	return w
}

func workerTestRecords(partition int32, from, count int64) []*kgo.Record {
	recs := make([]*kgo.Record, 0, count)
	for i := int64(0); i < count; i++ {
		off := from + i
		recs = append(recs, &kgo.Record{
			Topic:     workerTestTopic,
			Partition: partition,
			Offset:    off,
			Value:     []byte(strconv.FormatInt(off, 10)),
		})
	}
	return recs
}

func eventually(t *testing.T, cond func() bool, msg string) {
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

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func waitFatal(t *testing.T, env *workerTestEnv) error {
	t.Helper()
	select {
	case err := <-env.fatal:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fatal error")
		return nil
	}
}

func assertNoFatal(t *testing.T, env *workerTestEnv) {
	t.Helper()
	select {
	case err := <-env.fatal:
		t.Fatalf("unexpected fatal error: %v", err)
	default:
	}
}

func assertOffsets(t *testing.T, name string, got []int64, want ...int64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s offsets = %v, want %v", name, got, want)
	}
}

// ---- tests -----------------------------------------------------------

func TestWorkerProcessesRecordsInOrder(t *testing.T) {
	var seen workerTestLog
	env := newWorkerTestEnv(4, func(_ context.Context, o int64) error {
		seen.add(o)
		return nil
	})
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 5)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(env.committer.offsets()) == 5 }, "commit of 5 records")

	assertOffsets(t, "handled", seen.snapshot(), 0, 1, 2, 3, 4)
	assertOffsets(t, "committed", env.committer.offsets(), 0, 1, 2, 3, 4)
}

func TestWorkerPreservesOrderAcrossBatches(t *testing.T) {
	var seen workerTestLog
	env := newWorkerTestEnv(4, func(_ context.Context, o int64) error {
		seen.add(o)
		return nil
	})
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.enqueue(context.Background(), workerTestRecords(0, 3, 3)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(env.committer.offsets()) == 6 }, "commit of 6 records")

	assertOffsets(t, "handled", seen.snapshot(), 0, 1, 2, 3, 4, 5)
	assertOffsets(t, "committed", env.committer.offsets(), 0, 1, 2, 3, 4, 5)
}

func TestWorkerCommitsOnlyFinishedPrefixOnHandlerError(t *testing.T) {
	errBoom := errors.New("boom")
	var seen workerTestLog
	env := newWorkerTestEnv(4, func(_ context.Context, o int64) error {
		if o == 3 {
			return errBoom
		}
		seen.add(o)
		return nil
	})
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 6)); err != nil {
		t.Fatal(err)
	}

	err := waitFatal(t, env)
	if !errors.Is(err, errBoom) {
		t.Fatalf("fatal error = %v, want it to wrap errBoom", err)
	}
	waitSignal(t, w.done)

	assertOffsets(t, "handled", seen.snapshot(), 0, 1, 2)
	assertOffsets(t, "committed", env.committer.offsets(), 0, 1, 2)
}

func TestWorkerMissingHandlerIsFatal(t *testing.T) {
	env := newWorkerTestEnv(4, nil) // nothing registered
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 2)); err != nil {
		t.Fatal(err)
	}

	err := waitFatal(t, env)
	if !errors.Is(err, ErrNoHandler) {
		t.Fatalf("fatal error = %v, want ErrNoHandler", err)
	}
	waitSignal(t, w.done)

	if got := env.committer.offsets(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing", got)
	}
}

func TestWorkerGracefulStopFinishesCurrentRecord(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var seen workerTestLog

	env := newWorkerTestEnv(4, func(_ context.Context, o int64) error {
		if o == 1 {
			close(started)
			<-release
		}
		seen.add(o)
		return nil
	})
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 4)); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)

	stopped := make(chan struct{})
	go func() {
		w.stop(false)
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("stop returned while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitSignal(t, stopped)

	assertOffsets(t, "handled", seen.snapshot(), 0, 1)
	assertOffsets(t, "committed", env.committer.offsets(), 0, 1)
	assertNoFatal(t, env)
}

func TestWorkerAbortStopCancelsInFlightHandler(t *testing.T) {
	started := make(chan struct{})

	env := newWorkerTestEnv(4, func(ctx context.Context, o int64) error {
		if o == 1 {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 4)); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)

	w.stop(true)

	assertOffsets(t, "committed", env.committer.offsets(), 0)
	assertNoFatal(t, env)
}

func TestWorkerSharedSlotsBoundConcurrency(t *testing.T) {
	var mu sync.Mutex
	running, maxRunning := 0, 0

	env := newWorkerTestEnv(2, func(_ context.Context, _ int64) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()

		time.Sleep(30 * time.Millisecond)

		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})

	for p := int32(0); p < 4; p++ {
		w := env.startWorker(t, p, 1)
		if err := w.enqueue(context.Background(), workerTestRecords(p, 0, 1)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return env.committer.callCount() == 4 }, "all 4 partitions committed")

	mu.Lock()
	defer mu.Unlock()
	if maxRunning > 2 {
		t.Fatalf("max concurrent handlers = %d, want <= 2", maxRunning)
	}
}

func TestWorkerEnqueueUnblocksOnContextCancel(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	env := newWorkerTestEnv(4, func(_ context.Context, _ int64) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	w := env.startWorker(t, 0, 1)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	// Batch 1 is picked up and blocks in the handler; batch 2 fills the
	// single queue slot; batch 3 must block.
	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)
	if err := w.enqueue(context.Background(), workerTestRecords(0, 1, 1)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- w.enqueue(ctx, workerTestRecords(0, 2, 1)) }()

	select {
	case err := <-result:
		t.Fatalf("enqueue returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("enqueue error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue did not unblock after cancel")
	}
}

func TestWorkerEnqueueAfterStopReturnsErrWorkerStopped(t *testing.T) {
	env := newWorkerTestEnv(4, func(context.Context, int64) error { return nil })
	w := env.startWorker(t, 0, 0) // unbuffered: no send can succeed once stopped

	w.stop(false)

	err := w.enqueue(context.Background(), workerTestRecords(0, 0, 1))
	if !errors.Is(err, errWorkerStopped) {
		t.Fatalf("enqueue error = %v, want errWorkerStopped", err)
	}
}

func TestWorkerCommitFailureIsNotFatal(t *testing.T) {
	var seen workerTestLog
	env := newWorkerTestEnv(4, func(_ context.Context, o int64) error {
		seen.add(o)
		return nil
	})
	env.committer.err = errors.New("commit rejected")
	w := env.startWorker(t, 0, 2)

	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 2)); err != nil {
		t.Fatal(err)
	}
	if err := w.enqueue(context.Background(), workerTestRecords(0, 2, 2)); err != nil {
		t.Fatal(err)
	}

	eventually(t, func() bool { return len(seen.snapshot()) == 4 }, "both batches handled")
	eventually(t, func() bool { return env.committer.callCount() == 2 }, "both commits attempted")
	assertNoFatal(t, env)
}

func TestWorkerStopIsIdempotent(t *testing.T) {
	env := newWorkerTestEnv(4, func(context.Context, int64) error { return nil })
	w := env.startWorker(t, 0, 1)

	w.stop(false)
	w.stop(false)
	w.stop(true)
}

func TestWorkerStopWhileWaitingForSlotDropsBatchUncommitted(t *testing.T) {
	var seen workerTestLog
	env := newWorkerTestEnv(1, func(_ context.Context, o int64) error {
		seen.add(o)
		return nil
	})
	env.slots <- struct{}{} // occupy the only slot so the worker must wait

	w := env.startWorker(t, 0, 1)
	if err := w.enqueue(context.Background(), workerTestRecords(0, 0, 3)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	w.stop(false)

	if got := seen.snapshot(); len(got) != 0 {
		t.Fatalf("handled %v, want nothing", got)
	}
	if got := env.committer.offsets(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing", got)
	}
}

func TestConfigWithDefaults(t *testing.T) {
	got := Config{}.withDefaults()
	if got.MaxConcurrentHandlers != defaultMaxConcurrentHandlers {
		t.Fatalf("MaxConcurrentHandlers = %d, want %d", got.MaxConcurrentHandlers, defaultMaxConcurrentHandlers)
	}
	if got.PartitionQueueDepth != defaultPartitionQueueDepth {
		t.Fatalf("PartitionQueueDepth = %d, want %d", got.PartitionQueueDepth, defaultPartitionQueueDepth)
	}

	kept := Config{MaxConcurrentHandlers: 3, PartitionQueueDepth: 7}.withDefaults()
	if kept.MaxConcurrentHandlers != 3 || kept.PartitionQueueDepth != 7 {
		t.Fatalf("explicit values overwritten: %+v", kept)
	}
}
