package ingestion

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestConsumer(t *testing.T, h workerTestHandler) (*Consumer, *workerTestCommitter) {
	t.Helper()

	router := NewRouter()
	if h != nil {
		router.Register(workerTestTopic, h)
	}
	committer := &workerTestCommitter{}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	fatalCtx, fatalCancel := context.WithCancel(context.Background())

	c := &Consumer{
		router:       router,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		config:       Config{}.withDefaults(),
		committer:    committer,
		slots:        make(chan struct{}, 4),
		workers:      make(map[topicPartition]*partitionWorker),
		workerCtx:    workerCtx,
		workerCancel: workerCancel,
		fatalCtx:     fatalCtx,
		fatalCancel:  fatalCancel,
	}

	t.Cleanup(func() {
		c.stopWorkers(c.takeWorkers(nil), true)
		workerCancel()
		fatalCancel()
	})

	return c, committer
}

func testFetches(partition int32, recs []*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      workerTestTopic,
			Partitions: []kgo.FetchPartition{{Partition: partition, Records: recs}},
		}},
	}}
}

func TestConsumerAssignCreatesWorkerPerPartition(t *testing.T) {
	c, _ := newTestConsumer(t, func(context.Context, int64) error { return nil })

	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0, 1, 2}})

	for p := int32(0); p < 3; p++ {
		if c.workerFor(topicPartition{topic: workerTestTopic, partition: p}) == nil {
			t.Fatalf("no worker for partition %d", p)
		}
	}

	// Re-assigning must not replace a running worker.
	before := c.workerFor(topicPartition{topic: workerTestTopic, partition: 0})
	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})
	if c.workerFor(topicPartition{topic: workerTestTopic, partition: 0}) != before {
		t.Fatal("existing worker was replaced on re-assign")
	}
}

func TestConsumerDispatchRoutesPerPartitionInOrder(t *testing.T) {
	c, committer := newTestConsumer(t, func(context.Context, int64) error { return nil })
	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0, 1}})

	// Distinct offset ranges identify which partition a commit came from.
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: workerTestTopic,
			Partitions: []kgo.FetchPartition{
				{Partition: 0, Records: workerTestRecords(0, 0, 3)},
				{Partition: 1, Records: workerTestRecords(1, 100, 3)},
			},
		}},
	}}

	if err := c.dispatch(context.Background(), fetches); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(committer.offsets()) == 6 }, "both partitions committed")

	var p0, p1 []int64
	for _, o := range committer.offsets() {
		if o < 100 {
			p0 = append(p0, o)
		} else {
			p1 = append(p1, o)
		}
	}
	assertOffsets(t, "partition 0", p0, 0, 1, 2)
	assertOffsets(t, "partition 1", p1, 100, 101, 102)
}

func TestConsumerDispatchWithoutWorkerSkipsUncommitted(t *testing.T) {
	c, committer := newTestConsumer(t, func(context.Context, int64) error { return nil })

	if err := c.dispatch(context.Background(), testFetches(5, workerTestRecords(5, 0, 2))); err != nil {
		t.Fatalf("dispatch error = %v, want nil", err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := committer.offsets(); len(got) != 0 {
		t.Fatalf("committed %v, want nothing", got)
	}
}

func TestConsumerRevokeFinishesCurrentRecordAndRemovesWorker(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	c, committer := newTestConsumer(t, func(_ context.Context, o int64) error {
		if o == 1 {
			close(started)
			<-release
		}
		return nil
	})
	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})

	if err := c.dispatch(context.Background(), testFetches(0, workerTestRecords(0, 0, 4))); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)

	revoked := make(chan struct{})
	go func() {
		c.onRevoked(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})
		close(revoked)
	}()

	select {
	case <-revoked:
		t.Fatal("revoke returned while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitSignal(t, revoked)

	assertOffsets(t, "committed", committer.offsets(), 0, 1)
	if c.workerFor(topicPartition{topic: workerTestTopic, partition: 0}) != nil {
		t.Fatal("worker still registered after revoke")
	}
}

func TestConsumerLostAbortsInFlightHandler(t *testing.T) {
	started := make(chan struct{})

	c, committer := newTestConsumer(t, func(ctx context.Context, o int64) error {
		if o == 1 {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})

	if err := c.dispatch(context.Background(), testFetches(0, workerTestRecords(0, 0, 4))); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)

	c.onLost(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})

	assertOffsets(t, "committed", committer.offsets(), 0)
	if c.fatal() != nil {
		t.Fatalf("lost partition must not be fatal, got %v", c.fatal())
	}
}

func TestConsumerWorkerFailureSetsFatalAndCancels(t *testing.T) {
	errBoom := errors.New("boom")

	c, committer := newTestConsumer(t, func(_ context.Context, o int64) error {
		if o == 1 {
			return errBoom
		}
		return nil
	})
	c.onAssigned(context.Background(), nil, map[string][]int32{workerTestTopic: {0}})

	if err := c.dispatch(context.Background(), testFetches(0, workerTestRecords(0, 0, 3))); err != nil {
		t.Fatal(err)
	}

	eventually(t, func() bool { return c.fatal() != nil }, "fatal error recorded")

	if !errors.Is(c.fatal(), errBoom) {
		t.Fatalf("fatal = %v, want it to wrap errBoom", c.fatal())
	}
	if c.fatalCtx.Err() == nil {
		t.Fatal("fatal context was not cancelled")
	}
	if got := committer.offsets(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("committed %v, want [0]", got)
	}
}

func TestConsumerNewRequiresRouter(t *testing.T) {
	if _, err := NewConsumer(Config{}, nil, nil); err == nil {
		t.Fatal("expected error for nil router")
	}
}
