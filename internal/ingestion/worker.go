package ingestion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// offsetCommitter is the one franz-go capability a partition worker
// needs. *kgo.Client satisfies it; tests substitute a fake so the
// worker's ordering/commit rules can be verified without a real broker.
type offsetCommitter interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

var _ offsetCommitter = (*kgo.Client)(nil)

// commitTimeout bounds each offset commit. Commits deliberately do NOT
// use the worker's own context: during a graceful revoke or shutdown that
// context may already be cancelled, yet the commit for work that did
// finish must still go out.
const commitTimeout = 10 * time.Second

var errWorkerStopped = errors.New("ingestion: partition worker stopped")

type topicPartition struct {
	topic     string
	partition int32
}

func (tp topicPartition) String() string {
	return fmt.Sprintf("%s/%d", tp.topic, tp.partition)
}

type workerDeps struct {
	router     *Router
	committer  offsetCommitter
	logger     *slog.Logger
	slots      chan struct{} // shared limiter: bounds concurrently running handlers
	onFatal    func(error)   // reports an unrecoverable error to the consumer
	queueDepth int           // bounded queue of pending batches for this partition
}

// partitionWorker owns exactly one assigned topic-partition. Records for
// that partition are handled strictly one at a time, in offset order, so
// ordering is preserved independently per topic/partition while
// different partitions run in parallel.
type partitionWorker struct {
	tp   topicPartition
	deps workerDeps

	ctx    context.Context // cancelled to abort in-flight handlers
	cancel context.CancelFunc

	batches  chan []*kgo.Record
	quit     chan struct{} // closed to request a graceful stop
	stopOnce sync.Once
	done     chan struct{} // closed when the worker goroutine has exited
}

func newPartitionWorker(parent context.Context, tp topicPartition, deps workerDeps) *partitionWorker {
	ctx, cancel := context.WithCancel(parent)
	return &partitionWorker{
		tp:      tp,
		deps:    deps,
		ctx:     ctx,
		cancel:  cancel,
		batches: make(chan []*kgo.Record, deps.queueDepth),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (w *partitionWorker) start() { go w.run() }

func (w *partitionWorker) run() {
	defer close(w.done)
	defer w.cancel()

	for {
		// Prefer a pending stop over picking up more work.
		if w.stopping() {
			return
		}
		select {
		case <-w.quit:
			return
		case <-w.ctx.Done():
			return
		case recs := <-w.batches:
			if !w.processBatch(recs) {
				return
			}
		}
	}
}

func (w *partitionWorker) stopping() bool {
	select {
	case <-w.quit:
		return true
	default:
		return false
	}
}

// processBatch handles recs in order and commits only the prefix that
// was fully handled. It returns false when the worker must exit (after
// reporting a fatal error).
//
// Offset rule: never commit a record whose handler did not complete.
// Anything after the last completed record (because of a stop, an abort
// or a failure) is left uncommitted and will be redelivered - safe under
// the service's at-least-once, idempotent-write design.
func (w *partitionWorker) processBatch(recs []*kgo.Record) bool {
	// Bound how many partitions run handlers at the same time.
	select {
	case w.deps.slots <- struct{}{}:
	case <-w.quit:
		return true // dropped, uncommitted: redelivered to the next owner
	case <-w.ctx.Done():
		return true
	}
	defer func() { <-w.deps.slots }()

	processed := 0
	for _, rec := range recs {
		// A stop request takes effect between records, which keeps a
		// rebalance from waiting on a whole batch.
		if w.stopping() || w.ctx.Err() != nil {
			break
		}

		if err := w.deps.router.Route(w.ctx, rec.Topic, rec.Key, rec.Value); err != nil {
			if w.ctx.Err() != nil {
				break // aborted mid-record: not finished, not committed
			}

			// PLACEHOLDER (fail closed): until PETPG-226 adds retry
			// classification and dead-lettering, any handler error is
			// unrecoverable here. Commit what finished, then stop the
			// consumer rather than skip the record and lose data.
			w.commit(recs[:processed])
			w.deps.onFatal(fmt.Errorf("handling %s offset %d: %w", w.tp, rec.Offset, err))
			return false
		}
		processed++
	}

	w.commit(recs[:processed])
	return true
}

func (w *partitionWorker) commit(recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()

	if err := w.deps.committer.CommitRecords(ctx, recs...); err != nil {
		// Not fatal: a failed commit only means redelivery. Expected
		// after a lost partition, where commits are rejected.
		w.deps.logger.Warn("offset commit failed; records may be redelivered",
			"partition", w.tp.String(),
			"through_offset", recs[len(recs)-1].Offset,
			"error", err,
		)
	}
}

// enqueue hands a batch to the worker. The queue is bounded, so a slow
// partition applies backpressure to the poll loop instead of growing
// memory without limit.
func (w *partitionWorker) enqueue(ctx context.Context, recs []*kgo.Record) error {
	select {
	case w.batches <- recs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errWorkerStopped
	}
}

// stop asks the worker to finish and waits for it. Graceful (abort=false)
// lets the record currently being handled complete and be committed;
// abort=true cancels in-flight handlers first (used when the partition
// was lost and commits would be rejected anyway). Safe to call twice.
func (w *partitionWorker) stop(abort bool) {
	if abort {
		w.cancel()
	}
	w.stopOnce.Do(func() { close(w.quit) })
	<-w.done
}
