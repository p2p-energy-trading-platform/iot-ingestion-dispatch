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

// recordTimeout bounds each write to the failure store, so a hung database
// cannot hold a partition worker forever.
const recordTimeout = 10 * time.Second

var errWorkerStopped = errors.New("ingestion: partition worker stopped")

// errRecordInterrupted means a stop or abort cut a record short. The record
// did not finish, so its offset stays uncommitted and it is redelivered.
var errRecordInterrupted = errors.New("ingestion: record handling interrupted")

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
	recorder   FailureRecorder
	retry      RetryPolicy
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

	// retryCtx only cuts short the waits between retry attempts. It is
	// cancelled on any stop, so a revoke never sits out a backoff pause,
	// while the attempt already running still finishes under ctx.
	retryCtx    context.Context
	retryCancel context.CancelFunc

	batches  chan []*kgo.Record
	quit     chan struct{} // closed to request a graceful stop
	stopOnce sync.Once
	done     chan struct{} // closed when the worker goroutine has exited
}

func newPartitionWorker(parent context.Context, tp topicPartition, deps workerDeps) *partitionWorker {
	ctx, cancel := context.WithCancel(parent)
	retryCtx, retryCancel := context.WithCancel(ctx)
	return &partitionWorker{
		tp:          tp,
		deps:        deps,
		ctx:         ctx,
		cancel:      cancel,
		retryCtx:    retryCtx,
		retryCancel: retryCancel,
		batches:     make(chan []*kgo.Record, deps.queueDepth),
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (w *partitionWorker) start() { go w.run() }

func (w *partitionWorker) run() {
	defer close(w.done)
	defer w.cancel()
	defer w.retryCancel()

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
// Offset rule: never commit a record that did not finish. A record finishes
// when its handler succeeds, or when it failed permanently (or exhausted its
// retries) and the failure was durably recorded. Anything after the last
// finished record, because of a stop, an abort, a missing handler or a failed
// recording, stays uncommitted and is redelivered, which is safe under the
// service's at-least-once, idempotent-write design.
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

		if err := w.handleRecord(rec); err != nil {
			if w.ctx.Err() != nil || errors.Is(err, errRecordInterrupted) {
				break // interrupted mid-record: not finished, not committed
			}

			// Either a wiring bug (no handler for the topic) or the failure
			// could not be recorded. This record's offset must stay
			// uncommitted: commit what finished, then stop the consumer
			// rather than skip the record and lose data.
			w.commit(recs[:processed])
			w.deps.onFatal(fmt.Errorf("handling %s offset %d: %w", w.tp, rec.Offset, err))
			return false
		}
		processed++
	}

	w.commit(recs[:processed])
	return true
}

// handleRecord runs the topic handler with bounded retry. It returns nil when
// the record is finished: it succeeded, or it failed for good and the failure
// was recorded. It returns errRecordInterrupted when a stop cut it short, or
// another error when the record must stay uncommitted.
func (w *partitionWorker) handleRecord(rec *kgo.Record) error {
	attempts, err := retryHandle(w.retryCtx, w.deps.retry, w.deps.logger, func() error {
		return w.deps.router.Route(w.ctx, rec.Topic, rec.Key, rec.Value)
	})
	if err == nil {
		return nil
	}
	if w.retryCtx.Err() != nil {
		return errRecordInterrupted
	}
	if errors.Is(err, ErrNoHandler) {
		return err
	}
	return w.recordFailure(rec, err, attempts)
}

// recordFailure persists a permanent or exhausted failure. The caller commits
// the record's offset only when this returns nil.
func (w *partitionWorker) recordFailure(rec *kgo.Record, handlerErr error, attempts int) error {
	if w.deps.recorder == nil {
		return fmt.Errorf("failure recorder is not configured; handler error: %w", handlerErr)
	}

	failure := newIngestionFailure(rec, handlerErr, attempts)

	ctx, cancel := context.WithTimeout(w.ctx, recordTimeout)
	defer cancel()

	if err := w.deps.recorder.RecordFailure(ctx, failure); err != nil {
		return fmt.Errorf("record failure at stage %s: %w; handler error: %w", failure.Stage, err, handlerErr)
	}

	w.deps.logger.Warn("recorded ingestion failure; committing offset",
		"partition", w.tp.String(),
		"offset", rec.Offset,
		"stage", failure.Stage,
		"attempts", attempts,
		"error", handlerErr,
	)
	return nil
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
// lets the attempt currently running complete and be committed, but cuts any
// backoff wait short; abort=true cancels in-flight handlers first (used when
// the partition was lost and commits would be rejected anyway). Safe to call
// twice.
func (w *partitionWorker) stop(abort bool) {
	if abort {
		w.cancel()
	}
	w.stopOnce.Do(func() {
		close(w.quit)
		w.retryCancel()
	})
	<-w.done
}
