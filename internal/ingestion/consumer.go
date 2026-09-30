package ingestion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxConcurrentHandlers = 16
	defaultPartitionQueueDepth   = 2
)

// ErrConsumerClosed is returned by Run when the Kafka client was closed
// underneath it.
var ErrConsumerClosed = errors.New("ingestion: consumer closed")

type Config struct {
	Brokers        []string
	ConsumerGroup  string
	MeterTopic     string
	HeartbeatTopic string

	// MaxConcurrentHandlers bounds how many partition workers may run a
	// handler at the same time, across all partitions. Zero uses the default.
	MaxConcurrentHandlers int
	// PartitionQueueDepth bounds pending batches per partition before the
	// poll loop is back-pressured. Zero uses the default.
	PartitionQueueDepth int
}

func (c Config) withDefaults() Config {
	if c.MaxConcurrentHandlers <= 0 {
		c.MaxConcurrentHandlers = defaultMaxConcurrentHandlers
	}
	if c.PartitionQueueDepth <= 0 {
		c.PartitionQueueDepth = defaultPartitionQueueDepth
	}
	return c
}

// Consumer polls Kafka and fans records out to one partitionWorker per
// assigned topic-partition. Ordering is preserved per topic/partition;
// different partitions run in parallel, bounded by a shared slot limiter.
type Consumer struct {
	client    *kgo.Client
	router    *Router
	logger    *slog.Logger
	config    Config
	committer offsetCommitter

	ready chan struct{} // closed once client is assigned
	slots chan struct{}

	mu      sync.Mutex
	workers map[topicPartition]*partitionWorker

	workerCtx    context.Context // parent of every worker context
	workerCancel context.CancelFunc

	fatalCtx    context.Context // cancelled when a worker reports a fatal error
	fatalCancel context.CancelFunc
	fatalMu     sync.Mutex
	fatalErr    error

	closeOnce sync.Once
}

// clientCommitter lets workers (created inside group callbacks that can fire
// before NewConsumer has returned) commit through the client safely.
type clientCommitter struct{ consumer *Consumer }

var _ offsetCommitter = clientCommitter{}

func (cc clientCommitter) CommitRecords(ctx context.Context, rs ...*kgo.Record) error {
	select {
	case <-cc.consumer.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	return cc.consumer.client.CommitRecords(ctx, rs...)
}

// NewConsumer takes the router at construction time because partition
// callbacks can fire as soon as the client joins the group. Register all
// handlers on the router before calling Run.
func NewConsumer(config Config, router *Router, logger *slog.Logger) (*Consumer, error) {
	if router == nil {
		return nil, errors.New("create kafka consumer: router is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	config = config.withDefaults()

	workerCtx, workerCancel := context.WithCancel(context.Background())
	fatalCtx, fatalCancel := context.WithCancel(context.Background())

	consumer := &Consumer{
		router:       router,
		logger:       logger,
		config:       config,
		ready:        make(chan struct{}),
		slots:        make(chan struct{}, config.MaxConcurrentHandlers),
		workers:      make(map[topicPartition]*partitionWorker),
		workerCtx:    workerCtx,
		workerCancel: workerCancel,
		fatalCtx:     fatalCtx,
		fatalCancel:  fatalCancel,
	}

	consumer.committer = clientCommitter{consumer: consumer}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(config.MeterTopic, config.HeartbeatTopic),
		kgo.DisableAutoCommit(),
		// Rebalances wait until AllowRebalance, so a revoke can never run
		// while Run is mid-dispatch. CloseAllowingRebalance in Close is the
		// matching shutdown call.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(consumer.onAssigned),
		kgo.OnPartitionsRevoked(consumer.onRevoked),
		kgo.OnPartitionsLost(consumer.onLost),
	)
	if err != nil {
		workerCancel()
		fatalCancel()
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	consumer.client = client
	close(consumer.ready)

	return consumer, nil
}

// Run polls until ctx is cancelled, a worker reports a fatal error, or the
// client is closed. Offsets are committed by the workers, never here.
func (c *Consumer) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(c.fatalCtx, cancel)
	defer stop()

	for {
		fetches := c.client.PollFetches(runCtx)

		if fetches.IsClientClosed() {
			return ErrConsumerClosed
		}
		if runCtx.Err() != nil {
			c.client.AllowRebalance()
			return c.runResult(ctx)
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("kafka fetch error",
				"topic", topic, "partition", partition, "error", err)
		})

		err := c.dispatch(runCtx, fetches)
		c.client.AllowRebalance()
		if err != nil {
			return c.runResult(ctx)
		}
	}
}

func (c *Consumer) runResult(ctx context.Context) error {
	if err := c.fatal(); err != nil {
		return err
	}
	return ctx.Err()
}

// dispatch hands each partition's records to that partition's worker. It
// returns non-nil only when ctx was cancelled while back-pressured.
func (c *Consumer) dispatch(ctx context.Context, fetches kgo.Fetches) error {
	var enqueueErr error

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if enqueueErr != nil || len(p.Records) == 0 {
			return
		}

		tp := topicPartition{topic: p.Topic, partition: p.Partition}
		worker := c.workerFor(tp)
		if worker == nil {
			// Left uncommitted, so it is redelivered to whoever owns it.
			c.logger.Warn("records for partition without a worker; skipping",
				"partition", tp.String(), "count", len(p.Records))
			return
		}

		if err := worker.enqueue(ctx, p.Records); err != nil {
			if errors.Is(err, errWorkerStopped) {
				c.logger.Warn("partition worker stopped; records left uncommitted",
					"partition", tp.String())
				return
			}
			enqueueErr = err
		}
	})

	return enqueueErr
}

func (c *Consumer) workerFor(tp topicPartition) *partitionWorker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workers[tp]
}

func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for topic, partitions := range assigned {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			if _, exists := c.workers[tp]; exists {
				continue
			}

			worker := newPartitionWorker(c.workerCtx, tp, workerDeps{
				router:     c.router,
				committer:  c.committer,
				logger:     c.logger,
				slots:      c.slots,
				onFatal:    c.fail,
				queueDepth: c.config.PartitionQueueDepth,
			})
			c.workers[tp] = worker
			worker.start()

			c.logger.Info("partition assigned", "partition", tp.String())
		}
	}
}

// onRevoked lets the record currently being handled finish and commits the
// finished prefix; queued-but-unstarted batches are dropped (redelivered).
func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.stopWorkers(c.takeWorkers(revoked), false)
}

// onLost aborts in-flight handlers: commits would be rejected anyway.
func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.stopWorkers(c.takeWorkers(lost), true)
}

// takeWorkers removes and returns workers for the given partitions; a nil
// filter takes all of them.
func (c *Consumer) takeWorkers(filter map[string][]int32) []*partitionWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	var taken []*partitionWorker
	if filter == nil {
		for tp, w := range c.workers {
			taken = append(taken, w)
			delete(c.workers, tp)
		}
		return taken
	}

	for topic, partitions := range filter {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			if w, ok := c.workers[tp]; ok {
				taken = append(taken, w)
				delete(c.workers, tp)
				c.logger.Info("partition released", "partition", tp.String(), "abort", false)
			}
		}
	}
	return taken
}

func (c *Consumer) stopWorkers(workers []*partitionWorker, abort bool) {
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *partitionWorker) {
			defer wg.Done()
			w.stop(abort)
		}(w)
	}
	wg.Wait()
}

func (c *Consumer) fail(err error) {
	c.fatalMu.Lock()
	if c.fatalErr == nil {
		c.fatalErr = err
	}
	c.fatalMu.Unlock()
	c.fatalCancel()
}

func (c *Consumer) fatal() error {
	c.fatalMu.Lock()
	defer c.fatalMu.Unlock()
	return c.fatalErr
}

// Close stops all workers gracefully (committing finished work), then
// leaves the group. Safe to call more than once.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.stopWorkers(c.takeWorkers(nil), false)
		c.workerCancel()
		c.fatalCancel()
		c.client.CloseAllowingRebalance()
	})
}
