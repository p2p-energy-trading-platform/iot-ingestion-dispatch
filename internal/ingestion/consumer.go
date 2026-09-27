package ingestion

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
}

type Config struct {
	Brokers        []string
	ConsumerGroup  string
	MeterTopic     string
	HeartbeatTopic string
}

func NewConsumer(config Config) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(config.MeterTopic, config.HeartbeatTopic),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	consumer := Consumer{
		client: client,
	}

	return &consumer, nil
}

func (consumer *Consumer) Close() {
	consumer.client.Close()
}

// Run polls Kafka continuously until ctx is cancelled, routing each
// record to the handler registered for its topic via router, then
// committing offsets for the batch. Auto-commit is disabled (see
// NewConsumer), so this loop owns offset advancement entirely, using
// CommitUncommittedOffsets - franz-go's documented simple pattern for a
// disabled-autocommit poll/process/commit loop.
//
// TWO DELIBERATE SCOPE BOUNDARIES (PETPG-221 covers only the loop itself):
//
//  1. A handler error is currently logged and the batch is still
//     committed forward. This is a placeholder, not final behavior -
//     PETPG-226 (retry and permanent-failure handling) will replace this
//     with real transient/permanent classification and dead-letter
//     recording to iot_data.ingestion_failures. Acceptable for now only
//     because no real handlers are registered yet (meter/heartbeat
//     handlers are separate, later tickets) - every record would
//     currently hit the router's "no handler registered" error.
//
//  2. franz-go's own docs note CommitUncommittedOffsets "may lead to
//     duplicate records if a consumer group rebalance [happens]" mid-batch.
//     PETPG-227 (delivery and rebalance safety) is explicitly scoped to
//     address this - not attempted here.
func (consumer *Consumer) Run(ctx context.Context, router *Router, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetches := consumer.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				logger.Error("kafka fetch error", "error", fetchErr)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			if err := router.Route(ctx, record.Topic, record.Key, record.Value); err != nil {
				logger.Error("record handling failed",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"error", err,
				)
			}
		}

		if err := consumer.client.CommitUncommittedOffsets(ctx); err != nil {
			logger.Error("commit offsets failed", "error", err)
		}
	}
}
