package ingestion

import (
	"fmt"

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

// NOTE: This is just starting boilerplate for kafka consumer

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
