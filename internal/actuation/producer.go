package actuation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaActuationProducer struct {
	client *kgo.Client
	topic  string
}

func NewKafkaActuationProducer(brokers []string, topic string, extraOpts ...kgo.Opt) (*KafkaActuationProducer, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}, extraOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("Failed to create kafka producer client: %w", err)
	}

	return &KafkaActuationProducer{
		client: client,
		topic:  topic,
	}, nil
}

func (k *KafkaActuationProducer) SendCommand(ctx context.Context, cmd Command) error {
	if cmd.Timestamp == "" {
		cmd.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	valueBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshall actuation command %w", err)
	}

	record := &kgo.Record{
		Topic: k.topic,
		Key:   []byte(cmd.MeterID),
		Value: valueBytes,
	}

	// produce sync is synchronous
	results := k.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to send actuation message to kafka: %w", err)
	}

	return nil
}

func (k *KafkaActuationProducer) Close() {
	k.client.Close()
}
