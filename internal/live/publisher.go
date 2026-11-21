package live

import (
	"context"
	"log/slog"
	"time"

	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// Sender is satisfied by *redis.Store.
type Sender interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// Publisher sends live events. Publishing is best effort: a failure is
// logged, never returned, so it cannot fail ingestion or hold a Kafka offset.
type Publisher struct {
	sender Sender
	logger *slog.Logger
}

func NewPublisher(sender Sender, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{sender: sender, logger: logger}
}

// PublishReading announces a reading that was stored as the house's latest.
func (p *Publisher) PublishReading(ctx context.Context, r redisstore.MeterProjection) {
	payload, err := encodeReading(r)
	p.publish(ctx, r.HouseID, typeReading, payload, err)
}

// PublishOnline announces that the house's meter came online.
func (p *Publisher) PublishOnline(ctx context.Context, houseID string, lastHeartbeatAt time.Time) {
	payload, err := encodeOnline(lastHeartbeatAt)
	p.publish(ctx, houseID, typeStatus, payload, err)
}

func (p *Publisher) publish(ctx context.Context, houseID, eventType string, payload []byte, err error) {
	if err == nil {
		err = p.sender.Publish(ctx, redisstore.HouseLiveChannel(houseID), payload)
	}
	if err != nil {
		p.logger.WarnContext(ctx, "live event not published", "house_id", houseID, "type", eventType, "error", err)
	}
}
