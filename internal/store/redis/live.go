package redis

import (
	"context"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const (
	liveChannelPrefix = "house:"
	liveChannelSuffix = ":live"
)

// LiveMessage is one event received on a house's live channel.
type LiveMessage struct {
	HouseID string
	Payload []byte
}

// Publish sends payload to channel. Redis delivers it only to current
// subscribers; nothing is stored.
func (store *Store) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := store.rdb.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("redis: publish to %s: %w", channel, err)
	}
	return nil
}

// LiveSubscriber holds one Redis pub/sub connection for houses' live
// channels. go-redis reconnects it and re-subscribes after a connection loss.
type LiveSubscriber struct {
	pubsub   *goredis.PubSub
	messages chan LiveMessage
}

// NewLiveSubscriber opens a pub/sub connection with no channels yet. Read
// Messages until it is closed.
func (store *Store) NewLiveSubscriber(ctx context.Context) *LiveSubscriber {
	sub := &LiveSubscriber{
		pubsub:   store.rdb.Subscribe(ctx),
		messages: make(chan LiveMessage),
	}
	go sub.forward()
	return sub
}

func (sub *LiveSubscriber) forward() {
	defer close(sub.messages)
	for msg := range sub.pubsub.Channel() {
		houseID, ok := houseFromLiveChannel(msg.Channel)
		if !ok {
			continue
		}
		sub.messages <- LiveMessage{HouseID: houseID, Payload: []byte(msg.Payload)}
	}
}

// Messages delivers events for the subscribed houses. It is closed by Close.
func (sub *LiveSubscriber) Messages() <-chan LiveMessage { return sub.messages }

// Subscribe starts receiving the houses' live events. Redis applies it
// asynchronously, so events published right after may be missed.
func (sub *LiveSubscriber) Subscribe(ctx context.Context, houseIDs ...string) error {
	if err := sub.pubsub.Subscribe(ctx, liveChannels(houseIDs)...); err != nil {
		return fmt.Errorf("redis: subscribe to live channels: %w", err)
	}
	return nil
}

// Unsubscribe stops receiving the houses' live events.
func (sub *LiveSubscriber) Unsubscribe(ctx context.Context, houseIDs ...string) error {
	if err := sub.pubsub.Unsubscribe(ctx, liveChannels(houseIDs)...); err != nil {
		return fmt.Errorf("redis: unsubscribe from live channels: %w", err)
	}
	return nil
}

// Close ends the connection and closes Messages.
func (sub *LiveSubscriber) Close() error {
	return sub.pubsub.Close()
}

func liveChannels(houseIDs []string) []string {
	channels := make([]string, len(houseIDs))
	for i, id := range houseIDs {
		channels[i] = HouseLiveChannel(id)
	}
	return channels
}

func houseFromLiveChannel(channel string) (string, bool) {
	if !strings.HasPrefix(channel, liveChannelPrefix) || !strings.HasSuffix(channel, liveChannelSuffix) {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(channel, liveChannelPrefix), liveChannelSuffix), true
}
