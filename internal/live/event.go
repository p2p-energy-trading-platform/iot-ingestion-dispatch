// Package live carries a house's live events, its new readings and meter
// status changes, from ingestion to StreamHouseLive callers on any instance.
//
// Ingestion publishes events on the Redis channel house:{id}:live (Publisher).
// Each instance runs one Hub, which subscribes to the channels of the houses
// its streams watch and fans events out to them. Delivery is best effort:
// nothing is stored or replayed, and callers resync over the unary RPCs.
package live

import (
	"encoding/json"
	"fmt"
	"time"

	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

const (
	typeReading = "reading"
	typeStatus  = "status"

	statusOnline = "online"
)

// event is the payload on house:{id}:live. It is private to this service;
// it is never the external wire format.
type event struct {
	Type            string                      `json:"type"`
	Reading         *redisstore.MeterProjection `json:"reading,omitempty"`
	Status          string                      `json:"status,omitempty"`
	LastHeartbeatAt *time.Time                  `json:"last_heartbeat_at,omitempty"`
}

// Status is a house's meter status. LastHeartbeatAt is zero when offline.
type Status struct {
	Online          bool
	LastHeartbeatAt time.Time
}

func encodeReading(r redisstore.MeterProjection) ([]byte, error) {
	return json.Marshal(event{Type: typeReading, Reading: &r})
}

func encodeOnline(lastHeartbeatAt time.Time) ([]byte, error) {
	at := lastHeartbeatAt.UTC()
	return json.Marshal(event{Type: typeStatus, Status: statusOnline, LastHeartbeatAt: &at})
}

func decodeEvent(payload []byte) (event, error) {
	var e event
	if err := json.Unmarshal(payload, &e); err != nil {
		return event{}, fmt.Errorf("decode live event: %w", err)
	}
	switch {
	case e.Type == typeReading && e.Reading != nil:
	case e.Type == typeStatus && e.Status == statusOnline && e.LastHeartbeatAt != nil:
	default:
		return event{}, fmt.Errorf("live event type %q is not supported or incomplete", e.Type)
	}
	return e, nil
}
