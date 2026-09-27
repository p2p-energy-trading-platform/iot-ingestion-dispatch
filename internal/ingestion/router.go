package ingestion

import (
	"context"
	"fmt"
)

// RecordHandler processes one decoded Kafka record for a single topic.
// Concrete implementations (meter_handler.go, heartbeat_handler.go) are
// separate, later tickets (PETPG-223/224) - the consumer loop and router
// here depend only on this interface, so the poll loop can be built and
// tested before those handlers exist.
type RecordHandler interface {
	HandleRecord(ctx context.Context, key, value []byte) error
}

// Router dispatches a record to the handler registered for its topic.
// Strict: an unrecognized topic is itself an error, never silently
// dropped - this should never happen if the consumer only ever
// subscribes to known topics, but it defends against misconfiguration or
// a future topic being added to Config without a matching handler.
type Router struct {
	handlers map[string]RecordHandler
}

func NewRouter() *Router {
	return &Router{handlers: make(map[string]RecordHandler)}
}

// Register wires a handler for a specific topic. Call once per topic
// during startup wiring (internal/app), before the consumer starts
// polling.
func (r *Router) Register(topic string, handler RecordHandler) {
	r.handlers[topic] = handler
}

// Route dispatches to the handler registered for topic. Returns an error
// (never silently drops) if no handler is registered.
func (r *Router) Route(ctx context.Context, topic string, key, value []byte) error {
	handler, ok := r.handlers[topic]
	if !ok {
		return fmt.Errorf("ingestion: no handler registered for topic %q", topic)
	}
	return handler.HandleRecord(ctx, key, value)
}
