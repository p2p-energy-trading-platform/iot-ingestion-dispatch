package ingestion

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoHandler is returned by Router.Route when a record arrives for a
// topic nobody registered a handler for. It is a wiring/configuration
// bug, never a data problem, so the consumer treats it as fatal rather
// than skipping the record.
var ErrNoHandler = errors.New("ingestion: no handler registered for topic")

// RecordHandler processes one Kafka record for a single topic. Concrete
// implementations (meter_handler.go, heartbeat_handler.go) are separate,
// later tickets (PETPG-223/224) - the consumer loop and router depend
// only on this interface, so the loop can be built and tested before
// those handlers exist.
type RecordHandler interface {
	HandleRecord(ctx context.Context, key, value []byte) error
}

// Router dispatches a record to the handler registered for its topic.
// Strict: an unrecognized topic is an error, never silently dropped.
//
// Register must be called during startup wiring, before the consumer
// starts polling - the router is read concurrently by partition workers
// afterwards and is not safe for concurrent Register calls.
type Router struct {
	handlers map[string]RecordHandler
}

func NewRouter() *Router {
	return &Router{handlers: make(map[string]RecordHandler)}
}

// Register wires a handler for a specific topic.
func (r *Router) Register(topic string, handler RecordHandler) {
	r.handlers[topic] = handler
}

// Route dispatches to the handler registered for topic.
func (r *Router) Route(ctx context.Context, topic string, key, value []byte) error {
	handler, ok := r.handlers[topic]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoHandler, topic)
	}
	return handler.HandleRecord(ctx, key, value)
}
