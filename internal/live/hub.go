package live

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

const (
	// offlineGrace lets the status key expire before the hub looks for it.
	offlineGrace = time.Second
	// offlineRetry is how soon a failed status check is tried again.
	offlineRetry = 30 * time.Second
	// statusCheckTimeout bounds one status read for offline detection.
	statusCheckTimeout = 5 * time.Second
)

// ErrHubClosed is returned by Watch after Close.
var ErrHubClosed = errors.New("live: hub is closed")

// Subscriber is satisfied by *redis.LiveSubscriber.
type Subscriber interface {
	Messages() <-chan redisstore.LiveMessage
	Subscribe(ctx context.Context, houseIDs ...string) error
	Unsubscribe(ctx context.Context, houseIDs ...string) error
	Close() error
}

// StatusReader is satisfied by *redis.Store.
type StatusReader interface {
	GetHouseStatus(ctx context.Context, houseID string) (redisstore.HouseStatus, error)
}

// Timer is a scheduled call that can be cancelled.
type Timer interface {
	Stop() bool
}

// Clock lets tests control time.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, fn func()) Timer
}

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) AfterFunc(d time.Duration, fn func()) Timer { return time.AfterFunc(d, fn) }

type HubOptions struct {
	Clock  Clock
	Logger *slog.Logger
}

// Hub fans live events out to the streams on this instance. It holds one
// Redis subscription, for the houses that have at least one stream.
//
// A house goes offline when its status key expires, which publishes nothing.
// So for each watched house the hub sets a timer for the end of the liveness
// window and, when it fires, checks whether the status key is gone.
type Hub struct {
	sub    Subscriber
	status StatusReader
	clock  Clock
	logger *slog.Logger

	// mu guards the fields below. It is never held during Redis calls, so a
	// slow Redis cannot hold up the delivery of other houses' events.
	mu     sync.Mutex
	closed bool
	houses map[string]*watchedHouse

	// subMu keeps Redis SUBSCRIBE and UNSUBSCRIBE calls in order, so a house
	// that is watched again while it is being unsubscribed ends up subscribed.
	subMu sync.Mutex

	closeOnce sync.Once
	runDone   chan struct{}
}

type watchedHouse struct {
	// ready is closed once the first Watch has subscribed (err is nil) or
	// failed (err is set). Later Watch calls for the house wait for it.
	ready   chan struct{}
	err     error
	streams map[*Stream]struct{}
	online  bool
	timer   Timer
	// timerGen identifies the current timer, so a stale one does nothing.
	timerGen uint64
}

func NewHub(sub Subscriber, status StatusReader, opts HubOptions) *Hub {
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	h := &Hub{
		sub:     sub,
		status:  status,
		clock:   opts.Clock,
		logger:  opts.Logger,
		houses:  map[string]*watchedHouse{},
		runDone: make(chan struct{}),
	}
	go h.run()
	return h
}

// Watch starts a stream of the house's events. The first stream of a house
// subscribes to its channel and reads its status for offline detection.
func (h *Hub) Watch(ctx context.Context, houseID string) (*Stream, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}
	house, ok := h.houses[houseID]
	if !ok {
		house = &watchedHouse{ready: make(chan struct{}), streams: map[*Stream]struct{}{}}
		h.houses[houseID] = house
	}
	h.mu.Unlock()

	if !ok {
		h.prepare(ctx, houseID, house)
	} else {
		select {
		case <-house.ready:
		case <-ctx.Done():
			return nil, fmt.Errorf("watch house %q: %w", houseID, ctx.Err())
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if house.err != nil {
		return nil, house.err
	}
	if h.closed || h.houses[houseID] != house {
		return nil, ErrHubClosed
	}
	s := &Stream{
		hub:     h,
		houseID: houseID,
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	house.streams[s] = struct{}{}
	return s, nil
}

// prepare subscribes to a newly watched house and starts offline detection,
// without holding h.mu, then wakes any Watch calls waiting for the house.
func (h *Hub) prepare(ctx context.Context, houseID string, house *watchedHouse) {
	defer close(house.ready)

	h.subMu.Lock()
	err := h.sub.Subscribe(ctx, houseID)
	h.subMu.Unlock()
	if err != nil {
		h.mu.Lock()
		house.err = fmt.Errorf("watch house %q: %w", houseID, err)
		if h.houses[houseID] == house {
			delete(h.houses, houseID)
		}
		h.mu.Unlock()
		return
	}

	status, statusErr := h.status.GetHouseStatus(ctx, houseID)

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.houses[houseID] != house {
		return // the hub closed meanwhile
	}
	switch {
	case errors.Is(statusErr, redisstore.ErrNotFound):
		// Offline; the next online event starts the timer.
	case statusErr != nil:
		h.logger.Warn("read house status for live updates", "house_id", houseID, "error", statusErr)
		h.scheduleCheck(houseID, house, offlineRetry)
	case house.timer == nil:
		// Unless an online event already arrived and set a newer timer.
		house.online = true
		h.scheduleCheck(houseID, house, status.LastHeartbeatAt.Add(redisstore.HeartbeatTTL+offlineGrace).Sub(h.clock.Now()))
	}
}

// Close ends every stream (their Done channels close) and the Redis
// subscription. Watch fails afterwards.
func (h *Hub) Close() {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		for _, house := range h.houses {
			stopTimer(house)
			for s := range house.streams {
				close(s.done)
			}
		}
		h.houses = map[string]*watchedHouse{}
		h.mu.Unlock()

		if err := h.sub.Close(); err != nil {
			h.logger.Warn("close live subscription", "error", err)
		}
		<-h.runDone
	})
}

func (h *Hub) run() {
	defer close(h.runDone)
	for msg := range h.sub.Messages() {
		e, err := decodeEvent(msg.Payload)
		if err != nil {
			h.logger.Debug("live event ignored", "house_id", msg.HouseID, "error", err)
			continue
		}
		h.dispatch(msg.HouseID, e)
	}
}

func (h *Hub) dispatch(houseID string, e event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	house, ok := h.houses[houseID]
	if !ok {
		return
	}
	switch e.Type {
	case typeReading:
		for s := range house.streams {
			s.offerReading(*e.Reading)
		}
	case typeStatus:
		house.online = true
		h.scheduleCheck(houseID, house, e.LastHeartbeatAt.Add(redisstore.HeartbeatTTL+offlineGrace).Sub(h.clock.Now()))
		h.broadcastStatus(house, Status{Online: true, LastHeartbeatAt: *e.LastHeartbeatAt})
	}
}

// scheduleCheck replaces the house's timer. Called with h.mu held.
func (h *Hub) scheduleCheck(houseID string, house *watchedHouse, after time.Duration) {
	stopTimer(house)
	if after < 0 {
		after = 0
	}
	house.timerGen++
	gen := house.timerGen
	house.timer = h.clock.AfterFunc(after, func() { h.checkOffline(houseID, gen) })
}

func (h *Hub) checkOffline(houseID string, gen uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), statusCheckTimeout)
	defer cancel()
	status, err := h.status.GetHouseStatus(ctx, houseID)

	h.mu.Lock()
	defer h.mu.Unlock()
	house, ok := h.houses[houseID]
	if !ok || house.timerGen != gen {
		return
	}
	house.timer = nil

	switch {
	case errors.Is(err, redisstore.ErrNotFound):
		if house.online {
			house.online = false
			h.broadcastStatus(house, Status{Online: false})
		}
	case err != nil:
		h.logger.Warn("check house status for live updates", "house_id", houseID, "error", err)
		h.scheduleCheck(houseID, house, offlineRetry)
	default:
		house.online = true
		h.scheduleCheck(houseID, house, status.LastHeartbeatAt.Add(redisstore.HeartbeatTTL+offlineGrace).Sub(h.clock.Now()))
	}
}

// broadcastStatus is called with h.mu held.
func (h *Hub) broadcastStatus(house *watchedHouse, status Status) {
	for s := range house.streams {
		s.offerStatus(status)
	}
}

func (h *Hub) remove(s *Stream) {
	if h.release(s) {
		h.unsubscribe(s.houseID)
	}
}

// release removes the stream and reports whether it was the house's last one.
func (h *Hub) release(s *Stream) (last bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	house, ok := h.houses[s.houseID]
	if !ok {
		return false // the hub closed, or this stream was already removed
	}
	if _, ok := house.streams[s]; !ok {
		return false
	}
	delete(house.streams, s)
	if len(house.streams) > 0 {
		return false
	}
	stopTimer(house)
	delete(h.houses, s.houseID)
	return true
}

// unsubscribe drops the house's channel unless it is watched again by now.
// It runs without h.mu; subMu orders it with any new subscribe.
func (h *Hub) unsubscribe(houseID string) {
	h.subMu.Lock()
	defer h.subMu.Unlock()

	h.mu.Lock()
	_, watchedAgain := h.houses[houseID]
	closed := h.closed
	h.mu.Unlock()
	if watchedAgain || closed {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), statusCheckTimeout)
	defer cancel()
	if err := h.sub.Unsubscribe(ctx, houseID); err != nil {
		h.logger.Warn("unsubscribe from live channel", "house_id", houseID, "error", err)
	}
}

func stopTimer(house *watchedHouse) {
	if house.timer != nil {
		house.timer.Stop()
		house.timer = nil
	}
	house.timerGen++
}

// Stream receives one house's events. It keeps only the latest reading and
// the latest status, so a slow caller skips readings instead of queueing them.
type Stream struct {
	hub     *Hub
	houseID string

	mu      sync.Mutex
	status  *Status
	reading *redisstore.MeterProjection

	notify    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// Notify receives a value when Take has something new.
func (s *Stream) Notify() <-chan struct{} { return s.notify }

// Done is closed when the hub shuts down.
func (s *Stream) Done() <-chan struct{} { return s.done }

// Take returns and clears the pending status and reading; either may be nil.
func (s *Stream) Take() (*Status, *redisstore.MeterProjection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, reading := s.status, s.reading
	s.status, s.reading = nil, nil
	return status, reading
}

// Close stops the stream. It is safe to call more than once.
func (s *Stream) Close() {
	s.closeOnce.Do(func() { s.hub.remove(s) })
}

func (s *Stream) offerReading(r redisstore.MeterProjection) {
	s.mu.Lock()
	s.reading = &r
	s.mu.Unlock()
	s.wake()
}

func (s *Stream) offerStatus(status Status) {
	s.mu.Lock()
	s.status = &status
	s.mu.Unlock()
	s.wake()
}

func (s *Stream) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
