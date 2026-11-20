package live

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// ---- fakes ----

type fakeSender struct {
	mu        sync.Mutex
	published map[string][]string
	err       error
}

func (f *fakeSender) Publish(_ context.Context, channel string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.published == nil {
		f.published = map[string][]string{}
	}
	f.published[channel] = append(f.published[channel], string(payload))
	return nil
}

type fakeMetrics struct {
	mu                                sync.Mutex
	published                         map[string]int
	publishErrors, conflated, offline int
	streams                           int
}

func (m *fakeMetrics) LiveEventPublished(eventType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.published == nil {
		m.published = map[string]int{}
	}
	m.published[eventType]++
}
func (m *fakeMetrics) LivePublishFailed()       { m.mu.Lock(); m.publishErrors++; m.mu.Unlock() }
func (m *fakeMetrics) LiveEventConflated()      { m.mu.Lock(); m.conflated++; m.mu.Unlock() }
func (m *fakeMetrics) LiveOfflineDetected()     { m.mu.Lock(); m.offline++; m.mu.Unlock() }
func (m *fakeMetrics) LiveStreamsChanged(d int) { m.mu.Lock(); m.streams += d; m.mu.Unlock() }

func (m *fakeMetrics) snapshot() fakeMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fakeMetrics{conflated: m.conflated, offline: m.offline, streams: m.streams, publishErrors: m.publishErrors}
}

type fakeSubscriber struct {
	mu           sync.Mutex
	messages     chan redisstore.LiveMessage
	subscribed   map[string]bool
	calls        []string
	subscribeErr error
	closed       bool
	// Gates hold a Redis call open until the test releases them.
	subscribeGate   *gate
	unsubscribeGate *gate
}

// gate blocks callers until released, and reports when the first one arrives.
type gate struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{arrived: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) wait() {
	if g == nil {
		return
	}
	g.once.Do(func() { close(g.arrived) })
	<-g.release
}

func (g *gate) waitArrived(t *testing.T) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(time.Second):
		t.Fatal("nothing reached the gate")
	}
}

func newFakeSubscriber() *fakeSubscriber {
	return &fakeSubscriber{messages: make(chan redisstore.LiveMessage), subscribed: map[string]bool{}}
}

func (f *fakeSubscriber) Messages() <-chan redisstore.LiveMessage { return f.messages }

func (f *fakeSubscriber) Subscribe(_ context.Context, houseIDs ...string) error {
	f.subscribeGate.wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	for _, id := range houseIDs {
		f.subscribed[id] = true
		f.calls = append(f.calls, "subscribe:"+id)
	}
	return nil
}

func (f *fakeSubscriber) Unsubscribe(_ context.Context, houseIDs ...string) error {
	f.unsubscribeGate.wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range houseIDs {
		delete(f.subscribed, id)
		f.calls = append(f.calls, "unsubscribe:"+id)
	}
	return nil
}

func (f *fakeSubscriber) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.messages)
	}
	return nil
}

func (f *fakeSubscriber) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// deliver sends an event as if it came from Redis.
func (f *fakeSubscriber) deliver(t *testing.T, houseID string, payload []byte) {
	t.Helper()
	select {
	case f.messages <- redisstore.LiveMessage{HouseID: houseID, Payload: payload}:
	case <-time.After(time.Second):
		t.Fatal("hub is not reading messages")
	}
}

type fakeStatus struct {
	mu     sync.Mutex
	status map[string]redisstore.HouseStatus
	err    error
	gate   *gate
}

func (f *fakeStatus) set(houseID string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status == nil {
		f.status = map[string]redisstore.HouseStatus{}
	}
	f.status[houseID] = redisstore.HouseStatus{Status: "online", LastHeartbeatAt: at}
}

func (f *fakeStatus) expire(houseID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.status, houseID)
}

func (f *fakeStatus) GetHouseStatus(_ context.Context, houseID string) (redisstore.HouseStatus, error) {
	f.gate.wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return redisstore.HouseStatus{}, f.err
	}
	s, ok := f.status[houseID]
	if !ok {
		return redisstore.HouseStatus{}, redisstore.ErrNotFound
	}
	return s, nil
}

// fakeClock runs timers only when the test fires them.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock   *fakeClock
	after   time.Duration
	fn      func()
	stopped bool
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{clock: c, after: d, fn: fn}
	c.timers = append(c.timers, timer)
	return timer
}

// pending returns the delays of timers that are still scheduled.
func (c *fakeClock) pending() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, t := range c.timers {
		if !t.stopped {
			out = append(out, t.after)
		}
	}
	return out
}

// fire runs every scheduled timer once, as if their time had come.
func (c *fakeClock) fire() {
	c.mu.Lock()
	var due []*fakeTimer
	for _, t := range c.timers {
		if !t.stopped {
			t.stopped = true
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		t.fn()
	}
}

// ---- helpers ----

var t0 = time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC)

type hubEnv struct {
	hub     *Hub
	sub     *fakeSubscriber
	status  *fakeStatus
	clock   *fakeClock
	metrics *fakeMetrics
}

func newHubEnv(t *testing.T) *hubEnv {
	t.Helper()
	env := &hubEnv{
		sub:     newFakeSubscriber(),
		status:  &fakeStatus{},
		clock:   &fakeClock{now: t0},
		metrics: &fakeMetrics{},
	}
	env.hub = NewHub(env.sub, env.status, HubOptions{Clock: env.clock, Metrics: env.metrics})
	t.Cleanup(env.hub.Close)
	return env
}

func readingPayload(t *testing.T, houseID string, seq int64) []byte {
	t.Helper()
	payload, err := encodeReading(redisstore.MeterProjection{HouseID: houseID, MeterID: "meter-" + houseID, Seq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func onlinePayload(t *testing.T, at time.Time) []byte {
	t.Helper()
	payload, err := encodeOnline(at)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// next waits for the stream's next delivery and takes it.
func next(t *testing.T, s *Stream) (*Status, *redisstore.MeterProjection) {
	t.Helper()
	select {
	case <-s.Notify():
		return s.Take()
	case <-time.After(time.Second):
		t.Fatal("no event delivered")
		return nil, nil
	}
}

func assertNothing(t *testing.T, s *Stream) {
	t.Helper()
	select {
	case <-s.Notify():
		status, reading := s.Take()
		t.Fatalf("unexpected delivery: status %+v, reading %+v", status, reading)
	case <-time.After(50 * time.Millisecond):
	}
}

func watch(t *testing.T, hub *Hub, houseID string) *Stream {
	t.Helper()
	s, err := hub.Watch(context.Background(), houseID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// ---- publisher ----

func TestPublisherSendsReadingsToTheHouseChannel(t *testing.T) {
	sender, metrics := &fakeSender{}, &fakeMetrics{}
	p := NewPublisher(sender, nil, metrics)

	p.PublishReading(context.Background(), redisstore.MeterProjection{HouseID: "h1", MeterID: "m1", Seq: 7, NetKw: 0.3})

	got := sender.published["house:h1:live"]
	if len(got) != 1 {
		t.Fatalf("published %v, want one message on house:h1:live", sender.published)
	}
	var msg struct {
		V       int    `json:"v"`
		Type    string `json:"type"`
		Reading struct {
			MeterID string  `json:"meter_id"`
			Seq     int64   `json:"seq"`
			NetKw   float64 `json:"net_kw"`
		} `json:"reading"`
	}
	if err := json.Unmarshal([]byte(got[0]), &msg); err != nil {
		t.Fatal(err)
	}
	if msg.V != 1 || msg.Type != "reading" || msg.Reading.MeterID != "m1" || msg.Reading.Seq != 7 || msg.Reading.NetKw != 0.3 {
		t.Fatalf("message = %s", got[0])
	}
	if metrics.published["reading"] != 1 {
		t.Fatalf("published metric = %v", metrics.published)
	}
}

func TestPublisherSendsOnlineStatus(t *testing.T) {
	sender := &fakeSender{}
	NewPublisher(sender, nil, nil).PublishOnline(context.Background(), "h1", t0)

	want := `{"v":1,"type":"status","status":"online","last_heartbeat_at":"2026-11-20T09:30:00Z"}`
	if got := sender.published["house:h1:live"]; len(got) != 1 || got[0] != want {
		t.Fatalf("published %v, want %s", got, want)
	}
}

func TestPublisherCountsFailuresWithoutReturningThem(t *testing.T) {
	metrics := &fakeMetrics{}
	p := NewPublisher(&fakeSender{err: errors.New("redis down")}, nil, metrics)

	p.PublishReading(context.Background(), redisstore.MeterProjection{HouseID: "h1"})
	p.PublishOnline(context.Background(), "h1", t0)

	if metrics.publishErrors != 2 {
		t.Fatalf("publish errors = %d, want 2", metrics.publishErrors)
	}
}

// ---- hub ----

func TestHubDeliversReadingsToEveryStreamOfTheHouse(t *testing.T) {
	env := newHubEnv(t)
	a, b := watch(t, env.hub, "h1"), watch(t, env.hub, "h1")
	other := watch(t, env.hub, "h2")

	env.sub.deliver(t, "h1", readingPayload(t, "h1", 1))

	for _, s := range []*Stream{a, b} {
		if _, reading := next(t, s); reading == nil || reading.Seq != 1 {
			t.Fatalf("reading = %+v, want seq 1", reading)
		}
	}
	assertNothing(t, other)
}

func TestHubSubscribesOnFirstStreamAndUnsubscribesAfterTheLast(t *testing.T) {
	env := newHubEnv(t)
	a := watch(t, env.hub, "h1")
	b := watch(t, env.hub, "h1")

	a.Close()
	if got := env.sub.callLog(); len(got) != 1 || got[0] != "subscribe:h1" {
		t.Fatalf("calls = %v, want only the first subscribe", got)
	}
	b.Close()
	if got := env.sub.callLog(); len(got) != 2 || got[1] != "unsubscribe:h1" {
		t.Fatalf("calls = %v, want unsubscribe after the last stream", got)
	}
	if m := env.metrics.snapshot(); m.streams != 0 {
		t.Fatalf("streams gauge = %d, want 0", m.streams)
	}
}

func TestHubKeepsOnlyTheLatestReadingForASlowStream(t *testing.T) {
	env := newHubEnv(t)
	s := watch(t, env.hub, "h1")

	env.sub.deliver(t, "h1", readingPayload(t, "h1", 1))
	env.sub.deliver(t, "h1", readingPayload(t, "h1", 2))
	env.sub.deliver(t, "h1", onlinePayload(t, t0))
	// The hub has handled everything once a further message is accepted.
	env.sub.deliver(t, "h-unwatched", readingPayload(t, "h-unwatched", 1))

	status, reading := next(t, s)
	if reading == nil || reading.Seq != 2 {
		t.Fatalf("reading = %+v, want only the latest (seq 2)", reading)
	}
	if status == nil || !status.Online || !status.LastHeartbeatAt.Equal(t0) {
		t.Fatalf("status = %+v, want online at %v kept next to the reading", status, t0)
	}
	if m := env.metrics.snapshot(); m.conflated != 1 {
		t.Fatalf("conflated = %d, want 1", m.conflated)
	}
}

func TestHubIgnoresMalformedEvents(t *testing.T) {
	env := newHubEnv(t)
	s := watch(t, env.hub, "h1")

	env.sub.deliver(t, "h1", []byte("not json"))
	env.sub.deliver(t, "h1", []byte(`{"v":2,"type":"reading"}`))
	env.sub.deliver(t, "h1", []byte(`{"v":1,"type":"unknown"}`))

	assertNothing(t, s)
}

func TestHubReportsOfflineWhenTheStatusExpires(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0.Add(-9*time.Minute))
	s := watch(t, env.hub, "h1")

	// Checked just after the liveness window ends.
	if got := env.clock.pending(); len(got) != 1 || got[0] != time.Minute+offlineGrace {
		t.Fatalf("timers = %v, want one at 1m + grace", got)
	}

	env.status.expire("h1")
	env.clock.fire()

	status, reading := next(t, s)
	if status == nil || status.Online || reading != nil {
		t.Fatalf("status = %+v, reading = %+v; want offline only", status, reading)
	}
	if m := env.metrics.snapshot(); m.offline != 1 {
		t.Fatalf("offline detected = %d, want 1", m.offline)
	}
	if got := env.clock.pending(); len(got) != 0 {
		t.Fatalf("timers = %v, want none while offline", got)
	}
}

func TestHubReschedulesWhileHeartbeatsContinue(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0.Add(-9*time.Minute))
	s := watch(t, env.hub, "h1")

	// A newer heartbeat arrived meanwhile; heartbeats on an online house publish nothing.
	env.status.set("h1", t0.Add(-2*time.Minute))
	env.clock.fire()

	assertNothing(t, s)
	if got := env.clock.pending(); len(got) != 1 || got[0] != 8*time.Minute+offlineGrace {
		t.Fatalf("timers = %v, want one at 8m + grace", got)
	}
}

func TestHubRetriesTheCheckWhenRedisFails(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0.Add(-9*time.Minute))
	s := watch(t, env.hub, "h1")

	env.status.err = errors.New("redis down")
	env.clock.fire()

	assertNothing(t, s)
	if got := env.clock.pending(); len(got) != 1 || got[0] != offlineRetry {
		t.Fatalf("timers = %v, want a retry in %v", got, offlineRetry)
	}
}

func TestHubWatchesForOfflineAgainWhenTheHouseComesOnline(t *testing.T) {
	env := newHubEnv(t)
	s := watch(t, env.hub, "h1")
	if got := env.clock.pending(); len(got) != 0 {
		t.Fatalf("timers = %v, want none for an offline house", got)
	}

	env.sub.deliver(t, "h1", onlinePayload(t, t0))

	if status, _ := next(t, s); status == nil || !status.Online {
		t.Fatalf("status = %+v, want online", status)
	}
	if got := env.clock.pending(); len(got) != 1 || got[0] != redisstore.HeartbeatTTL+offlineGrace {
		t.Fatalf("timers = %v, want one at the TTL + grace", got)
	}
}

func TestHubStopsTheTimerWhenNobodyWatches(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0)
	s := watch(t, env.hub, "h1")

	s.Close()

	if got := env.clock.pending(); len(got) != 0 {
		t.Fatalf("timers = %v, want none", got)
	}
}

func TestHubWatchFailsWhenRedisSubscribeFails(t *testing.T) {
	env := newHubEnv(t)
	env.sub.subscribeErr = errors.New("redis down")

	if _, err := env.hub.Watch(context.Background(), "h1"); err == nil {
		t.Fatal("Watch succeeded although Redis refused the subscription")
	}

	env.sub.subscribeErr = nil
	watch(t, env.hub, "h1")
	if got := env.sub.callLog(); len(got) != 1 || got[0] != "subscribe:h1" {
		t.Fatalf("calls = %v, want a fresh subscribe on the next Watch", got)
	}
}

func TestHubCloseEndsStreamsAndRefusesNewOnes(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0)
	s := watch(t, env.hub, "h1")

	env.hub.Close()

	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("stream not ended by Close")
	}
	if _, err := env.hub.Watch(context.Background(), "h1"); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("err = %v, want ErrHubClosed", err)
	}
	if !env.sub.closed {
		t.Fatal("Redis subscription not closed")
	}
	if got := env.clock.pending(); len(got) != 0 {
		t.Fatalf("timers = %v, want none after Close", got)
	}
	s.Close() // safe after the hub closed
}

func TestHubIgnoresAReplacedTimerThatFiresAnyway(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0.Add(-9*time.Minute))
	s := watch(t, env.hub, "h1")
	env.clock.mu.Lock()
	replaced := env.clock.timers[0]
	env.clock.mu.Unlock()

	// A newer heartbeat replaces the timer, but the old one had already started.
	env.sub.deliver(t, "h1", onlinePayload(t, t0))
	next(t, s)
	env.status.expire("h1")
	replaced.fn()

	assertNothing(t, s)
	if got := env.clock.pending(); len(got) != 1 || got[0] != redisstore.HeartbeatTTL+offlineGrace {
		t.Fatalf("timers = %v, want only the replacement", got)
	}
}

// watchAsync starts Watch in the background; the result arrives on the channel.
func watchAsync(hub *Hub, houseID string) chan *Stream {
	out := make(chan *Stream, 1)
	go func() {
		s, err := hub.Watch(context.Background(), houseID)
		if err != nil {
			s = nil
		}
		out <- s
	}()
	return out
}

func awaitStream(t *testing.T, ch chan *Stream) *Stream {
	t.Helper()
	select {
	case s := <-ch:
		if s == nil {
			t.Fatal("Watch failed")
		}
		t.Cleanup(s.Close)
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return")
		return nil
	}
}

func TestHubKeepsDeliveringWhileReadingANewHousesStatus(t *testing.T) {
	env := newHubEnv(t)
	other := watch(t, env.hub, "h2")
	env.status.gate = newGate()

	pending := watchAsync(env.hub, "h1")
	env.status.gate.waitArrived(t)
	env.sub.deliver(t, "h2", readingPayload(t, "h2", 1))

	if _, reading := next(t, other); reading == nil {
		t.Fatal("h2 got nothing while h1's status was being read")
	}
	close(env.status.gate.release)
	awaitStream(t, pending)
}

func TestHubKeepsDeliveringWhileSubscribingANewHouse(t *testing.T) {
	env := newHubEnv(t)
	other := watch(t, env.hub, "h2")
	env.sub.subscribeGate = newGate()

	pending := watchAsync(env.hub, "h1")
	env.sub.subscribeGate.waitArrived(t)
	env.sub.deliver(t, "h2", readingPayload(t, "h2", 1))

	if _, reading := next(t, other); reading == nil {
		t.Fatal("h2 got nothing while h1 was being subscribed")
	}
	close(env.sub.subscribeGate.release)
	awaitStream(t, pending)
}

func TestHubSubscribesOnceForConcurrentFirstStreams(t *testing.T) {
	env := newHubEnv(t)
	env.sub.subscribeGate = newGate()

	first := watchAsync(env.hub, "h1")
	env.sub.subscribeGate.waitArrived(t)
	second := watchAsync(env.hub, "h1")
	time.Sleep(20 * time.Millisecond)
	close(env.sub.subscribeGate.release)
	a, b := awaitStream(t, first), awaitStream(t, second)

	if got := env.sub.callLog(); len(got) != 1 || got[0] != "subscribe:h1" {
		t.Fatalf("calls = %v, want one subscribe", got)
	}
	env.sub.deliver(t, "h1", readingPayload(t, "h1", 1))
	for _, s := range []*Stream{a, b} {
		if _, reading := next(t, s); reading == nil {
			t.Fatal("a stream got nothing")
		}
	}
}

func TestHubConcurrentFirstStreamsAllFailWhenSubscribeFails(t *testing.T) {
	env := newHubEnv(t)
	env.sub.subscribeGate = newGate()
	env.sub.subscribeErr = errors.New("redis down")

	first := watchAsync(env.hub, "h1")
	env.sub.subscribeGate.waitArrived(t)
	second := watchAsync(env.hub, "h1")
	time.Sleep(20 * time.Millisecond)
	close(env.sub.subscribeGate.release)

	for _, ch := range []chan *Stream{first, second} {
		select {
		case s := <-ch:
			if s != nil {
				t.Fatal("Watch succeeded although Redis refused the subscription")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Watch did not return")
		}
	}
	if m := env.metrics.snapshot(); m.streams != 0 {
		t.Fatalf("streams gauge = %d, want 0", m.streams)
	}
}

func TestHubStaysSubscribedWhenAHouseIsWatchedAgainWhileUnsubscribing(t *testing.T) {
	env := newHubEnv(t)
	s := watch(t, env.hub, "h1")
	env.sub.unsubscribeGate = newGate()

	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	env.sub.unsubscribeGate.waitArrived(t)
	again := watchAsync(env.hub, "h1")
	time.Sleep(20 * time.Millisecond)
	close(env.sub.unsubscribeGate.release)
	<-closed
	fresh := awaitStream(t, again)

	env.sub.mu.Lock()
	subscribed := env.sub.subscribed["h1"]
	env.sub.mu.Unlock()
	if !subscribed {
		t.Fatalf("h1 is not subscribed although it is watched; calls %v", env.sub.callLog())
	}
	env.sub.deliver(t, "h1", readingPayload(t, "h1", 1))
	if _, reading := next(t, fresh); reading == nil {
		t.Fatal("the new stream got nothing")
	}
}

func TestHubCloseWhileAFirstWatchIsSubscribing(t *testing.T) {
	env := newHubEnv(t)
	env.sub.subscribeGate = newGate()

	pending := watchAsync(env.hub, "h1")
	env.sub.subscribeGate.waitArrived(t)
	closed := make(chan struct{})
	go func() { env.hub.Close(); close(closed) }()
	time.Sleep(20 * time.Millisecond)
	close(env.sub.subscribeGate.release)

	select {
	case s := <-pending:
		if s != nil {
			t.Fatal("Watch succeeded on a closed hub")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return")
	}
	<-closed
	if m := env.metrics.snapshot(); m.streams != 0 {
		t.Fatalf("streams gauge = %d, want 0", m.streams)
	}
}

func TestHubKeepsAnOnlineEventThatArrivesDuringTheFirstStatusRead(t *testing.T) {
	env := newHubEnv(t)
	env.status.set("h1", t0.Add(-9*time.Minute))
	env.status.gate = newGate()

	pending := watchAsync(env.hub, "h1")
	env.status.gate.waitArrived(t)
	// A newer heartbeat is announced while the older status is being read.
	env.sub.deliver(t, "h1", onlinePayload(t, t0))
	env.sub.deliver(t, "h-unwatched", readingPayload(t, "h-unwatched", 1))
	close(env.status.gate.release)
	awaitStream(t, pending)

	if got := env.clock.pending(); len(got) != 1 || got[0] != redisstore.HeartbeatTTL+offlineGrace {
		t.Fatalf("timers = %v, want only the one from the newer heartbeat", got)
	}
}

func TestHubSkipsTheUnsubscribeOfAHouseWatchedAgainBeforeItRan(t *testing.T) {
	env := newHubEnv(t)
	s := watch(t, env.hub, "h1")
	// A slow subscribe for another house makes h1's unsubscribe wait its turn.
	env.sub.subscribeGate = newGate()
	other := watchAsync(env.hub, "h2")
	env.sub.subscribeGate.waitArrived(t)

	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	time.Sleep(20 * time.Millisecond)
	again := watchAsync(env.hub, "h1")
	time.Sleep(20 * time.Millisecond)
	close(env.sub.subscribeGate.release)
	<-closed
	awaitStream(t, other)
	awaitStream(t, again)

	for _, call := range env.sub.callLog() {
		if call == "unsubscribe:h1" {
			t.Fatalf("h1 was unsubscribed although it was watched again first; calls %v", env.sub.callLog())
		}
	}
	env.sub.mu.Lock()
	defer env.sub.mu.Unlock()
	if !env.sub.subscribed["h1"] {
		t.Fatal("h1 is not subscribed")
	}
}
