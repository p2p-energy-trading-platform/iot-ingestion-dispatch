//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/live"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/query"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"
	grpcservices "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc/services"
)

const liveHouse = "grid01-house0042"

// manualClock runs the hub's offline timers only when the test fires them;
// the real liveness window is 10 minutes.
type manualClock struct {
	mu     sync.Mutex
	timers []func()
}

type manualTimer struct{ stopped *bool }

func (t manualTimer) Stop() bool { *t.stopped = true; return true }

func (c *manualClock) Now() time.Time { return time.Now() }

func (c *manualClock) AfterFunc(_ time.Duration, fn func()) live.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	stopped := new(bool)
	c.timers = append(c.timers, func() {
		if !*stopped {
			fn()
		}
	})
	return manualTimer{stopped: stopped}
}

func (c *manualClock) fireAll() {
	c.mu.Lock()
	timers := c.timers
	c.timers = nil
	c.mu.Unlock()
	for _, fn := range timers {
		fn()
	}
}

type liveEnv struct {
	*env
	client iotv1.IotServiceClient
	hub    *live.Hub
	clock  *manualClock
}

// newLiveEnv wires live updates the way the app does: the real handlers
// publish to Redis, a hub on its own Redis connection fans out, and the real
// gRPC server serves StreamHouseLive over TCP.
func newLiveEnv(t *testing.T) *liveEnv {
	t.Helper()
	e := newEnv(t)
	ctx := context.Background()

	registry := admission.NewRegistry()
	if err := admission.NewRefresher(registry, e.pg, admission.RefresherConfig{}, quietLogger()).Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	publisher := live.NewPublisher(e.rd, quietLogger(), nil)
	e.router = ingestion.NewRouter()
	e.router.Register(meterTopic, ingestion.NewMeterHandler(registry, e.pg, e.rd, quietLogger()).WithLivePublisher(publisher))
	e.router.Register(heartbeatTopic, ingestion.NewHeartbeatHandler(registry, e.pg, e.rd, quietLogger()).WithLivePublisher(publisher))

	clock := &manualClock{}
	hub := live.NewHub(e.rd.NewLiveSubscriber(ctx), e.rd, live.HubOptions{Clock: clock, Logger: quietLogger()})
	t.Cleanup(hub.Close)

	handler := grpcservices.NewIotHandler(query.NewService(e.pg, e.pg, e.rd, e.rd), quietLogger()).WithLive(hub, time.Minute)
	address := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	server := grpctransport.New(address, quietLogger(), grpctransport.Services{Iot: handler})
	go func() { _ = server.Run() }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &liveEnv{env: e, client: iotv1.NewIotServiceClient(conn), hub: hub, clock: clock}
}

type liveStream = grpc.ServerStreamingClient[iotv1.StreamHouseLiveResponse]

func (e *liveEnv) open(t *testing.T, owner string) liveStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var stream liveStream
	var err error
	// The server starts in the background; retry until it answers.
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		stream, err = e.client.StreamHouseLive(ctx, &iotv1.StreamHouseLiveRequest{HouseId: liveHouse, OwnerUserId: owner}, grpc.WaitForReady(true))
		if err == nil || time.Since(start) > 5*time.Second {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func next(t *testing.T, stream liveStream) *iotv1.StreamHouseLiveResponse {
	t.Helper()
	type result struct {
		msg *iotv1.StreamHouseLiveResponse
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := stream.Recv()
		done <- result{msg, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		return r.msg
	case <-time.After(5 * time.Second):
		t.Fatal("no message within 5s")
		return nil
	}
}

// routeUntilDelivered routes a record again until its event arrives. Redis
// applies SUBSCRIBE asynchronously, so the first event after opening a stream
// can be missed; delivery is best effort by design.
func routeUntilDelivered(t *testing.T, e *liveEnv, stream liveStream, topic string, payload func(attempt int) string) *iotv1.StreamHouseLiveResponse {
	t.Helper()
	got := make(chan *iotv1.StreamHouseLiveResponse, 1)
	go func() {
		msg, err := stream.Recv()
		if err == nil {
			got <- msg
		}
	}()
	for attempt := 0; attempt < 50; attempt++ {
		if err := route(e.env, topic, payload(attempt)); err != nil {
			t.Fatal(err)
		}
		select {
		case msg := <-got:
			return msg
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("event never delivered")
	return nil
}

func TestLiveStreamSendsIngestedReadingsThatBecomeTheLatest(t *testing.T) {
	e := newLiveEnv(t)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

	stream := e.open(t, "")
	if s := next(t, stream).GetStatus(); s.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_OFFLINE {
		t.Fatalf("snapshot = %v, want offline (no heartbeat, no reading yet)", s)
	}

	first := routeUntilDelivered(t, e, stream, meterTopic, func(i int) string {
		return meterJSON(42, 100+i, t0.Add(time.Duration(i)*time.Second), 1.5)
	}).GetReading()
	if first.GetMeterId() != "meter-"+liveHouse || first.GetNetKw() != 1.5 || len(first.GetStorageAssets()) != 1 {
		t.Fatalf("reading = %v", first)
	}

	// An older reading is stored in history but is not live news.
	if err := route(e.env, meterTopic, meterJSON(42, 1, t0.Add(-time.Minute), 9)); err != nil {
		t.Fatal(err)
	}
	if err := route(e.env, meterTopic, meterJSON(42, 999, t0.Add(10*time.Minute), 2.5)); err != nil {
		t.Fatal(err)
	}
	if r := next(t, stream).GetReading(); r.GetSeq() != 999 || r.GetNetKw() != 2.5 {
		t.Fatalf("next reading = %v, want seq 999 (the older one must be skipped)", r)
	}
}

func TestLiveStreamSendsTheSnapshotOfAnOnlineHouse(t *testing.T) {
	e := newLiveEnv(t)
	at := time.Now().UTC().Truncate(time.Second)
	if err := route(e.env, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}
	if err := route(e.env, meterTopic, meterJSON(42, 7, at, 0.8)); err != nil {
		t.Fatal(err)
	}

	stream := e.open(t, "")

	if s := next(t, stream).GetStatus(); s.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_ONLINE || s.GetLastHeartbeatAt() == nil {
		t.Fatalf("snapshot status = %v, want online with a heartbeat time", s)
	}
	if r := next(t, stream).GetReading(); r.GetSeq() != 7 || !r.GetMeasuredAt().AsTime().Equal(at) {
		t.Fatalf("snapshot reading = %v, want seq 7 at %v", r, at)
	}
}

func TestLiveStreamReportsOnlineAndOffline(t *testing.T) {
	e := newLiveEnv(t)
	stream := e.open(t, "")
	next(t, stream) // offline snapshot

	online := routeUntilDelivered(t, e, stream, heartbeatTopic, func(int) string {
		// Until the event arrives, expire the status so each heartbeat counts as coming online.
		_ = e.rd.FlushForTest(context.Background())
		return heartbeatJSON(42, "grid01", "bat_0042", 10)
	}).GetStatus()
	if online.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_ONLINE {
		t.Fatalf("status = %v, want online", online)
	}

	// Further heartbeats publish nothing: the next message is this reading.
	if err := route(e.env, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}
	if err := route(e.env, meterTopic, meterJSON(42, 5, time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	if r := next(t, stream).GetReading(); r.GetSeq() != 5 {
		t.Fatalf("next = %v, want the reading (no status event for a house already online)", r)
	}

	// The status key expires at the end of the liveness window.
	if err := e.rd.FlushForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.clock.fireAll()
	if s := next(t, stream).GetStatus(); s.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_OFFLINE || s.GetLastHeartbeatAt() != nil {
		t.Fatalf("status = %v, want offline", s)
	}
}

func TestLiveStreamRefusesSomeoneElsesHouse(t *testing.T) {
	e := newLiveEnv(t)
	stream := e.open(t, "55555555-5555-5555-5555-555555555555")

	_, err := stream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound for a house the user does not own", err)
	}
}

func TestLiveStreamEndsWithUnavailableOnShutdown(t *testing.T) {
	e := newLiveEnv(t)
	stream := e.open(t, "")
	next(t, stream)

	e.hub.Close()

	_, err := stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
}
