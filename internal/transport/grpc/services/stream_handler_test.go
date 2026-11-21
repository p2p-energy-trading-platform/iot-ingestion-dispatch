package services

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/live"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/query"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/test/helpers"
)

// fakeRedis stands in for Redis pub/sub: what the publisher sends to a
// watched house's channel reaches the hub's subscriber.
type fakeRedis struct {
	mu         sync.Mutex
	messages   chan redisstore.LiveMessage
	subscribed map[string]bool
	closed     bool
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{messages: make(chan redisstore.LiveMessage, 16), subscribed: map[string]bool{}}
}

func (f *fakeRedis) Publish(_ context.Context, channel string, payload []byte) error {
	house := strings.TrimSuffix(strings.TrimPrefix(channel, "house:"), ":live")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribed[house] && !f.closed {
		f.messages <- redisstore.LiveMessage{HouseID: house, Payload: payload}
	}
	return nil
}

func (f *fakeRedis) Messages() <-chan redisstore.LiveMessage { return f.messages }

func (f *fakeRedis) Subscribe(_ context.Context, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		f.subscribed[id] = true
	}
	return nil
}

func (f *fakeRedis) Unsubscribe(_ context.Context, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.subscribed, id)
	}
	return nil
}

func (f *fakeRedis) isSubscribed(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subscribed[id]
}

func (f *fakeRedis) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.messages)
	}
	return nil
}

func (f *fakeRedis) GetHouseStatus(context.Context, string) (redisstore.HouseStatus, error) {
	return redisstore.HouseStatus{}, redisstore.ErrNotFound
}

type streamEnv struct {
	client    iotv1.IotServiceClient
	queries   *fakeQueries
	redis     *fakeRedis
	hub       *live.Hub
	publisher *live.Publisher
}

// newStreamEnv serves the handler over an in-memory gRPC connection.
func newStreamEnv(t *testing.T, maxAge time.Duration) *streamEnv {
	t.Helper()
	env := &streamEnv{
		queries: &fakeQueries{
			house: query.House{
				HouseInfoRow: postgresstore.HouseInfoRow{HouseID: "h1", GridID: "grid01"},
				Online:       true, LastHeartbeatAt: &beat,
			},
			latest: redisstore.MeterProjection{HouseID: "h1", MeterID: "m1", Seq: 1, EventTimeUnixMs: beat.UnixMilli()},
		},
		redis: newFakeRedis(),
	}
	env.hub = live.NewHub(env.redis, env.redis, live.HubOptions{Logger: helpers.TestLogger()})
	t.Cleanup(env.hub.Close)
	env.publisher = live.NewPublisher(env.redis, helpers.TestLogger())

	handler := NewIotHandler(env.queries, helpers.TestLogger()).WithLive(env.hub, maxAge)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	iotv1.RegisterIotServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	env.client = iotv1.NewIotServiceClient(conn)
	return env
}

func (env *streamEnv) open(t *testing.T, ctx context.Context, req *iotv1.StreamHouseLiveRequest) grpc.ServerStreamingClient[iotv1.StreamHouseLiveResponse] {
	t.Helper()
	stream, err := env.client.StreamHouseLive(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func recv(t *testing.T, stream grpc.ServerStreamingClient[iotv1.StreamHouseLiveResponse]) *iotv1.StreamHouseLiveResponse {
	t.Helper()
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	return msg
}

func recvCode(t *testing.T, stream grpc.ServerStreamingClient[iotv1.StreamHouseLiveResponse]) codes.Code {
	t.Helper()
	for {
		if _, err := stream.Recv(); err != nil {
			return status.Code(err)
		}
	}
}

// waitFor polls until the condition holds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for start := time.Now(); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Since(start) > 2*time.Second {
			t.Fatal("timed out")
		}
	}
}

func TestStreamHouseLiveSendsTheSnapshotThenLiveEvents(t *testing.T) {
	env := newStreamEnv(t, time.Minute)
	stream := env.open(t, context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1", OwnerUserId: "u1"})

	first := recv(t, stream).GetStatus()
	if first.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_ONLINE || !first.GetLastHeartbeatAt().AsTime().Equal(beat) {
		t.Fatalf("first message status = %v, want online at %v", first, beat)
	}
	if r := recv(t, stream).GetReading(); r.GetMeterId() != "m1" || r.GetSeq() != 1 || !r.GetMeasuredAt().AsTime().Equal(beat) {
		t.Fatalf("second message reading = %v, want the latest reading", r)
	}
	if env.queries.gotOwner != "" || env.queries.gotHouseID != "h1" {
		t.Fatalf("latest reading read for %q/%q; ownership is checked once, before", env.queries.gotHouseID, env.queries.gotOwner)
	}

	waitFor(t, func() bool { return env.redis.isSubscribed("h1") })
	env.publisher.PublishReading(context.Background(), redisstore.MeterProjection{
		HouseID: "h1", MeterID: "m1", Seq: 2, SolarKw: 1.4,
		StorageAssets: []redisstore.StorageAssetProjection{{AssetID: "bat", AssetType: "bess", SocPct: 63}},
	})
	r := recv(t, stream).GetReading()
	if r.GetSeq() != 2 || r.GetSolarKw() != 1.4 || len(r.GetStorageAssets()) != 1 ||
		r.GetStorageAssets()[0].GetAssetType() != iotv1.AssetType_ASSET_TYPE_BESS {
		t.Fatalf("live reading = %v", r)
	}

	// A real heartbeat carries the time it was received, so the hub does not treat it as expired.
	later := time.Now().UTC().Truncate(time.Second)
	env.publisher.PublishOnline(context.Background(), "h1", later)
	if s := recv(t, stream).GetStatus(); s.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_ONLINE || !s.GetLastHeartbeatAt().AsTime().Equal(later) {
		t.Fatalf("live status = %v", s)
	}
}

func TestStreamHouseLiveSnapshotWithoutReadingOrHeartbeat(t *testing.T) {
	env := newStreamEnv(t, time.Minute)
	env.queries.house = query.House{HouseInfoRow: postgresstore.HouseInfoRow{HouseID: "h1", GridID: "grid01"}}
	env.queries.latestErr = domain.ErrNotFound
	stream := env.open(t, context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1"})

	s := recv(t, stream).GetStatus()
	if s.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_OFFLINE || s.GetLastHeartbeatAt() != nil {
		t.Fatalf("status = %v, want offline without a heartbeat time", s)
	}

	waitFor(t, func() bool { return env.redis.isSubscribed("h1") })
	env.publisher.PublishReading(context.Background(), redisstore.MeterProjection{HouseID: "h1", Seq: 5})
	if r := recv(t, stream).GetReading(); r.GetSeq() != 5 {
		t.Fatalf("next message = %v, want the live reading (no reading snapshot)", r)
	}
}

func TestStreamHouseLiveChecksTheHouseFirst(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"unknown or not owned", domain.ErrNotFound, codes.NotFound},
		{"bad ID", domain.NewValidationError("house_id", "is required"), codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newStreamEnv(t, time.Minute)
			env.queries.err = tc.err
			stream := env.open(t, context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1", OwnerUserId: "u2"})

			if got := recvCode(t, stream); got != tc.want {
				t.Fatalf("code = %v, want %v", got, tc.want)
			}
			if env.queries.gotOwner != "u2" {
				t.Fatalf("owner checked = %q, want u2", env.queries.gotOwner)
			}
			if env.redis.isSubscribed("h1") {
				t.Fatal("subscribed to a house the caller may not watch")
			}
		})
	}
}

func TestStreamHouseLiveEndsAtItsMaximumAge(t *testing.T) {
	env := newStreamEnv(t, 100*time.Millisecond)
	stream := env.open(t, context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1"})

	if got := recvCode(t, stream); got != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded", got)
	}
	waitFor(t, func() bool { return !env.redis.isSubscribed("h1") })
}

func TestStreamHouseLiveEndsWithUnavailableWhenTheHubCloses(t *testing.T) {
	env := newStreamEnv(t, time.Minute)
	stream := env.open(t, context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1"})
	recv(t, stream)
	recv(t, stream)

	env.hub.Close()

	if got := recvCode(t, stream); got != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", got)
	}
}

func TestStreamHouseLiveReleasesTheHouseWhenTheClientLeaves(t *testing.T) {
	env := newStreamEnv(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	stream := env.open(t, ctx, &iotv1.StreamHouseLiveRequest{HouseId: "h1"})
	recv(t, stream)
	waitFor(t, func() bool { return env.redis.isSubscribed("h1") })

	cancel()

	waitFor(t, func() bool { return !env.redis.isSubscribed("h1") })
}
