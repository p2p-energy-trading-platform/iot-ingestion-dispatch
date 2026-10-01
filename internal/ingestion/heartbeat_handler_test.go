package ingestion

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

const validHeartbeatJSON = `{
  "schema_version": "1.0",
  "grid_id": "grid01",
  "house_id": "grid01-house0042",
  "meter_id": "meter-grid01-house0042",
  "status": "online",
  "device_class": "residential_prosumer",
  "rated_solar_kw": 5.0,
  "flexible_assets": [
    {"asset_id": "bat_001", "asset_type": "bess", "capacity_kwh": 10.0, "max_charge_kw": 3.5, "max_discharge_kw": 3.5},
    {"asset_id": "ev_001", "asset_type": "ev", "capacity_kwh": 40.0, "max_charge_kw": 7.2, "max_discharge_kw": 3.6, "v2g_capable": true}
  ]
}`

type hbStoreCall struct {
	hb domain.Heartbeat
	at time.Time
}

type fakeHBStore struct {
	order *[]string
	calls []hbStoreCall
	err   error
}

func (f *fakeHBStore) ApplyHeartbeat(_ context.Context, hb domain.Heartbeat, at time.Time) error {
	*f.order = append(*f.order, "postgres")
	f.calls = append(f.calls, hbStoreCall{hb: hb, at: at})
	return f.err
}

type hbCacheCall struct {
	gridID, houseID string
	status          redisstore.HouseStatus
}

type fakeHBCache struct {
	order *[]string
	calls []hbCacheCall
	err   error
}

func (f *fakeHBCache) UpdateHeartbeat(_ context.Context, gridID, houseID string, s redisstore.HouseStatus) error {
	*f.order = append(*f.order, "redis")
	f.calls = append(f.calls, hbCacheCall{gridID: gridID, houseID: houseID, status: s})
	return f.err
}

func newTestHeartbeatHandler(fixed time.Time) (*HeartbeatHandler, *fakeHBStore, *fakeHBCache, *[]string) {
	order := &[]string{}
	store := &fakeHBStore{order: order}
	cache := &fakeHBCache{order: order}
	h := NewHeartbeatHandler(
		fakeAdmitter{known: map[string]bool{"grid01": true}},
		store, cache,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	h.now = func() time.Time { return fixed }
	return h, store, cache, order
}

func TestHeartbeatHandlerHappyPathOrderAndSharedTimestamp(t *testing.T) {
	fixed := time.Date(2026, 10, 1, 8, 15, 30, 123456789, time.UTC)
	h, store, cache, order := newTestHeartbeatHandler(fixed)

	if err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON)); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(*order, []string{"postgres", "redis"}) {
		t.Fatalf("call order = %v, want [postgres redis]", *order)
	}

	want := fixed.UTC().Truncate(time.Microsecond)
	if !store.calls[0].at.Equal(want) {
		t.Fatalf("postgres timestamp = %v, want %v", store.calls[0].at, want)
	}
	if !cache.calls[0].status.LastHeartbeatAt.Equal(store.calls[0].at) {
		t.Fatalf("redis timestamp %v differs from postgres timestamp %v",
			cache.calls[0].status.LastHeartbeatAt, store.calls[0].at)
	}

	c := cache.calls[0]
	if c.gridID != "grid01" || c.houseID != "grid01-house0042" || c.status.Status != "online" {
		t.Fatalf("unexpected redis call: %+v", c)
	}
	if got := len(store.calls[0].hb.FlexibleAssets); got != 2 {
		t.Fatalf("store received %d assets, want 2", got)
	}
}

func TestHeartbeatHandlerInvalidPayloadTouchesNothing(t *testing.T) {
	h, store, cache, _ := newTestHeartbeatHandler(time.Now())

	err := h.HandleRecord(context.Background(), nil, []byte(`{"schema_version":"1.0"`))
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("error = %v, want ErrValidation", err)
	}
	if len(store.calls) != 0 || len(cache.calls) != 0 {
		t.Fatal("stores were called for an invalid payload")
	}
}

func TestHeartbeatHandlerUnknownGridTouchesNothing(t *testing.T) {
	h, store, cache, _ := newTestHeartbeatHandler(time.Now())
	h.admission = fakeAdmitter{known: map[string]bool{"grid99": true}}

	err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON))
	if !errors.Is(err, admission.ErrUnknownGrid) {
		t.Fatalf("error = %v, want ErrUnknownGrid", err)
	}
	if len(store.calls) != 0 || len(cache.calls) != 0 {
		t.Fatal("stores were called for an unknown grid")
	}
}

func TestHeartbeatHandlerStoreFailureSkipsRedis(t *testing.T) {
	h, store, cache, _ := newTestHeartbeatHandler(time.Now())
	store.err = domain.ErrIntegrityConflict

	err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}
	if len(cache.calls) != 0 {
		t.Fatal("redis was updated after a failed postgres write")
	}
}

func TestHeartbeatHandlerRedisFailureIsReturned(t *testing.T) {
	errRedis := errors.New("redis down")
	h, _, cache, _ := newTestHeartbeatHandler(time.Now())
	cache.err = errRedis

	err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON))
	if !errors.Is(err, errRedis) {
		t.Fatalf("error = %v, want it to wrap errRedis", err)
	}
}

func TestCheckDuplicateAssetIDs(t *testing.T) {
	ok := domain.Heartbeat{FlexibleAssets: []domain.FlexibleAsset{{AssetID: "a"}, {AssetID: "b"}}}
	if err := checkDuplicateAssetIDs(ok); err != nil {
		t.Fatalf("unique ids: error = %v", err)
	}

	dup := domain.Heartbeat{FlexibleAssets: []domain.FlexibleAsset{{AssetID: "a"}, {AssetID: "a"}}}
	if err := checkDuplicateAssetIDs(dup); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("duplicate ids: error = %v, want ErrValidation", err)
	}
}
