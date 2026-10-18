package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

// These tests need a real Redis (Lua scripts and TTLs are the behaviour under
// test). Set TEST_REDIS_ADDRESS, for example localhost:6379; CI provides one.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDRESS")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDRESS not set")
	}
	store, err := New(context.Background(), Config{Address: addr, DB: 15, MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// uniqueHouse keeps parallel packages and reruns from sharing keys.
func uniqueHouse(t *testing.T, store *Store) (grid, house string) {
	t.Helper()
	suffix := time.Now().UnixNano()
	grid, house = fmt.Sprintf("gridT%d", suffix), fmt.Sprintf("%d-houseT", suffix)
	t.Cleanup(func() {
		ctx := context.Background()
		store.rdb.Del(ctx, MeterLatestKey(grid, house), HouseStatusKey(house), GridHousesKey(grid))
	})
	return grid, house
}

func projection(grid, house string, at time.Time, seq int64, net float64) MeterProjection {
	return MeterProjection{
		EventTimeUnixMs: at.UnixMilli(), Seq: seq,
		GridID: grid, HouseID: house, MeterID: "meter-" + house, NetKw: net,
	}
}

func TestSetLatestIfNewerOrdering(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	grid, house := uniqueHouse(t, store)
	t0 := time.Now().UTC().Truncate(time.Millisecond)

	steps := []struct {
		name        string
		at          time.Time
		seq         int64
		net         float64
		wantApplied bool
		wantNet     float64
	}{
		{"first write", t0, 5, 1, true, 1},
		{"older time is ignored", t0.Add(-time.Second), 99, 2, false, 1},
		{"same time lower seq is ignored", t0, 4, 3, false, 1},
		{"duplicate is a no-op", t0, 5, 4, false, 1},
		{"same time higher seq wins", t0, 6, 5, true, 5},
		{"newer time wins even with lower seq", t0.Add(time.Second), 1, 6, true, 6},
	}
	for _, s := range steps {
		applied, err := store.SetLatestIfNewer(ctx, projection(grid, house, s.at, s.seq, s.net))
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if applied != s.wantApplied {
			t.Fatalf("%s: applied = %v, want %v", s.name, applied, s.wantApplied)
		}
		got, err := store.GetLatest(ctx, grid, house)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got.NetKw != s.wantNet {
			t.Fatalf("%s: stored net_kw = %v, want %v", s.name, got.NetKw, s.wantNet)
		}
	}
}

func TestSetLatestHasNoTTL(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	grid, house := uniqueHouse(t, store)

	if _, err := store.SetLatestIfNewer(ctx, projection(grid, house, time.Now(), 1, 1)); err != nil {
		t.Fatal(err)
	}
	if ttl := store.rdb.TTL(ctx, MeterLatestKey(grid, house)).Val(); ttl != -1 {
		t.Fatalf("latest key TTL = %v, want -1 (no expiry)", ttl)
	}
}

func TestGetLatestNotFound(t *testing.T) {
	store := newTestStore(t)
	grid, house := uniqueHouse(t, store)
	if _, err := store.GetLatest(context.Background(), grid, house); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestUpdateHeartbeatSetsStatusTTLAndMembership(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	grid, house := uniqueHouse(t, store)
	at := time.Now().UTC().Truncate(time.Second)

	if err := store.UpdateHeartbeat(ctx, grid, house, HouseStatus{Status: "online", LastHeartbeatAt: at}); err != nil {
		t.Fatal(err)
	}
	// Idempotent: a replay changes nothing.
	if err := store.UpdateHeartbeat(ctx, grid, house, HouseStatus{Status: "online", LastHeartbeatAt: at}); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetHouseStatus(ctx, house)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "online" || !got.LastHeartbeatAt.Equal(at) {
		t.Fatalf("status = %+v, want online at %v", got, at)
	}

	ttl := store.rdb.TTL(ctx, HouseStatusKey(house)).Val()
	if ttl <= 0 || ttl > HeartbeatTTL {
		t.Fatalf("status TTL = %v, want within (0, %v]", ttl, HeartbeatTTL)
	}

	ids, err := store.GridHouseIDs(ctx, grid)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != house {
		t.Fatalf("grid membership = %v, want [%s]", ids, house)
	}
	if ttl := store.rdb.TTL(ctx, GridHousesKey(grid)).Val(); ttl != -1 {
		t.Fatalf("membership TTL = %v, want -1 (no expiry)", ttl)
	}
}

func TestRestoreHeartbeatUsesRemainingLiveness(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	grid, house := uniqueHouse(t, store)
	now := time.Now().UTC()

	// Last heartbeat 8 minutes ago: only ~2 minutes of liveness remain.
	restored, err := store.RestoreHeartbeat(ctx, grid, house,
		HouseStatus{Status: "online", LastHeartbeatAt: now.Add(-8 * time.Minute)}, now)
	if err != nil || !restored {
		t.Fatalf("restored = %v, err = %v, want true, nil", restored, err)
	}
	ttl := store.rdb.TTL(ctx, HouseStatusKey(house)).Val()
	if ttl <= 0 || ttl > 2*time.Minute+time.Second {
		t.Fatalf("restored TTL = %v, want about 2m", ttl)
	}

	// Heartbeat outside the window: no status key is created.
	grid2, house2 := uniqueHouse(t, store)
	restored, err = store.RestoreHeartbeat(ctx, grid2, house2,
		HouseStatus{Status: "online", LastHeartbeatAt: now.Add(-11 * time.Minute)}, now)
	if err != nil || restored {
		t.Fatalf("restored = %v, err = %v, want false, nil", restored, err)
	}
	if _, err := store.GetHouseStatus(ctx, house2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired house must have no status key, got %v", err)
	}
}

func TestProjectionOfKeepsStorageAssets(t *testing.T) {
	plugged := true
	p := ProjectionOf(domain.MeterReading{
		MeterID: "m", HouseID: "h", GridID: "g", Seq: 3,
		EventTime: time.UnixMilli(1700000000123),
		StorageAssets: []domain.StorageAssetReading{
			{AssetID: "ev1", AssetType: domain.AssetTypeEV, SocPct: 50, PluggedIn: &plugged},
		},
	})
	if p.EventTimeUnixMs != 1700000000123 || p.Seq != 3 || len(p.StorageAssets) != 1 ||
		p.StorageAssets[0].PluggedIn == nil || !*p.StorageAssets[0].PluggedIn {
		t.Fatalf("unexpected projection %+v", p)
	}
}

func TestLivenessAndLatestSafe(t *testing.T) {
	for policy, want := range map[string]bool{
		"noeviction": true, "volatile-lru": true, "volatile-ttl": true,
		"allkeys-lru": false, "allkeys-lfu": false, "allkeys-random": false, "": false,
	} {
		if got := LivenessAndLatestSafe(policy); got != want {
			t.Errorf("LivenessAndLatestSafe(%q) = %v, want %v", policy, got, want)
		}
	}
}

func TestConfigOptions(t *testing.T) {
	opts := Config{Address: "r:6379", MaxRetries: 0}.options()
	if opts.MaxRetries != -1 {
		t.Errorf("MaxRetries = %d, want -1 (explicitly disabled)", opts.MaxRetries)
	}
	if opts.TLSConfig != nil {
		t.Error("TLS must be off unless requested")
	}
	secure := Config{Address: "r:6380", TLS: true, MaxRetries: 3}.options()
	if secure.TLSConfig == nil || secure.TLSConfig.InsecureSkipVerify {
		t.Error("TLS must verify certificates by default")
	}
	if secure.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", secure.MaxRetries)
	}
}
