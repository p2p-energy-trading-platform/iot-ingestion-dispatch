package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

func TestCompareAsset(t *testing.T) {
	base := domain.FlexibleAsset{
		AssetID: "ev_001", AssetType: domain.AssetTypeEV,
		CapacityKwh: 40, MaxChargeKw: 7.2, MaxDischargeKw: 3.6, V2GCapable: boolp(true),
	}
	with := func(mutate func(*domain.FlexibleAsset)) domain.FlexibleAsset {
		a := base
		a.V2GCapable = boolp(true)
		mutate(&a)
		return a
	}

	tests := []struct {
		name      string
		storedHse string
		incoming  domain.FlexibleAsset
		want      assetAction
	}{
		{"identical", "h1", with(func(*domain.FlexibleAsset) {}), assetUnchanged},
		{"capacity changed", "h1", with(func(a *domain.FlexibleAsset) { a.CapacityKwh = 60 }), assetChanged},
		{"max charge changed", "h1", with(func(a *domain.FlexibleAsset) { a.MaxChargeKw = 11 }), assetChanged},
		{"max discharge changed", "h1", with(func(a *domain.FlexibleAsset) { a.MaxDischargeKw = 0 }), assetChanged},
		{"v2g changed", "h1", with(func(a *domain.FlexibleAsset) { a.V2GCapable = boolp(false) }), assetChanged},
		{"v2g removed", "h1", with(func(a *domain.FlexibleAsset) { a.V2GCapable = nil }), assetChanged},
		{"different owner", "h2", with(func(*domain.FlexibleAsset) {}), assetConflict},
		{"different type", "h1", with(func(a *domain.FlexibleAsset) { a.AssetType = domain.AssetTypeBESS }), assetConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareAsset(tt.storedHse, base, tt.incoming, "h1"); got != tt.want {
				t.Fatalf("compareAsset = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---- integration (requires a migrated TimescaleDB) --------------------

type hbFixture struct {
	store   *Store
	gridID  string
	houseID string
}

func newHBFixture(t *testing.T) hbFixture {
	t.Helper()
	store := integrationStore(t)
	ctx := context.Background()

	n := time.Now().UnixNano()
	f := hbFixture{
		store:   store,
		gridID:  fmt.Sprintf("gridhbt%d", n),
		houseID: fmt.Sprintf("gridhbt%d-house0001", n),
	}

	if _, err := store.pool.Exec(ctx,
		`INSERT INTO iot_data.grids (grid_id, lat, lon) VALUES ($1, 6.9, 79.8)`, f.gridID); err != nil {
		t.Fatalf("insert test grid: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.pool.Exec(ctx,
			`DELETE FROM iot_data.flexible_assets WHERE house_id IN (SELECT house_id FROM iot_data.houses WHERE grid_id = $1)`, f.gridID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM iot_data.houses WHERE grid_id = $1`, f.gridID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM iot_data.grids WHERE grid_id = $1`, f.gridID)
	})
	return f
}

func (f hbFixture) heartbeat(houseID string, assets ...domain.FlexibleAsset) domain.Heartbeat {
	return domain.Heartbeat{
		SchemaVersion:  "1.0",
		GridID:         f.gridID,
		HouseID:        houseID,
		MeterID:        "meter-" + houseID,
		Status:         "online",
		DeviceClass:    domain.DeviceClassResidentialProsumer,
		RatedSolarKw:   5,
		FlexibleAssets: assets,
	}
}

func (f hbFixture) assetID(suffix string) string { return f.houseID + "-" + suffix }

func bessAsset(id string, capacity float64) domain.FlexibleAsset {
	return domain.FlexibleAsset{AssetID: id, AssetType: domain.AssetTypeBESS,
		CapacityKwh: capacity, MaxChargeKw: 3.5, MaxDischargeKw: 3.5}
}

func (f hbFixture) houseLastHeartbeat(t *testing.T, houseID string) time.Time {
	t.Helper()
	var ts time.Time
	if err := f.store.pool.QueryRow(context.Background(),
		`SELECT last_heartbeat_at FROM iot_data.houses WHERE house_id = $1`, houseID).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	return ts
}

func (f hbFixture) assetState(t *testing.T, assetID string) (capacity float64, lastSeen time.Time) {
	t.Helper()
	if err := f.store.pool.QueryRow(context.Background(),
		`SELECT capacity_kwh, last_seen_at FROM iot_data.flexible_assets WHERE asset_id = $1`, assetID).
		Scan(&capacity, &lastSeen); err != nil {
		t.Fatal(err)
	}
	return capacity, lastSeen
}

func (f hbFixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.store.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestApplyHeartbeatDiscoversHouseAndAssets(t *testing.T) {
	f := newHBFixture(t)
	hb := f.heartbeat(f.houseID, bessAsset(f.assetID("bat"), 10))
	at := time.Now().UTC().Truncate(time.Microsecond)

	if err := f.store.ApplyHeartbeat(context.Background(), hb, at); err != nil {
		t.Fatal(err)
	}

	if got := f.houseLastHeartbeat(t, f.houseID); !got.Equal(at) {
		t.Fatalf("house last_heartbeat_at = %v, want %v", got, at)
	}
	capacity, assetSeen := f.assetState(t, f.assetID("bat"))
	if capacity != 10 || !assetSeen.Equal(at) {
		t.Fatalf("asset = (%v, %v), want (10, %v)", capacity, assetSeen, at)
	}
}

func TestApplyHeartbeatReplayRefreshesLivenessButNotUnchangedAssets(t *testing.T) {
	f := newHBFixture(t)
	hb := f.heartbeat(f.houseID, bessAsset(f.assetID("bat"), 10))
	first := time.Now().UTC().Truncate(time.Microsecond)
	second := first.Add(time.Minute)
	ctx := context.Background()

	if err := f.store.ApplyHeartbeat(ctx, hb, first); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ApplyHeartbeat(ctx, hb, second); err != nil {
		t.Fatal(err)
	}

	if got := f.houseLastHeartbeat(t, f.houseID); !got.Equal(second) {
		t.Fatalf("house last_heartbeat_at = %v, want %v", got, second)
	}
	if _, assetSeen := f.assetState(t, f.assetID("bat")); !assetSeen.Equal(first) {
		t.Fatalf("unchanged asset was written: last_seen_at = %v, want %v", assetSeen, first)
	}
}

func TestApplyHeartbeatNeverMovesLivenessBackwards(t *testing.T) {
	f := newHBFixture(t)
	hb := f.heartbeat(f.houseID)
	later := time.Now().UTC().Truncate(time.Microsecond)
	ctx := context.Background()

	if err := f.store.ApplyHeartbeat(ctx, hb, later); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ApplyHeartbeat(ctx, hb, later.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := f.houseLastHeartbeat(t, f.houseID); !got.Equal(later) {
		t.Fatalf("last_heartbeat_at = %v, want %v (must not regress)", got, later)
	}
}

func TestApplyHeartbeatUpdatesChangedCapability(t *testing.T) {
	f := newHBFixture(t)
	id := f.assetID("bat")
	first := time.Now().UTC().Truncate(time.Microsecond)
	second := first.Add(time.Minute)
	ctx := context.Background()

	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID, bessAsset(id, 10)), first); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID, bessAsset(id, 15)), second); err != nil {
		t.Fatal(err)
	}

	capacity, assetSeen := f.assetState(t, id)
	if capacity != 15 || !assetSeen.Equal(second) {
		t.Fatalf("asset = (%v, %v), want (15, %v)", capacity, assetSeen, second)
	}
}

func TestApplyHeartbeatHouseIdentityMismatchIsConflict(t *testing.T) {
	f := newHBFixture(t)
	at := time.Now().UTC()
	ctx := context.Background()

	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID), at); err != nil {
		t.Fatal(err)
	}

	changed := f.heartbeat(f.houseID)
	changed.DeviceClass = domain.DeviceClassCommercial
	err := f.store.ApplyHeartbeat(ctx, changed, at.Add(time.Minute))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}
}

func TestApplyHeartbeatAssetOwnedByOtherHouseRollsBack(t *testing.T) {
	f := newHBFixture(t)
	at := time.Now().UTC()
	ctx := context.Background()
	shared := f.assetID("bat")

	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID, bessAsset(shared, 10)), at); err != nil {
		t.Fatal(err)
	}

	otherHouse := f.gridID + "-house0002"
	err := f.store.ApplyHeartbeat(ctx, f.heartbeat(otherHouse, bessAsset(shared, 10)), at.Add(time.Minute))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}

	if n := f.count(t, `SELECT count(*) FROM iot_data.houses WHERE house_id = $1`, otherHouse); n != 0 {
		t.Fatalf("second house persisted despite conflict (rows = %d), transaction did not roll back", n)
	}
	var owner string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT house_id FROM iot_data.flexible_assets WHERE asset_id = $1`, shared).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != f.houseID {
		t.Fatalf("asset owner = %q, want %q (ownership must not change)", owner, f.houseID)
	}
}

func TestApplyHeartbeatUnprovisionedGridFails(t *testing.T) {
	f := newHBFixture(t)
	hb := f.heartbeat("nogrid-house0001")
	hb.GridID = "gridthatdoesnotexist"

	err := f.store.ApplyHeartbeat(context.Background(), hb, time.Now().UTC())
	if err == nil {
		t.Fatal("expected an error for a grid missing from iot_data.grids")
	}
	if errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("missing grid should not be reported as an integrity conflict: %v", err)
	}
}
