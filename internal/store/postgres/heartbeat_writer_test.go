package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
			`DELETE FROM iot_data.discovered_assets WHERE observed_house_id IN (SELECT house_id FROM iot_data.houses WHERE grid_id = $1)`, f.gridID)
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

// registerAsset stands in for the authoritative registration flow, which is
// not part of the heartbeat service. The house must already exist.
func (f hbFixture) registerAsset(t *testing.T, houseID string, a domain.FlexibleAsset, seenAt time.Time) {
	t.Helper()
	if _, err := f.store.pool.Exec(context.Background(),
		`INSERT INTO iot_data.flexible_assets
		    (asset_id, house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw,
		     v2g_capable, first_seen_at, last_seen_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		a.AssetID, houseID, string(a.AssetType), a.CapacityKwh, a.MaxChargeKw, a.MaxDischargeKw,
		a.V2GCapable, seenAt); err != nil {
		t.Fatalf("register asset: %v", err)
	}
}

type assetRecord struct {
	house    string
	capacity float64
	lastSeen time.Time
	found    bool
}

func (f hbFixture) assetRecord(t *testing.T, table, houseColumn, assetID string) assetRecord {
	t.Helper()
	q := fmt.Sprintf(`SELECT %s, capacity_kwh, last_seen_at FROM iot_data.%s WHERE asset_id = $1`, houseColumn, table)

	var r assetRecord
	err := f.store.pool.QueryRow(context.Background(), q, assetID).Scan(&r.house, &r.capacity, &r.lastSeen)
	switch {
	case err == nil:
		r.found = true
	case errors.Is(err, pgx.ErrNoRows):
		// not found: r.found stays false
	default:
		t.Fatal(err)
	}
	return r
}

func (f hbFixture) discovered(t *testing.T, assetID string) assetRecord {
	return f.assetRecord(t, "discovered_assets", "observed_house_id", assetID)
}

func (f hbFixture) registered(t *testing.T, assetID string) assetRecord {
	return f.assetRecord(t, "flexible_assets", "house_id", assetID)
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

func (f hbFixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.store.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestApplyHeartbeatRecordsUnknownAssetAsDiscoveredNotRegistered(t *testing.T) {
	f := newHBFixture(t)
	id := f.assetID("bat")
	at := time.Now().UTC().Truncate(time.Microsecond)

	if err := f.store.ApplyHeartbeat(context.Background(), f.heartbeat(f.houseID, bessAsset(id, 10)), at); err != nil {
		t.Fatal(err)
	}

	if got := f.houseLastHeartbeat(t, f.houseID); !got.Equal(at) {
		t.Fatalf("house last_heartbeat_at = %v, want %v", got, at)
	}

	d := f.discovered(t, id)
	if !d.found || d.house != f.houseID || d.capacity != 10 || !d.lastSeen.Equal(at) {
		t.Fatalf("discovered = %+v, want observed under %s, capacity 10, seen at %v", d, f.houseID, at)
	}
	if r := f.registered(t, id); r.found {
		t.Fatalf("a heartbeat created a registered asset: %+v", r)
	}
}

func TestApplyHeartbeatReplayRefreshesLivenessButNotUnchangedDiscoveredAsset(t *testing.T) {
	f := newHBFixture(t)
	id := f.assetID("bat")
	hb := f.heartbeat(f.houseID, bessAsset(id, 10))
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
	if d := f.discovered(t, id); !d.lastSeen.Equal(first) {
		t.Fatalf("unchanged observation was written: last_seen_at = %v, want %v", d.lastSeen, first)
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

func TestApplyHeartbeatUpdatesChangedCapabilityOfDiscoveredAsset(t *testing.T) {
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

	d := f.discovered(t, id)
	if d.capacity != 15 || !d.lastSeen.Equal(second) {
		t.Fatalf("discovered = %+v, want capacity 15 seen at %v", d, second)
	}
}

func TestApplyHeartbeatUpdatesCapabilitiesOfRegisteredAssetOnly(t *testing.T) {
	f := newHBFixture(t)
	id := f.assetID("bat")
	first := time.Now().UTC().Truncate(time.Microsecond)
	second := first.Add(time.Minute)
	third := first.Add(2 * time.Minute)
	ctx := context.Background()

	// The house has to exist before an asset can be registered to it.
	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID), first); err != nil {
		t.Fatal(err)
	}
	f.registerAsset(t, f.houseID, bessAsset(id, 10), first)

	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID, bessAsset(id, 15)), second); err != nil {
		t.Fatal(err)
	}
	r := f.registered(t, id)
	if r.house != f.houseID || r.capacity != 15 || !r.lastSeen.Equal(second) {
		t.Fatalf("registered = %+v, want owner %s, capacity 15, seen at %v", r, f.houseID, second)
	}

	// An identical heartbeat changes nothing.
	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID, bessAsset(id, 15)), third); err != nil {
		t.Fatal(err)
	}
	if r := f.registered(t, id); !r.lastSeen.Equal(second) {
		t.Fatalf("unchanged registered asset was written: last_seen_at = %v, want %v", r.lastSeen, second)
	}

	if d := f.discovered(t, id); d.found {
		t.Fatalf("a registered asset was also recorded as discovered: %+v", d)
	}
}

func TestApplyHeartbeatAssetRegisteredToOtherHouseRollsBack(t *testing.T) {
	f := newHBFixture(t)
	shared := f.assetID("bat")
	at := time.Now().UTC().Truncate(time.Microsecond)
	ctx := context.Background()

	if err := f.store.ApplyHeartbeat(ctx, f.heartbeat(f.houseID), at); err != nil {
		t.Fatal(err)
	}
	f.registerAsset(t, f.houseID, bessAsset(shared, 10), at)

	otherHouse := f.gridID + "-house0002"
	err := f.store.ApplyHeartbeat(ctx, f.heartbeat(otherHouse, bessAsset(shared, 10)), at.Add(time.Minute))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}

	if n := f.count(t, `SELECT count(*) FROM iot_data.houses WHERE house_id = $1`, otherHouse); n != 0 {
		t.Fatalf("second house persisted despite conflict (rows = %d), transaction did not roll back", n)
	}
	if d := f.discovered(t, shared); d.found {
		t.Fatalf("conflicting asset was recorded as discovered: %+v", d)
	}
	if r := f.registered(t, shared); r.house != f.houseID {
		t.Fatalf("asset owner = %q, want %q (ownership must not change)", r.house, f.houseID)
	}
}

func TestApplyHeartbeatAssetDiscoveredUnderOtherHouseIsConflict(t *testing.T) {
	f := newHBFixture(t)
	shared := f.assetID("bat")
	at := time.Now().UTC().Truncate(time.Microsecond)
	ctx := context.Background()

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
	if d := f.discovered(t, shared); d.house != f.houseID {
		t.Fatalf("observed house = %q, want %q (observation must not move)", d.house, f.houseID)
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
