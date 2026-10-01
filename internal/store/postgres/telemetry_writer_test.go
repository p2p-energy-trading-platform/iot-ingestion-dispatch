package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

func f64(v float64) *float64 { return &v }
func boolp(v bool) *bool     { return &v }

func baseReading() domain.MeterReading {
	return domain.MeterReading{
		SchemaVersion: "1.0",
		MeterID:       "meter-grid01-house0042",
		HouseID:       "grid01-house0042",
		GridID:        "grid01",
		DeviceClass:   domain.DeviceClassResidentialProsumer,
		EventTime:     time.Date(2026, 6, 16, 9, 30, 5, 0, time.UTC),
		Seq:           1,
		SolarKw:       2.41, ConsumptionKw: 0.87, NetKw: 1.54,
		WeatherIrradianceWm2: f64(612),
		StorageAssets: []domain.StorageAssetReading{
			{AssetID: "bat_001", AssetType: domain.AssetTypeBESS, SocPct: 63.5, PowerKw: -0.3, CapacityKwh: 10},
			{AssetID: "ev_001", AssetType: domain.AssetTypeEV, SocPct: 41, PowerKw: -1.5, CapacityKwh: 40, PluggedIn: boolp(true)},
		},
	}
}

func TestReadingsEqual(t *testing.T) {
	a := baseReading()

	same := baseReading()
	same.StorageAssets[0], same.StorageAssets[1] = same.StorageAssets[1], same.StorageAssets[0]
	if !readingsEqual(a, same) {
		t.Fatal("identical readings with reordered assets should be equal")
	}

	mutations := map[string]func(*domain.MeterReading){
		"solar":          func(r *domain.MeterReading) { r.SolarKw = 9 },
		"net":            func(r *domain.MeterReading) { r.NetKw = 9 },
		"house":          func(r *domain.MeterReading) { r.HouseID = "grid01-house0001" },
		"device class":   func(r *domain.MeterReading) { r.DeviceClass = domain.DeviceClassConsumer },
		"irradiance":     func(r *domain.MeterReading) { r.WeatherIrradianceWm2 = f64(1) },
		"irradiance nil": func(r *domain.MeterReading) { r.WeatherIrradianceWm2 = nil },
		"asset soc":      func(r *domain.MeterReading) { r.StorageAssets[0].SocPct = 1 },
		"asset plugged":  func(r *domain.MeterReading) { r.StorageAssets[1].PluggedIn = boolp(false) },
		"asset removed":  func(r *domain.MeterReading) { r.StorageAssets = r.StorageAssets[:1] },
		"asset id":       func(r *domain.MeterReading) { r.StorageAssets[0].AssetID = "bat_002" },
		"event time":     func(r *domain.MeterReading) { r.EventTime = r.EventTime.Add(time.Second) },
		"schema version": func(r *domain.MeterReading) { r.SchemaVersion = "2.0" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			b := baseReading()
			mutate(&b)
			if readingsEqual(a, b) {
				t.Fatal("readings should differ")
			}
		})
	}
}

func TestReadingsEqualDoesNotMutateInput(t *testing.T) {
	a, b := baseReading(), baseReading()
	readingsEqual(a, b)
	if a.StorageAssets[0].AssetID != "bat_001" {
		t.Fatal("readingsEqual reordered its input")
	}
}

// ---- integration (requires a migrated TimescaleDB) --------------------

func integrationStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	store, err := New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func uniqueReading(t *testing.T, store *Store) domain.MeterReading {
	t.Helper()
	r := baseReading()
	r.MeterID = fmt.Sprintf("meter-grid01-test%d", time.Now().UnixNano())
	r.HouseID = "grid01-house0042"

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.pool.Exec(ctx, `DELETE FROM iot_data.storage_asset_readings WHERE meter_id = $1`, r.MeterID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM iot_data.meter_readings WHERE meter_id = $1`, r.MeterID)
	})
	return r
}

func countRows(t *testing.T, store *Store, table, meterID string) int {
	t.Helper()
	var n int
	q := fmt.Sprintf(`SELECT count(*) FROM iot_data.%s WHERE meter_id = $1`, table)
	if err := store.pool.QueryRow(context.Background(), q, meterID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestInsertMeterReadingNewAndReplay(t *testing.T) {
	store := integrationStore(t)
	r := uniqueReading(t, store)
	ctx := context.Background()

	inserted, err := store.InsertMeterReading(ctx, r)
	if err != nil || !inserted {
		t.Fatalf("first insert = (%v, %v), want (true, nil)", inserted, err)
	}

	inserted, err = store.InsertMeterReading(ctx, r)
	if err != nil || inserted {
		t.Fatalf("replay = (%v, %v), want (false, nil)", inserted, err)
	}

	if n := countRows(t, store, "meter_readings", r.MeterID); n != 1 {
		t.Fatalf("meter_readings rows = %d, want 1", n)
	}
	if n := countRows(t, store, "storage_asset_readings", r.MeterID); n != 2 {
		t.Fatalf("storage_asset_readings rows = %d, want 2", n)
	}
}

func TestInsertMeterReadingReplayWithNanosecondTimestamp(t *testing.T) {
	store := integrationStore(t)
	r := uniqueReading(t, store)
	r.EventTime = r.EventTime.Add(123456789 * time.Nanosecond)
	ctx := context.Background()

	if _, err := store.InsertMeterReading(ctx, r); err != nil {
		t.Fatal(err)
	}
	inserted, err := store.InsertMeterReading(ctx, r)
	if err != nil || inserted {
		t.Fatalf("replay = (%v, %v), want (false, nil)", inserted, err)
	}
}

func TestInsertMeterReadingConflictingDuplicate(t *testing.T) {
	store := integrationStore(t)
	r := uniqueReading(t, store)
	ctx := context.Background()

	if _, err := store.InsertMeterReading(ctx, r); err != nil {
		t.Fatal(err)
	}

	conflicting := r
	conflicting.SolarKw = 99
	_, err := store.InsertMeterReading(ctx, conflicting)
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}

	var solar float64
	if err := store.pool.QueryRow(ctx,
		`SELECT solar_kw FROM iot_data.meter_readings WHERE meter_id = $1`, r.MeterID).Scan(&solar); err != nil {
		t.Fatal(err)
	}
	if solar != r.SolarKw {
		t.Fatalf("stored solar_kw = %v, history was modified (want %v)", solar, r.SolarKw)
	}
}

func TestInsertMeterReadingConflictingAssetsRollsBackParent(t *testing.T) {
	store := integrationStore(t)
	r := uniqueReading(t, store)
	r.StorageAssets = append(r.StorageAssets, r.StorageAssets[0]) // duplicate asset_id

	_, err := store.InsertMeterReading(context.Background(), r)
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}
	if n := countRows(t, store, "meter_readings", r.MeterID); n != 0 {
		t.Fatalf("parent rows = %d after failed insert, want 0 (rolled back)", n)
	}
	if n := countRows(t, store, "storage_asset_readings", r.MeterID); n != 0 {
		t.Fatalf("child rows = %d after failed insert, want 0", n)
	}
}
