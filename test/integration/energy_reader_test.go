//go:build integration

package integration

import (
	"context"
	"math"
	"testing"
	"time"

	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

const energyHouse = "grid01-house0042" // registered by newEnv

type storageFixture struct {
	assetID   string
	assetType string
	powerKw   float64
}

func insertReading(t *testing.T, e *env, at time.Time, solar, cons, net float64, storage ...storageFixture) {
	t.Helper()
	db := openDB(t, e.dsn)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO iot_data.meter_readings ("time", meter_id, house_id, grid_id, device_class, schema_version, seq, solar_kw, consumption_kw, net_kw)
		VALUES ($1, 'meter-grid01-house0042', $2, 'grid01', 'residential_prosumer', '1.0', $3, $4, $5, $6)`,
		at, energyHouse, at.Unix(), solar, cons, net); err != nil {
		t.Fatal(err)
	}
	for _, s := range storage {
		var pluggedIn any
		if s.assetType == "ev" {
			pluggedIn = true
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO iot_data.storage_asset_readings ("time", meter_id, house_id, asset_id, asset_type, soc_pct, power_kw, capacity_kwh, plugged_in)
			VALUES ($1, 'meter-grid01-house0042', $2, $3, $4, 50, $5, 10, $6)`,
			at, energyHouse, s.assetID, s.assetType, s.powerKw, pluggedIn); err != nil {
			t.Fatal(err)
		}
	}
}

func near(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %.12f, want %.12f", name, got, want)
	}
}

var energyBase = time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

func TestEnergySummaryFlows(t *testing.T) {
	e := newEnv(t)
	insertReading(t, e, energyBase, 6, 2, 1, storageFixture{"bat_1", "bess", 3})
	insertReading(t, e, energyBase.Add(10*time.Second), 0, 4, -3, storageFixture{"bat_1", "bess", -1})
	insertReading(t, e, energyBase.Add(20*time.Second), 2, 2, 0)

	s, err := e.pg.EnergySummary(context.Background(), energyHouse, energyBase, energyBase.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	const h = 1.0 / 360 // each reading lasts 10 s
	near(t, "generated", s.Totals.GeneratedKwh, 8*h)
	near(t, "consumed", s.Totals.ConsumedKwh, 8*h)
	near(t, "exported", s.Totals.ExportedKwh, 1*h)
	near(t, "imported", s.Totals.ImportedKwh, 3*h)
	near(t, "storage charged", s.Totals.StorageChargedKwh, 3*h)
	near(t, "storage discharged", s.Totals.StorageDischargedKwh, 1*h)
	near(t, "solar->home", s.Flows.SolarToHomeKwh, 4*h)
	near(t, "solar->storage", s.Flows.SolarToStorageKwh, 3*h)
	near(t, "solar->grid", s.Flows.SolarToGridKwh, 1*h)
	near(t, "storage->home", s.Flows.StorageToHomeKwh, 1*h)
	near(t, "grid->home", s.Flows.GridToHomeKwh, 3*h)
	near(t, "grid->storage", s.Flows.GridToStorageKwh, 0)
	near(t, "storage->grid", s.Flows.StorageToGridKwh, 0)
	if s.PeakConsumptionKw == nil || *s.PeakConsumptionKw != 4 || s.PeakTime == nil || !s.PeakTime.Equal(energyBase.Add(10*time.Second)) {
		t.Errorf("peak = %v at %v, want 4 at +10s", s.PeakConsumptionKw, s.PeakTime)
	}
}

func TestEnergyGapCapped(t *testing.T) {
	e := newEnv(t)
	insertReading(t, e, energyBase, 0, 3.6, -3.6)
	insertReading(t, e, energyBase.Add(10*time.Minute), 0, 3.6, -3.6)

	s, err := e.pg.EnergySummary(context.Background(), energyHouse, energyBase, energyBase.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	near(t, "consumed", s.Totals.ConsumedKwh, 0.03)
	near(t, "imported", s.Totals.ImportedKwh, 0.03)
}

func TestEnergyNoStorageNoNulls(t *testing.T) {
	e := newEnv(t)
	insertReading(t, e, energyBase, 1, 3, -2)

	s, err := e.pg.EnergySummary(context.Background(), energyHouse, energyBase, energyBase.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	const h = 1.0 / 360
	near(t, "solar->home", s.Flows.SolarToHomeKwh, 1*h)
	near(t, "grid->home", s.Flows.GridToHomeKwh, 2*h)
	near(t, "storage charged", s.Totals.StorageChargedKwh, 0)
	near(t, "storage->home", s.Flows.StorageToHomeKwh, 0)

	series, err := e.pg.EnergySeries(context.Background(), energyHouse, energyBase, energyBase.Add(10*time.Second), 5*time.Minute, "UTC")
	if err != nil || len(series) != 1 {
		t.Fatalf("series = %+v, %v", series, err)
	}
	near(t, "series storage discharged", series[0].Energy.StorageDischargedKwh, 0)
}

func TestEnergySummaryEmptyRange(t *testing.T) {
	e := newEnv(t)
	s, err := e.pg.EnergySummary(context.Background(), energyHouse, energyBase, energyBase.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if s.Totals != (postgresstore.EnergyTotals{}) || s.Flows != (postgresstore.EnergyFlows{}) || s.PeakConsumptionKw != nil || s.PeakTime != nil {
		t.Fatalf("empty range summary = %+v", s)
	}
	series, err := e.pg.EnergySeries(context.Background(), energyHouse, energyBase, energyBase.Add(time.Hour), time.Hour, "UTC")
	if err != nil || len(series) != 0 {
		t.Fatalf("empty range series = %+v, %v", series, err)
	}
}

func TestEnergySeriesDubaiDayBuckets(t *testing.T) {
	e := newEnv(t)
	insertReading(t, e, time.Date(2026, 10, 21, 19, 59, 50, 0, time.UTC), 1, 1, 0)
	insertReading(t, e, time.Date(2026, 10, 21, 20, 0, 10, 0, time.UTC), 1, 1, 0)

	start := time.Date(2026, 10, 20, 20, 0, 0, 0, time.UTC)
	series, err := e.pg.EnergySeries(context.Background(), energyHouse, start, start.Add(48*time.Hour), 24*time.Hour, "Asia/Dubai")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{start, start.Add(24 * time.Hour)}
	if len(series) != 2 || !series[0].Start.Equal(want[0]) || !series[1].Start.Equal(want[1]) {
		t.Fatalf("bucket starts = %+v, want %v", series, want)
	}
}

func TestEnergySeriesCoveredHours(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 4; i++ {
		insertReading(t, e, energyBase.Add(time.Duration(i)*5*time.Second), 3.6, 0, 3.6)
	}

	series, err := e.pg.EnergySeries(context.Background(), energyHouse, energyBase, energyBase.Add(20*time.Second), 5*time.Minute, "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || !series[0].Start.Equal(energyBase) {
		t.Fatalf("series = %+v", series)
	}
	near(t, "covered hours", series[0].CoveredHours, 20.0/3600)
	near(t, "generated", series[0].Energy.GeneratedKwh, 3.6*20/3600)
	near(t, "exported", series[0].Energy.ExportedKwh, 3.6*20/3600)
}
