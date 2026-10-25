//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
)

func scalar[T any](t *testing.T, dsn, query string, args ...any) T {
	t.Helper()
	var v T
	if err := openDB(t, dsn).QueryRowContext(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v
}

func TestMigrationsCleanInstall(t *testing.T) {
	dsn := newTimescale(t)
	ctx := context.Background()

	if _, err := gooseProvider(t, dsn).Up(ctx); err != nil {
		t.Fatalf("clean install: %v", err)
	}

	for _, table := range []string{"grids", "houses", "flexible_assets", "discovered_assets",
		"meter_readings", "storage_asset_readings", "ingestion_failures"} {
		if scalar[bool](t, dsn, `SELECT to_regclass('iot_data.`+table+`') IS NOT NULL`) == false {
			t.Errorf("table iot_data.%s missing after migration", table)
		}
	}

	if n := scalar[int](t, dsn, `SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema='iot_data'`); n != 2 {
		t.Errorf("hypertables = %d, want 2", n)
	}

	// 03-timescaledb-warm-storage.md: six month retention on both hypertables,
	// and a compression/columnstore policy.
	for _, table := range []string{"meter_readings", "storage_asset_readings"} {
		retention := scalar[string](t, dsn, `
			SELECT config->>'drop_after' FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_retention' AND hypertable_schema='iot_data' AND hypertable_name=$1`, table)
		if !strings.Contains(retention, "6 mon") {
			t.Errorf("%s retention = %q, want 6 months", table, retention)
		}
		if n := scalar[int](t, dsn, `
			SELECT count(*) FROM timescaledb_information.jobs
			WHERE proc_name IN ('policy_compression','policy_columnstore')
			  AND hypertable_schema='iot_data' AND hypertable_name=$1`, table); n != 1 {
			t.Errorf("%s compression policies = %d, want 1", table, n)
		}
	}

	// Idempotent: running it again changes nothing.
	if res, err := gooseProvider(t, dsn).Up(ctx); err != nil || len(res) != 0 {
		t.Errorf("second Up applied %d migration(s), err=%v; want none", len(res), err)
	}
}

func TestMigrationsUpgradeFromPreviousRelease(t *testing.T) {
	dsn := newTimescale(t)
	ctx := context.Background()
	p := gooseProvider(t, dsn)

	// Version 8 is the last schema released before 00009/00010.
	if _, err := p.UpTo(ctx, 8); err != nil {
		t.Fatalf("migrate to 8: %v", err)
	}
	db := openDB(t, dsn)
	for _, stmt := range []string{
		`INSERT INTO iot_data.grids (grid_id, lat, lon) VALUES ('grid01', 6.9271, 79.8612)`,
		`INSERT INTO iot_data.houses (house_id, grid_id, device_class, meter_id)
		 VALUES ('grid01-house0001', 'grid01', 'consumer', 'meter-grid01-house0001')`,
		`INSERT INTO iot_data.meter_readings ("time", meter_id, house_id, grid_id, device_class, schema_version, seq, solar_kw, consumption_kw, net_kw)
		 VALUES (now() - interval '2 days', 'meter-grid01-house0001', 'grid01-house0001', 'grid01', 'consumer', '1.0', 1, 0, 1.5, -1.5)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	if _, err := p.Up(ctx); err != nil {
		t.Fatalf("upgrade 8 -> latest with existing data: %v", err)
	}
	if n := scalar[int](t, dsn, `SELECT count(*) FROM iot_data.meter_readings`); n != 1 {
		t.Errorf("readings after upgrade = %d, want the 1 fixture row", n)
	}
}

func TestMigrationsDownThenUpAgain(t *testing.T) {
	dsn := newTimescale(t)
	ctx := context.Background()
	p := gooseProvider(t, dsn)

	if _, err := p.Up(ctx); err != nil {
		t.Fatal(err)
	}
	// Roll back 00010 (seed) and 00009 (constraints + columnstore), then forward.
	for i := 0; i < 2; i++ {
		if _, err := p.Down(ctx); err != nil {
			t.Fatalf("down step %d: %v", i+1, err)
		}
	}
	if n := scalar[int](t, dsn, `
		SELECT count(*) FROM timescaledb_information.jobs
		WHERE proc_name IN ('policy_compression','policy_columnstore') AND hypertable_schema='iot_data'`); n != 0 {
		t.Errorf("compression policies after rollback = %d, want 0", n)
	}
	if _, err := p.Up(ctx); err != nil {
		t.Fatalf("forward after rollback: %v", err)
	}
}

func TestDatabaseRejectsInvalidTelemetry(t *testing.T) {
	dsn := newTimescale(t)
	ctx := context.Background()
	if _, err := gooseProvider(t, dsn).Up(ctx); err != nil {
		t.Fatal(err)
	}
	db := openDB(t, dsn)

	const insert = `INSERT INTO iot_data.meter_readings
		("time", meter_id, house_id, grid_id, device_class, schema_version, seq, solar_kw, consumption_kw, net_kw)
		VALUES (now(), $1, $2, 'grid01', 'consumer', '1.0', 1, $3, 1, $4)`

	tests := []struct {
		name         string
		meter, house string
		solar, net   float64
	}{
		{"empty meter id", "", "h", 1, 1},
		{"empty house id", "m", "", 1, 1},
		{"negative solar", "m", "h", -1, 1},
	}
	for _, tt := range tests {
		if _, err := db.ExecContext(ctx, insert, tt.meter, tt.house, tt.solar, tt.net); err == nil {
			t.Errorf("%s: database accepted invalid telemetry", tt.name)
		}
	}
	if _, err := db.ExecContext(ctx, insert, "m", "h", 1, "NaN"); err == nil {
		t.Error("database accepted NaN net_kw")
	}
	if _, err := db.ExecContext(ctx, insert, "m", "h", 1, 1.0); err != nil {
		t.Errorf("valid telemetry rejected: %v", err)
	}
}
