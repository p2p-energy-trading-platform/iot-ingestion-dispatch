package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const pgUniqueViolation = "23505"

const insertMeterReadingSQL = `
INSERT INTO iot_data.meter_readings
    ("time", meter_id, house_id, grid_id, device_class, schema_version, seq,
     solar_kw, consumption_kw, net_kw, weather_irradiance_wm2, cloud_cover_pct)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT ("time", meter_id, seq) DO NOTHING`

const selectMeterReadingSQL = `
SELECT house_id, grid_id, device_class, schema_version,
       solar_kw, consumption_kw, net_kw, weather_irradiance_wm2, cloud_cover_pct
FROM iot_data.meter_readings
WHERE "time" = $1 AND meter_id = $2 AND seq = $3`

const insertStorageReadingSQL = `
INSERT INTO iot_data.storage_asset_readings
    ("time", meter_id, house_id, asset_id, asset_type,
     soc_pct, power_kw, capacity_kwh, plugged_in)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

const selectStorageReadingsSQL = `
SELECT asset_id, asset_type, soc_pct, power_kw, capacity_kwh, plugged_in
FROM iot_data.storage_asset_readings
WHERE "time" = $1 AND meter_id = $2
ORDER BY asset_id`

// InsertMeterReading durably records a reading and its storage-asset rows in
// one transaction, with immutable-history semantics:
//
//   - new reading: inserted, returns (true, nil)
//   - identical replay: nothing written, returns (false, nil)
//   - same identity, different values: domain.ErrIntegrityConflict, nothing
//     is written or updated
//
// Any other error is infrastructure failure and safe to retry.
func (store *Store) InsertMeterReading(ctx context.Context, r domain.MeterReading) (bool, error) {
	// timestamptz keeps microseconds; normalize so replays compare equal.
	r.EventTime = r.EventTime.UTC().Truncate(time.Microsecond)

	inserted := false

	err := pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, insertMeterReadingSQL,
			r.EventTime, r.MeterID, r.HouseID, r.GridID, string(r.DeviceClass),
			r.SchemaVersion, r.Seq, r.SolarKw, r.ConsumptionKw, r.NetKw,
			r.WeatherIrradianceWm2, r.CloudCoverPct,
		)
		if err != nil {
			return fmt.Errorf("insert meter reading: %w", err)
		}

		if tag.RowsAffected() == 0 {
			return verifyReplay(ctx, tx, r)
		}

		for _, a := range r.StorageAssets {
			if _, err := tx.Exec(ctx, insertStorageReadingSQL,
				r.EventTime, r.MeterID, r.HouseID, a.AssetID, string(a.AssetType),
				a.SocPct, a.PowerKw, a.CapacityKwh, a.PluggedIn,
			); err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
					return fmt.Errorf("%w: storage asset %q already recorded for meter %q at %s",
						domain.ErrIntegrityConflict, a.AssetID, r.MeterID, r.EventTime.Format(time.RFC3339Nano))
				}
				return fmt.Errorf("insert storage asset reading %q: %w", a.AssetID, err)
			}
		}

		inserted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

// verifyReplay loads the stored reading and confirms the incoming one is
// identical. Nothing is ever updated.
func verifyReplay(ctx context.Context, tx pgx.Tx, r domain.MeterReading) error {
	stored := domain.MeterReading{
		MeterID:   r.MeterID,
		EventTime: r.EventTime,
		Seq:       r.Seq,
	}

	var deviceClass string
	err := tx.QueryRow(ctx, selectMeterReadingSQL, r.EventTime, r.MeterID, r.Seq).Scan(
		&stored.HouseID, &stored.GridID, &deviceClass, &stored.SchemaVersion,
		&stored.SolarKw, &stored.ConsumptionKw, &stored.NetKw,
		&stored.WeatherIrradianceWm2, &stored.CloudCoverPct,
	)
	if err != nil {
		return fmt.Errorf("load existing meter reading: %w", err)
	}
	stored.DeviceClass = domain.DeviceClass(deviceClass)

	rows, err := tx.Query(ctx, selectStorageReadingsSQL, r.EventTime, r.MeterID)
	if err != nil {
		return fmt.Errorf("load existing storage readings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var a domain.StorageAssetReading
		var assetType string
		if err := rows.Scan(&a.AssetID, &assetType, &a.SocPct, &a.PowerKw, &a.CapacityKwh, &a.PluggedIn); err != nil {
			return fmt.Errorf("scan existing storage reading: %w", err)
		}
		a.AssetType = domain.AssetType(assetType)
		stored.StorageAssets = append(stored.StorageAssets, a)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load existing storage readings: %w", err)
	}

	if !readingsEqual(stored, r) {
		return fmt.Errorf("%w: meter %q seq %d at %s already recorded with different values",
			domain.ErrIntegrityConflict, r.MeterID, r.Seq, r.EventTime.Format(time.RFC3339Nano))
	}
	return nil
}

// readingsEqual compares every persisted field of two readings, ignoring the
// order of storage assets.
func readingsEqual(a, b domain.MeterReading) bool {
	if a.MeterID != b.MeterID || a.HouseID != b.HouseID || a.GridID != b.GridID ||
		a.DeviceClass != b.DeviceClass || a.SchemaVersion != b.SchemaVersion ||
		a.Seq != b.Seq || !a.EventTime.Equal(b.EventTime) ||
		a.SolarKw != b.SolarKw || a.ConsumptionKw != b.ConsumptionKw || a.NetKw != b.NetKw ||
		!equalOptional(a.WeatherIrradianceWm2, b.WeatherIrradianceWm2) ||
		!equalOptional(a.CloudCoverPct, b.CloudCoverPct) ||
		len(a.StorageAssets) != len(b.StorageAssets) {
		return false
	}

	sortedA := sortedAssets(a.StorageAssets)
	sortedB := sortedAssets(b.StorageAssets)
	for i := range sortedA {
		x, y := sortedA[i], sortedB[i]
		if x.AssetID != y.AssetID || x.AssetType != y.AssetType ||
			x.SocPct != y.SocPct || x.PowerKw != y.PowerKw ||
			x.CapacityKwh != y.CapacityKwh || !equalOptional(x.PluggedIn, y.PluggedIn) {
			return false
		}
	}
	return true
}

func sortedAssets(in []domain.StorageAssetReading) []domain.StorageAssetReading {
	out := slices.Clone(in)
	slices.SortFunc(out, func(x, y domain.StorageAssetReading) int {
		switch {
		case x.AssetID < y.AssetID:
			return -1
		case x.AssetID > y.AssetID:
			return 1
		}
		return 0
	})
	return out
}

func equalOptional[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
