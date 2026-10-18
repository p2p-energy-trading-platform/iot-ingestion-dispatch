package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

// HouseRow is one registered house, enough to rebuild its Redis liveness and
// grid-membership entries.
type HouseRow struct {
	HouseID         string
	GridID          string
	LastHeartbeatAt *time.Time
}

const housesPageSQL = `
SELECT house_id, grid_id, last_heartbeat_at
FROM iot_data.houses
WHERE house_id > $1
ORDER BY house_id
LIMIT $2`

// HousesPage returns up to limit registered houses with house_id greater than
// after, in house_id order (keyset pagination, so the scan is bounded per
// call and stable while the table changes).
func (store *Store) HousesPage(ctx context.Context, after string, limit int) ([]HouseRow, error) {
	rows, err := store.pool.Query(ctx, housesPageSQL, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: houses page: %w", err)
	}
	defer rows.Close()

	var out []HouseRow
	for rows.Next() {
		var h HouseRow
		if err := rows.Scan(&h.HouseID, &h.GridID, &h.LastHeartbeatAt); err != nil {
			return nil, fmt.Errorf("postgres: houses page scan: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: houses page rows: %w", err)
	}
	return out, nil
}

// The latest reading per house within the lookback window. DISTINCT ON picks
// the newest (time, seq) per house, the same ordering the Redis compare-and-set
// uses.
const latestReadingsPageSQL = `
SELECT DISTINCT ON (house_id)
       "time", meter_id, house_id, grid_id, device_class, schema_version, seq,
       solar_kw, consumption_kw, net_kw
FROM iot_data.meter_readings
WHERE "time" >= $1 AND house_id > $2
ORDER BY house_id, "time" DESC, seq DESC
LIMIT $3`

const latestStorageSQL = `
SELECT asset_id, asset_type, soc_pct, power_kw, capacity_kwh, plugged_in
FROM iot_data.storage_asset_readings
WHERE "time" = $1 AND meter_id = $2
ORDER BY asset_id`

// LatestReadingsPage returns the newest reading of up to limit houses whose
// house_id sorts after the given cursor, looking back no further than since.
// Houses without a reading in the window are simply absent: the rebuild is a
// best-effort projection of recent state, never a full history scan.
func (store *Store) LatestReadingsPage(ctx context.Context, since time.Time, after string, limit int) ([]domain.MeterReading, error) {
	rows, err := store.pool.Query(ctx, latestReadingsPageSQL, since, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: latest readings page: %w", err)
	}
	var out []domain.MeterReading
	for rows.Next() {
		var r domain.MeterReading
		var class string
		if err := rows.Scan(&r.EventTime, &r.MeterID, &r.HouseID, &r.GridID, &class, &r.SchemaVersion,
			&r.Seq, &r.SolarKw, &r.ConsumptionKw, &r.NetKw); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: latest readings scan: %w", err)
		}
		r.DeviceClass = domain.DeviceClass(class)
		r.EventTime = r.EventTime.UTC()
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: latest readings rows: %w", err)
	}

	for i := range out {
		assets, err := store.pool.Query(ctx, latestStorageSQL, out[i].EventTime, out[i].MeterID)
		if err != nil {
			return nil, fmt.Errorf("postgres: latest storage query: %w", err)
		}
		for assets.Next() {
			var a domain.StorageAssetReading
			var t string
			if err := assets.Scan(&a.AssetID, &t, &a.SocPct, &a.PowerKw, &a.CapacityKwh, &a.PluggedIn); err != nil {
				assets.Close()
				return nil, fmt.Errorf("postgres: latest storage scan: %w", err)
			}
			a.AssetType = domain.AssetType(t)
			out[i].StorageAssets = append(out[i].StorageAssets, a)
		}
		assets.Close()
		if err := assets.Err(); err != nil {
			return nil, fmt.Errorf("postgres: latest storage rows: %w", err)
		}
	}
	return out, nil
}
