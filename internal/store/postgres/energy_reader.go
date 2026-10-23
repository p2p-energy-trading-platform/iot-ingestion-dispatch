package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// maxReadingSeconds caps how long one reading counts for, so a meter outage
// is not filled in with the last value seen before it.
const maxReadingSeconds = 30

// energyReadingsCTE turns each reading in [$2, $3) of house $1 into per-flow
// power (kW) and how long it lasted (dt_h, hours). Each reading lasts until the
// next one (or the range end), capped at maxReadingSeconds. Flows follow
// solar -> home, then solar -> storage, then solar -> grid; a deficit is
// covered by storage -> home, then grid -> home.
//
// Storage power is summed per (time, meter) once and then joined, rather than
// looked up per reading, so long ranges can use a hash or merge join.
//
// bucketExpr is the expression selected as "bucket"; the summary passes a
// constant.
func energyReadingsCTE(bucketExpr string) string {
	return fmt.Sprintf(`
WITH s AS (
  SELECT sa."time", sa.meter_id,
         SUM(GREATEST(sa.power_kw, 0)) AS charge, SUM(GREATEST(-sa.power_kw, 0)) AS discharge
  FROM iot_data.storage_asset_readings sa
  WHERE sa.house_id = $1 AND sa."time" >= $2 AND sa."time" < $3
  GROUP BY sa."time", sa.meter_id
), r AS (
  SELECT %s AS bucket,
         m.solar_kw AS solar, m.consumption_kw AS cons, m.net_kw AS net,
         COALESCE(s.charge, 0) AS charge, COALESCE(s.discharge, 0) AS discharge,
         LEAST(EXTRACT(EPOCH FROM COALESCE(LEAD(m."time") OVER (ORDER BY m."time"), $3::timestamptz) - m."time"), %d) / 3600.0 AS dt_h
  FROM iot_data.meter_readings m
  LEFT JOIN s ON s."time" = m."time" AND s.meter_id = m.meter_id
  WHERE m.house_id = $1 AND m."time" >= $2 AND m."time" < $3
), f AS (
  SELECT *, LEAST(solar, cons) AS solar_home FROM r
), g AS (
  SELECT *, LEAST(solar - solar_home, charge) AS solar_storage,
            LEAST(discharge, cons - solar_home) AS storage_home
  FROM f
)`, bucketExpr, maxReadingSeconds)
}

const energySums = `
  COALESCE(SUM(solar * dt_h), 0),
  COALESCE(SUM(cons * dt_h), 0),
  COALESCE(SUM(GREATEST(net, 0) * dt_h), 0),
  COALESCE(SUM(GREATEST(-net, 0) * dt_h), 0),
  COALESCE(SUM(charge * dt_h), 0),
  COALESCE(SUM(discharge * dt_h), 0),
  COALESCE(SUM(solar_home * dt_h), 0),
  COALESCE(SUM(solar_storage * dt_h), 0),
  COALESCE(SUM((solar - solar_home - solar_storage) * dt_h), 0),
  COALESCE(SUM((cons - solar_home - storage_home) * dt_h), 0),
  COALESCE(SUM((charge - solar_storage) * dt_h), 0),
  COALESCE(SUM(storage_home * dt_h), 0),
  COALESCE(SUM((discharge - storage_home) * dt_h), 0),
  COALESCE(SUM(dt_h), 0)`

var energySeriesSQL = energyReadingsCTE(`time_bucket($4::double precision * interval '1 second', m."time", $5::text)`) + `
SELECT bucket,` + energySums + `
FROM g
GROUP BY bucket
ORDER BY bucket`

var energySummarySQL = energyReadingsCTE(`NULL::timestamptz`) + `
SELECT` + energySums + `
FROM g`

const peakDemandSQL = `
SELECT consumption_kw, "time"
FROM iot_data.meter_readings
WHERE house_id = $1 AND "time" >= $2 AND "time" < $3
ORDER BY consumption_kw DESC, "time" ASC
LIMIT 1`

func energyScanTargets(t *EnergyTotals, f *EnergyFlows, coveredHours *float64) []any {
	return []any{
		&t.GeneratedKwh, &t.ConsumedKwh, &t.ExportedKwh, &t.ImportedKwh, &t.StorageChargedKwh, &t.StorageDischargedKwh,
		&f.SolarToHomeKwh, &f.SolarToStorageKwh, &f.SolarToGridKwh, &f.GridToHomeKwh, &f.GridToStorageKwh,
		&f.StorageToHomeKwh, &f.StorageToGridKwh,
		coveredHours,
	}
}

// EnergySeries returns per-bucket energy for house houseID over [start, end),
// with buckets of width aligned in the IANA time zone tz. Only buckets that
// contain readings are returned.
func (store *Store) EnergySeries(ctx context.Context, houseID string, start, end time.Time, width time.Duration, tz string) ([]EnergyBucketRow, error) {
	rows, err := store.pool.Query(ctx, energySeriesSQL, houseID, start, end, width.Seconds(), tz)
	if err != nil {
		return nil, fmt.Errorf("postgres: energy series: %w", err)
	}
	defer rows.Close()

	out := []EnergyBucketRow{}
	for rows.Next() {
		var b EnergyBucketRow
		var flows EnergyFlows
		targets := append([]any{&b.Start}, energyScanTargets(&b.Energy, &flows, &b.CoveredHours)...)
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("postgres: energy series scan: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: energy series: %w", err)
	}
	return out, nil
}

// EnergySummary returns energy totals, flows and peak consumption for house
// houseID over [start, end).
func (store *Store) EnergySummary(ctx context.Context, houseID string, start, end time.Time) (EnergySummaryRow, error) {
	var s EnergySummaryRow
	var coveredHours float64
	if err := store.pool.QueryRow(ctx, energySummarySQL, houseID, start, end).
		Scan(energyScanTargets(&s.Totals, &s.Flows, &coveredHours)...); err != nil {
		return EnergySummaryRow{}, fmt.Errorf("postgres: energy summary: %w", err)
	}

	var peakKw float64
	var peakAt time.Time
	err := store.pool.QueryRow(ctx, peakDemandSQL, houseID, start, end).Scan(&peakKw, &peakAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return EnergySummaryRow{}, fmt.Errorf("postgres: peak demand: %w", err)
	default:
		s.PeakConsumptionKw = &peakKw
		s.PeakTime = &peakAt
	}
	return s, nil
}
