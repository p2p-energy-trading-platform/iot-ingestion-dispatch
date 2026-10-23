package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const houseInfoColumns = `
h.house_id, h.grid_id, COALESCE(g.display_name, ''), COALESCE(h.meter_id, ''),
h.device_class, h.rated_solar_kw, o.user_reference`

// houseSource joins each house to its grid and to its owner: the user of the
// latest APPROVED DEWA meter registration for the house's meter. Pending,
// rejected and timed-out claims grant no ownership.
const houseSource = `
FROM iot_data.houses h
LEFT JOIN iot_data.grids g ON g.grid_id = h.grid_id
LEFT JOIN LATERAL (
  SELECT r.user_reference
  FROM iot_data.meter_registrations r
  WHERE r.meter_id = h.meter_id AND r.approval_status = 'APPROVED'
  ORDER BY r.updated_at DESC, r.registration_id DESC
  LIMIT 1
) o ON true`

const listHousesSQL = `
SELECT` + houseInfoColumns + houseSource + `
WHERE ($1 = '' OR o.user_reference = $1)
  AND ($2 = '' OR h.grid_id = $2)
  AND h.house_id > $3
ORDER BY h.house_id
LIMIT $4`

const getHouseSQL = `
SELECT` + houseInfoColumns + houseSource + `
WHERE h.house_id = $1`

const houseAssetsSQL = `
SELECT asset_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw, v2g_capable
FROM iot_data.flexible_assets
WHERE house_id = $1
ORDER BY asset_id`

func scanHouseInfo(row pgx.Row) (HouseInfoRow, error) {
	var h HouseInfoRow
	err := row.Scan(&h.HouseID, &h.GridID, &h.GridDisplayName, &h.MeterID, &h.DeviceClass, &h.RatedSolarKw, &h.OwnerUserID)
	return h, err
}

// ListHouses returns registered houses matching the filter in house_id order.
func (store *Store) ListHouses(ctx context.Context, filter HouseFilter) ([]HouseInfoRow, error) {
	rows, err := store.pool.Query(ctx, listHousesSQL, filter.OwnerUserID, filter.GridID, filter.AfterHouseID, filter.Limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list houses: %w", err)
	}
	defer rows.Close()

	out := []HouseInfoRow{}
	for rows.Next() {
		h, err := scanHouseInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: list houses scan: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list houses: %w", err)
	}
	return out, nil
}

// GetHouse returns one registered house and its registered flexible assets.
// Assets that were only observed in heartbeats (discovered_assets) are not
// included. An unknown house is domain.ErrNotFound.
func (store *Store) GetHouse(ctx context.Context, houseID string) (HouseInfoRow, []FlexibleAssetRow, error) {
	h, err := scanHouseInfo(store.pool.QueryRow(ctx, getHouseSQL, houseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return HouseInfoRow{}, nil, fmt.Errorf("house %q: %w", houseID, domain.ErrNotFound)
	}
	if err != nil {
		return HouseInfoRow{}, nil, fmt.Errorf("postgres: get house: %w", err)
	}

	rows, err := store.pool.Query(ctx, houseAssetsSQL, houseID)
	if err != nil {
		return HouseInfoRow{}, nil, fmt.Errorf("postgres: house assets: %w", err)
	}
	defer rows.Close()

	assets := []FlexibleAssetRow{}
	for rows.Next() {
		var a FlexibleAssetRow
		if err := rows.Scan(&a.AssetID, &a.AssetType, &a.CapacityKwh, &a.MaxChargeKw, &a.MaxDischargeKw, &a.V2GCapable); err != nil {
			return HouseInfoRow{}, nil, fmt.Errorf("postgres: house assets scan: %w", err)
		}
		assets = append(assets, a)
	}
	if err := rows.Err(); err != nil {
		return HouseInfoRow{}, nil, fmt.Errorf("postgres: house assets: %w", err)
	}
	return h, assets, nil
}
