package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const pgForeignKeyViolation = "23503"

// status is written once, on discovery. Liveness is derived from
// last_heartbeat_at, never from status.
const insertHouseSQL = `
INSERT INTO iot_data.houses
    (house_id, grid_id, device_class, rated_solar_kw, status, first_seen_at, last_heartbeat_at)
VALUES ($1, $2, $3, $4, $5, $6, $6)
ON CONFLICT (house_id) DO NOTHING`

const selectHouseForUpdateSQL = `
SELECT grid_id, device_class
FROM iot_data.houses
WHERE house_id = $1
FOR UPDATE`

const updateHouseSQL = `
UPDATE iot_data.houses
SET rated_solar_kw    = $2,
    last_heartbeat_at = GREATEST(last_heartbeat_at, $3)
WHERE house_id = $1`

const selectAssetsForUpdateSQL = `
SELECT asset_id, house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw, v2g_capable
FROM iot_data.flexible_assets
WHERE asset_id = ANY($1)
FOR UPDATE`

const insertAssetSQL = `
INSERT INTO iot_data.flexible_assets
    (asset_id, house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw,
     v2g_capable, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`

const updateAssetSQL = `
UPDATE iot_data.flexible_assets
SET capacity_kwh     = $2,
    max_charge_kw    = $3,
    max_discharge_kw = $4,
    v2g_capable      = $5,
    last_seen_at     = GREATEST(last_seen_at, $6)
WHERE asset_id = $1`

// ApplyHeartbeat runs the Postgres half of the heartbeat workflow in one
// transaction: validate or discover the house and refresh its liveness, then
// validate asset ownership and write only assets that are new or changed.
//
// Identity conflicts (house grid/device class, asset owner/type) return
// domain.ErrIntegrityConflict and write nothing. Other errors are
// infrastructure failures and safe to retry.
func (store *Store) ApplyHeartbeat(ctx context.Context, hb domain.Heartbeat, receivedAt time.Time) error {
	receivedAt = receivedAt.UTC().Truncate(time.Microsecond)

	return pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		if err := applyHouse(ctx, tx, hb, receivedAt); err != nil {
			return err
		}
		return applyAssets(ctx, tx, hb, receivedAt)
	})
}

func applyHouse(ctx context.Context, tx pgx.Tx, hb domain.Heartbeat, receivedAt time.Time) error {
	tag, err := tx.Exec(ctx, insertHouseSQL,
		hb.HouseID, hb.GridID, string(hb.DeviceClass), hb.RatedSolarKw, hb.Status, receivedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
			return fmt.Errorf("grid %q is not provisioned in iot_data.grids: %w", hb.GridID, err)
		}
		return fmt.Errorf("insert house %q: %w", hb.HouseID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil // newly discovered; liveness already set by the insert
	}

	var gridID, deviceClass string
	if err := tx.QueryRow(ctx, selectHouseForUpdateSQL, hb.HouseID).Scan(&gridID, &deviceClass); err != nil {
		return fmt.Errorf("load house %q: %w", hb.HouseID, err)
	}
	if gridID != hb.GridID || deviceClass != string(hb.DeviceClass) {
		return fmt.Errorf("%w: house %q is registered as grid %q class %q, heartbeat says grid %q class %q",
			domain.ErrIntegrityConflict, hb.HouseID, gridID, deviceClass, hb.GridID, hb.DeviceClass)
	}

	if _, err := tx.Exec(ctx, updateHouseSQL, hb.HouseID, hb.RatedSolarKw, receivedAt); err != nil {
		return fmt.Errorf("update house %q: %w", hb.HouseID, err)
	}
	return nil
}

type storedAsset struct {
	houseID string
	asset   domain.FlexibleAsset
}

func applyAssets(ctx context.Context, tx pgx.Tx, hb domain.Heartbeat, receivedAt time.Time) error {
	if len(hb.FlexibleAssets) == 0 {
		return nil
	}

	ids := make([]string, len(hb.FlexibleAssets))
	for i, a := range hb.FlexibleAssets {
		ids[i] = a.AssetID
	}

	existing, err := loadAssets(ctx, tx, ids)
	if err != nil {
		return err
	}

	for _, incoming := range hb.FlexibleAssets {
		stored, found := existing[incoming.AssetID]
		if !found {
			if err := insertAsset(ctx, tx, hb.HouseID, incoming, receivedAt); err != nil {
				return err
			}
			continue
		}

		switch compareAsset(stored.houseID, stored.asset, incoming, hb.HouseID) {
		case assetConflict:
			return fmt.Errorf("%w: asset %q is registered to house %q as %q, heartbeat from house %q says %q",
				domain.ErrIntegrityConflict, incoming.AssetID, stored.houseID, stored.asset.AssetType,
				hb.HouseID, incoming.AssetType)
		case assetChanged:
			if _, err := tx.Exec(ctx, updateAssetSQL, incoming.AssetID,
				incoming.CapacityKwh, incoming.MaxChargeKw, incoming.MaxDischargeKw,
				incoming.V2GCapable, receivedAt); err != nil {
				return fmt.Errorf("update asset %q: %w", incoming.AssetID, err)
			}
		case assetUnchanged:
			// No write: nothing about the asset's capabilities changed.
		}
	}
	return nil
}

func loadAssets(ctx context.Context, tx pgx.Tx, ids []string) (map[string]storedAsset, error) {
	rows, err := tx.Query(ctx, selectAssetsForUpdateSQL, ids)
	if err != nil {
		return nil, fmt.Errorf("load assets: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]storedAsset, len(ids))
	for rows.Next() {
		var s storedAsset
		var assetType string
		if err := rows.Scan(&s.asset.AssetID, &s.houseID, &assetType,
			&s.asset.CapacityKwh, &s.asset.MaxChargeKw, &s.asset.MaxDischargeKw,
			&s.asset.V2GCapable); err != nil {
			return nil, fmt.Errorf("scan asset: %w", err)
		}
		s.asset.AssetType = domain.AssetType(assetType)
		existing[s.asset.AssetID] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load assets: %w", err)
	}
	return existing, nil
}

func insertAsset(ctx context.Context, tx pgx.Tx, houseID string, a domain.FlexibleAsset, receivedAt time.Time) error {
	if _, err := tx.Exec(ctx, insertAssetSQL, a.AssetID, houseID, string(a.AssetType),
		a.CapacityKwh, a.MaxChargeKw, a.MaxDischargeKw, a.V2GCapable, receivedAt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			// Another house registered this asset_id after we read.
			return fmt.Errorf("%w: asset %q was registered concurrently by another house",
				domain.ErrIntegrityConflict, a.AssetID)
		}
		return fmt.Errorf("insert asset %q: %w", a.AssetID, err)
	}
	return nil
}

type assetAction int

const (
	assetUnchanged assetAction = iota
	assetChanged
	assetConflict
)

// compareAsset decides what a heartbeat entry means for an already
// registered asset. Owner and type are identity (conflict if different);
// capacity, power limits and v2g capability are capabilities (changed).
func compareAsset(storedHouseID string, stored, incoming domain.FlexibleAsset, incomingHouseID string) assetAction {
	if storedHouseID != incomingHouseID || stored.AssetType != incoming.AssetType {
		return assetConflict
	}
	if stored.CapacityKwh != incoming.CapacityKwh ||
		stored.MaxChargeKw != incoming.MaxChargeKw ||
		stored.MaxDischargeKw != incoming.MaxDischargeKw ||
		!equalOptional(stored.V2GCapable, incoming.V2GCapable) {
		return assetChanged
	}
	return assetUnchanged
}
