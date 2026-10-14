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
    (house_id, grid_id, device_class, rated_solar_kw, status, first_seen_at, last_heartbeat_at, meter_id)
VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
ON CONFLICT (house_id) DO NOTHING`

const selectHouseForUpdateSQL = `
SELECT grid_id, device_class, COALESCE(meter_id, '')
FROM iot_data.houses
WHERE house_id = $1
FOR UPDATE`

const updateHouseSQL = `
UPDATE iot_data.houses
SET rated_solar_kw    = $2,
    last_heartbeat_at = GREATEST(last_heartbeat_at, $3),
    meter_id          = COALESCE(meter_id, $4)
WHERE house_id = $1`

// Authoritative registry. Heartbeats may read it and refresh capability
// fields, but never insert rows or change house_id or asset_type.
const selectRegisteredAssetsSQL = `
SELECT asset_id, house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw, v2g_capable
FROM iot_data.flexible_assets
WHERE asset_id = ANY($1)
FOR UPDATE`

const updateRegisteredAssetSQL = `
UPDATE iot_data.flexible_assets
SET capacity_kwh     = $2,
    max_charge_kw    = $3,
    max_discharge_kw = $4,
    v2g_capable      = $5,
    last_seen_at     = GREATEST(last_seen_at, $6)
WHERE asset_id = $1`

// Observations. This is the only place a heartbeat may record an asset that
// has no registration.
const selectDiscoveredAssetsSQL = `
SELECT asset_id, observed_house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw, v2g_capable
FROM iot_data.discovered_assets
WHERE asset_id = ANY($1)
FOR UPDATE`

const insertDiscoveredAssetSQL = `
INSERT INTO iot_data.discovered_assets
    (asset_id, observed_house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw,
     v2g_capable, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
ON CONFLICT (asset_id) DO NOTHING`

const updateDiscoveredAssetSQL = `
UPDATE iot_data.discovered_assets
SET capacity_kwh     = $2,
    max_charge_kw    = $3,
    max_discharge_kw = $4,
    v2g_capable      = $5,
    last_seen_at     = GREATEST(last_seen_at, $6)
WHERE asset_id = $1`

// ApplyHeartbeat runs the Postgres half of the heartbeat workflow in one
// transaction: validate or discover the house and refresh its liveness, then
// handle each reported asset without ever granting it anything.
//
//   - registered asset: validated against its house; capability fields are
//     updated only when they changed
//   - previously observed asset: same rules, against the observation
//   - unknown asset: recorded in discovered_assets only. A heartbeat never
//     inserts into flexible_assets, so it cannot grant ownership, trading,
//     dispatch or settlement eligibility, or user consent.
//
// An asset tied to a different house, or reported with a different type, is a
// permanent identity conflict: domain.ErrIntegrityConflict, and the whole
// transaction rolls back. Other errors are infrastructure failures and safe
// to retry.
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
		hb.HouseID, hb.GridID, string(hb.DeviceClass), hb.RatedSolarKw, hb.Status, receivedAt, hb.MeterID)
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

	var gridID, deviceClass, meterID string
	if err := tx.QueryRow(ctx, selectHouseForUpdateSQL, hb.HouseID).Scan(&gridID, &deviceClass, &meterID); err != nil {
		return fmt.Errorf("load house %q: %w", hb.HouseID, err)
	}
	if gridID != hb.GridID || deviceClass != string(hb.DeviceClass) || (meterID != "" && meterID != hb.MeterID) {
		return fmt.Errorf("%w: house %q is registered as grid %q class %q meter %q, heartbeat says grid %q class %q meter %q",
			domain.ErrIntegrityConflict, hb.HouseID, gridID, deviceClass, meterID, hb.GridID, hb.DeviceClass, hb.MeterID)
	}

	if _, err := tx.Exec(ctx, updateHouseSQL, hb.HouseID, hb.RatedSolarKw, receivedAt, hb.MeterID); err != nil {
		return fmt.Errorf("update house %q: %w", hb.HouseID, err)
	}
	return nil
}

type storedAsset struct {
	houseID string
	asset   domain.FlexibleAsset
}

// assetSource says where a known asset lives, so the same validation and
// capability-refresh logic serves both the registry and the observations.
type assetSource struct {
	relation  string // used in conflict messages: "<asset> is <relation> house X"
	updateSQL string
}

var (
	registeredSource = assetSource{relation: "registered to", updateSQL: updateRegisteredAssetSQL}
	discoveredSource = assetSource{relation: "observed under", updateSQL: updateDiscoveredAssetSQL}
)

func applyAssets(ctx context.Context, tx pgx.Tx, hb domain.Heartbeat, receivedAt time.Time) error {
	if len(hb.FlexibleAssets) == 0 {
		return nil
	}

	ids := make([]string, len(hb.FlexibleAssets))
	for i, a := range hb.FlexibleAssets {
		ids[i] = a.AssetID
	}

	registered, err := loadAssets(ctx, tx, selectRegisteredAssetsSQL, ids)
	if err != nil {
		return err
	}

	// Only assets without a registration can be discovery observations.
	var unknownIDs []string
	for _, a := range hb.FlexibleAssets {
		if _, ok := registered[a.AssetID]; !ok {
			unknownIDs = append(unknownIDs, a.AssetID)
		}
	}

	discovered := map[string]storedAsset{}
	if len(unknownIDs) > 0 {
		discovered, err = loadAssets(ctx, tx, selectDiscoveredAssetsSQL, unknownIDs)
		if err != nil {
			return err
		}
	}

	for _, incoming := range hb.FlexibleAssets {
		if stored, ok := registered[incoming.AssetID]; ok {
			if err := applyKnownAsset(ctx, tx, registeredSource, hb.HouseID, stored, incoming, receivedAt); err != nil {
				return err
			}
			continue
		}
		if stored, ok := discovered[incoming.AssetID]; ok {
			if err := applyKnownAsset(ctx, tx, discoveredSource, hb.HouseID, stored, incoming, receivedAt); err != nil {
				return err
			}
			continue
		}
		if err := observeNewAsset(ctx, tx, hb.HouseID, incoming, receivedAt); err != nil {
			return err
		}
	}
	return nil
}

func loadAssets(ctx context.Context, tx pgx.Tx, query string, ids []string) (map[string]storedAsset, error) {
	rows, err := tx.Query(ctx, query, ids)
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

// applyKnownAsset validates a heartbeat entry against a row we already have
// (registered or observed) and refreshes capability fields only when they
// changed. Owner and type are never updated here.
func applyKnownAsset(
	ctx context.Context,
	tx pgx.Tx,
	src assetSource,
	houseID string,
	stored storedAsset,
	incoming domain.FlexibleAsset,
	receivedAt time.Time,
) error {
	switch compareAsset(stored.houseID, stored.asset, incoming, houseID) {
	case assetConflict:
		return fmt.Errorf("%w: asset %q is %s house %q as %q, heartbeat from house %q reports %q",
			domain.ErrIntegrityConflict, incoming.AssetID, src.relation, stored.houseID,
			stored.asset.AssetType, houseID, incoming.AssetType)
	case assetChanged:
		if _, err := tx.Exec(ctx, src.updateSQL, incoming.AssetID,
			incoming.CapacityKwh, incoming.MaxChargeKw, incoming.MaxDischargeKw,
			incoming.V2GCapable, receivedAt); err != nil {
			return fmt.Errorf("update asset %q: %w", incoming.AssetID, err)
		}
	case assetUnchanged:
		// No write: nothing about the asset's capabilities changed.
	}
	return nil
}

// observeNewAsset records an asset nobody has registered or observed yet.
// It only ever writes discovered_assets.
func observeNewAsset(ctx context.Context, tx pgx.Tx, houseID string, a domain.FlexibleAsset, receivedAt time.Time) error {
	tag, err := tx.Exec(ctx, insertDiscoveredAssetSQL, a.AssetID, houseID, string(a.AssetType),
		a.CapacityKwh, a.MaxChargeKw, a.MaxDischargeKw, a.V2GCapable, receivedAt)
	if err != nil {
		return fmt.Errorf("record discovered asset %q: %w", a.AssetID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// Another transaction recorded this asset between our read and insert.
	// Validate against what it wrote instead of failing.
	existing, err := loadAssets(ctx, tx, selectDiscoveredAssetsSQL, []string{a.AssetID})
	if err != nil {
		return err
	}
	stored, ok := existing[a.AssetID]
	if !ok {
		return fmt.Errorf("discovered asset %q disappeared during heartbeat", a.AssetID)
	}
	return applyKnownAsset(ctx, tx, discoveredSource, houseID, stored, a, receivedAt)
}

type assetAction int

const (
	assetUnchanged assetAction = iota
	assetChanged
	assetConflict
)

// compareAsset decides what a heartbeat entry means for an asset we already
// know. Owner and type are identity (conflict if different); capacity, power
// limits and v2g capability are capabilities (changed).
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
