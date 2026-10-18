package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

// HouseRegistration is one house as provisioned by an administrator or, later,
// by the registration flow. It is the only way a house enters iot_data.houses:
// heartbeats never create or move houses (06-heartbeat-processing.md).
type HouseRegistration struct {
	HouseID      string
	GridID       string
	MeterID      string
	DeviceClass  domain.DeviceClass
	RatedSolarKw *float64
}

// RegistrationOutcome says what RegisterHouse did.
type RegistrationOutcome string

const (
	HouseCreated   RegistrationOutcome = "created"
	HouseUnchanged RegistrationOutcome = "unchanged"
)

const insertHouseSQL = `
INSERT INTO iot_data.houses (house_id, grid_id, meter_id, device_class, rated_solar_kw)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (house_id) DO NOTHING`

const selectRegisteredHouseSQL = `
SELECT grid_id, meter_id, device_class
FROM iot_data.houses
WHERE house_id = $1`

const pgForeignKeyViolation = "23503"

// RegisterHouse provisions a house. It is idempotent for an identical
// registration and never changes an existing one: a house that is already
// registered with a different grid, meter or device class is reported as
// domain.ErrHouseIdentity, because re-binding a meter or moving a house between
// grids is an ownership decision, not something provisioning may do silently.
// An unknown grid is domain.ErrGridNotProvisioned, and a meter already bound to
// another house is domain.ErrHouseIdentity.
func (store *Store) RegisterHouse(ctx context.Context, h HouseRegistration) (RegistrationOutcome, error) {
	var outcome RegistrationOutcome
	err := pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, insertHouseSQL, h.HouseID, h.GridID, h.MeterID, string(h.DeviceClass), h.RatedSolarKw)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				switch pgErr.Code {
				case pgForeignKeyViolation:
					return fmt.Errorf("%w: grid %q", domain.ErrGridNotProvisioned, h.GridID)
				case pgUniqueViolation:
					return fmt.Errorf("%w: meter %q is already registered to another house", domain.ErrHouseIdentity, h.MeterID)
				}
			}
			return fmt.Errorf("register house %q: %w", h.HouseID, err)
		}
		if tag.RowsAffected() == 1 {
			outcome = HouseCreated
			return nil
		}

		var gridID, deviceClass string
		var meterID *string
		if err := tx.QueryRow(ctx, selectRegisteredHouseSQL, h.HouseID).Scan(&gridID, &meterID, &deviceClass); err != nil {
			return fmt.Errorf("load registration of house %q: %w", h.HouseID, err)
		}
		if gridID != h.GridID || meterID == nil || *meterID != h.MeterID || deviceClass != string(h.DeviceClass) {
			existingMeter := "<none>"
			if meterID != nil {
				existingMeter = *meterID
			}
			return fmt.Errorf("%w: house %q is registered as grid %q meter %q class %q; refusing to change it to grid %q meter %q class %q",
				domain.ErrHouseIdentity, h.HouseID, gridID, existingMeter, deviceClass, h.GridID, h.MeterID, h.DeviceClass)
		}
		outcome = HouseUnchanged
		return nil
	})
	return outcome, err
}
