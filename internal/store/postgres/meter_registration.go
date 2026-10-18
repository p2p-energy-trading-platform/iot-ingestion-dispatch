
package postgres

import (
	"context"
	"fmt"
)

// MeterRegistration represents a smart meter registration record.
type MeterRegistration struct {
	RegistrationID string
	MeterID        string
	UserReference  string
	ApprovalStatus string
}

// SaveMeterRegistration saves a meter registration in PostgreSQL.
func (s *Store) SaveMeterRegistration(
	ctx context.Context,
	registration MeterRegistration,
) error {
	const query = `
		INSERT INTO iot_data.meter_registrations (
			registration_id,
			meter_id,
			user_reference,
			approval_status
		)
		VALUES ($1, $2, $3, $4)
	`

	_, err := s.pool.Exec(
		ctx,
		query,
		registration.RegistrationID,
		registration.MeterID,
		registration.UserReference,
		registration.ApprovalStatus,
	)
	if err != nil {
		return fmt.Errorf("save meter registration: %w", err)
	}

	return nil
}
