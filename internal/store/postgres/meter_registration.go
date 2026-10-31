package postgres

import (
	"context"
	"fmt"
)

// MeterRegistration represents a smart meter registration record.
type MeterRegistration struct {
	ID             string
	UserID         string
	RegistrationID string
	MeterID        string
	Role           string
	ApprovalStatus string
}

// SaveMeterRegistration saves a meter registration in PostgreSQL.
func (s *Store) SaveMeterRegistration(

	ctx context.Context,
	registration MeterRegistration,

) error {

	const query = `
		INSERT INTO iot_data.meter_registrations (
			user_id,
			meter_id,
			registration_id,
			role,
			approval_status
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (registration_id) DO UPDATE SET
			approval_status = EXCLUDED.approval_status,
			updated_at = NOW()
	`

	role := registration.Role
	if role == "" {
		role = "PRIMARY_OWNER"
	}

	status := registration.ApprovalStatus
	if status == "" {
		status = "DEWA_APPROVAL_PENDING"
	}

	_, err := s.pool.Exec(
		ctx,
		query,
		registration.UserID,
		registration.MeterID,
		registration.RegistrationID,
		role,
		status,
	)
	if err != nil {
		return fmt.Errorf("save meter registration: %w", err)
	}

	return nil

}

func (s *Store) GetRegistrationByID(

	ctx context.Context,
	registrationID string,

) (*MeterRegistration, error) {

	const query = `
		SELECT id, user_id, meter_id, registration_id, role, approval_status
		FROM iot_data.meter_registrations
		WHERE registration_id = $1
	`

	var reg MeterRegistration
	err := s.pool.QueryRow(ctx, query, registrationID).Scan(
		&reg.ID,
		&reg.UserID,
		&reg.MeterID,
		&reg.RegistrationID,
		&reg.Role,
		&reg.ApprovalStatus,
	)

	if err != nil {
		return nil, fmt.Errorf("get registration by ID %q: %w", registrationID, err)
	}

	return &reg, nil

}
