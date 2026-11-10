package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	regdomain "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
)

// SaveMeterRegistration saves a meter registration in PostgreSQL.
func (s *Store) SaveMeterRegistration(
	ctx context.Context,
	registration regdomain.RegistrationRecord,
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
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return fmt.Errorf("%w: meter %q is already registered to another owner", domain.ErrHouseIdentity, registration.MeterID)
		}
		return fmt.Errorf("save meter registration: %w", err)
	}

	return nil
}

func (s *Store) GetByRegistrationID(
	ctx context.Context,
	registrationID string,
) (*regdomain.RegistrationRecord, error) {
	const query = `
		SELECT id, user_id, meter_id, registration_id, role, approval_status
		FROM iot_data.meter_registrations
		WHERE registration_id = $1
		ORDER BY created_at DESC
        LIMIT 1
	`

	var reg regdomain.RegistrationRecord
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

func (s *Store) GetRegistrationByMeterID(
	ctx context.Context,
	meterID string,
) (*regdomain.RegistrationRecord, error) {
	const query = `
        SELECT id, user_id, registration_id, meter_id, role, approval_status
        FROM iot_data.meter_registrations
        WHERE meter_id = $1
		ORDER BY created_at DESC
        LIMIT 1
    `

	var reg regdomain.RegistrationRecord
	err := s.pool.QueryRow(ctx, query, meterID).Scan(
		&reg.ID,
		&reg.UserID,
		&reg.RegistrationID,
		&reg.MeterID,
		&reg.Role,
		&reg.ApprovalStatus,
	)

	if err != nil {
		return nil, fmt.Errorf("get registration by meter ID %q: %w", meterID, err)
	}

	return &reg, nil
}

func (s *Store) GetRegistrationByUserID(
	ctx context.Context,
	userID string,
) (*regdomain.RegistrationRecord, error) {
	const query = `
        SELECT id, user_id, registration_id, meter_id, role, approval_status
        FROM iot_data.meter_registrations
        WHERE user_id = $1
		ORDER BY created_at DESC
        LIMIT 1
    `

	var reg regdomain.RegistrationRecord
	err := s.pool.QueryRow(ctx, query, userID).Scan(
		&reg.ID,
		&reg.UserID,
		&reg.RegistrationID,
		&reg.MeterID,
		&reg.Role,
		&reg.ApprovalStatus,
	)

	if err != nil {
		return nil, fmt.Errorf("get registration by user ID %q: %w", userID, err)
	}

	return &reg, nil
}
