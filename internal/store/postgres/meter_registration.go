package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	regdomain "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
)

// oneActiveMeterPerUserIndex keeps one pending or approved primary-owner
// registration per account (migration 00013).
const oneActiveMeterPerUserIndex = "unique_active_primary_owner_per_user"

// activeRegistrationStatuses are the statuses that count as the account's meter.
const activeRegistrationStatuses = `('DEWA_APPROVAL_PENDING', 'APPROVED')`

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
			if pgErr.ConstraintName == oneActiveMeterPerUserIndex {
				return fmt.Errorf("account already has a smart meter: %w", domain.ErrMeterAlreadyRegistered)
			}
			return fmt.Errorf("meter %q is already registered to another owner: %w", registration.MeterID, domain.ErrMeterAlreadyRegistered)
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
		-- The account's active meter first, then the newest of the rest.
		ORDER BY (role = 'PRIMARY_OWNER' AND approval_status IN ` + activeRegistrationStatuses + `) DESC, created_at DESC
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

// GetActiveRegistrationByUserID returns the account's pending or approved
// primary-owner registration, or nil when it has none.
func (s *Store) GetActiveRegistrationByUserID(
	ctx context.Context,
	userID string,
) (*regdomain.RegistrationRecord, error) {
	query := `
		SELECT id, user_id, registration_id, meter_id, role, approval_status
		FROM iot_data.meter_registrations
		WHERE user_id = $1
		  AND role = 'PRIMARY_OWNER'
		  AND approval_status IN ` + activeRegistrationStatuses + `
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
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active registration by user ID %q: %w", userID, err)
	}

	return &reg, nil
}
