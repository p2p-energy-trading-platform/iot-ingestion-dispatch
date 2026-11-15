package postgres

import (
	"context"
	"fmt"
)

// UpdateMeterRegistrationStatus updates the approval status of a registration.
func (s *Store) UpdateMeterRegistrationStatus(
	ctx context.Context,
	registrationID string,
	status string,
) error {
	const query = `
		UPDATE iot_data.meter_registrations
		SET approval_status = $2::iot_data.meter_approval_status,
		    updated_at = NOW()
		WHERE registration_id = $1
	`

	result, err := s.pool.Exec(ctx, query, registrationID, status)
	if err != nil {
		return fmt.Errorf("update meter registration status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("meter registration %q not found", registrationID)
	}

	return nil
}

// UpdateMeterRegistrationRequestID stores the DEWA request ID used to resume polling.
func (s *Store) UpdateMeterRegistrationRequestID(
	ctx context.Context,
	registrationID string,
	requestID string,
) error {
	const query = `
		UPDATE iot_data.meter_registrations
		SET request_id = $2,
		    updated_at = NOW()
		WHERE registration_id = $1
	`

	result, err := s.pool.Exec(ctx, query, registrationID, requestID)
	if err != nil {
		return fmt.Errorf("update DEWA request ID: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("meter registration %q not found", registrationID)
	}

	return nil
}
