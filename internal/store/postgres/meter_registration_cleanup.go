package postgres

import (
	"context"
	"fmt"
	"time"
)

// DeleteStaleMeterRegistrations deletes one batch of old non-approved registrations.
func (s *Store) DeleteStaleMeterRegistrations(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("delete stale meter registrations: limit must be greater than zero")
	}

	const query = `
		WITH stale_registrations AS (
			SELECT id
			FROM iot_data.meter_registrations
			WHERE updated_at < $1
				AND approval_status IN (
					'DEWA_APPROVAL_PENDING',
					'REJECTED',
					'EXPIRED'
				)
			ORDER BY updated_at, id
			LIMIT $2
				FOR UPDATE SKIP LOCKED
		)
		DELETE FROM iot_data.meter_registrations AS registrations
		USING stale_registrations
		WHERE registrations.id = stale_registrations.id
	`

	result, err := s.pool.Exec(ctx, query, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete stale meter registrations: %w", err)
	}
	return result.RowsAffected(), nil
}
