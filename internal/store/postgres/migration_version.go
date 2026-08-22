package postgres

import (
	"context"
	"fmt"
)

// CurrentMigrationVersion returns the highest applied goose migration
// version recorded in goose_db_version.
func (store *Store) CurrentMigrationVersion(context context.Context) (int64, error) {
	var version int64

	err := store.pool.QueryRow(context,
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied = true`,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("postgres: read current migration version: %w", err)
	}

	return version, nil
}
