package app

import (
	"context"
	"fmt"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/migrations"
)

// ensureMigrationsCurrent implements step 2 of 05-startup-registry.md's
// startup sequence: verify (never apply) that the database's applied
// goose migration version exactly matches the highest migration embedded
// in this binary. cmd/migrate remains the only thing that ever runs
// migrations - this is a read-only guard against starting the service
// against a database that hasn't been migrated to match it yet.
func (app *App) ensureMigrationsCurrent(context context.Context) error {
	expected, err := migrations.LatestVersion()
	if err != nil {
		return fmt.Errorf("determine expected migration version: %w", err)
	}

	current, err := app.postgres.CurrentMigrationVersion(context)
	if err != nil {
		return fmt.Errorf("read current migration version: %w", err)
	}

	if current != expected {
		return fmt.Errorf(
			"database migration version %d does not match expected version %d - run cmd/migrate before starting this service",
			current, expected,
		)
	}

	app.logger.Info("migrations confirmed current", "version", current)
	return nil
}
