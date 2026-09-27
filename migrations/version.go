package migrations

import (
	"fmt"
	"strconv"
	"strings"
)

// LatestVersion scans the embedded migration files and returns the
// highest goose version number found (parsed from each file's numeric
// prefix, e.g. 00002_initial_iot_schema.sql -> 2). This is what the
// running service expects the database to already be at - NOT something
// this package applies. Applying migrations remains cmd/migrate's job
// alone, per this service's one-deployment-job migration policy.
func LatestVersion() (int64, error) {
	entries, err := FS.ReadDir(".")
	if err != nil {
		return 0, fmt.Errorf("migrations: read embedded dir: %w", err)
	}

	var latest int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		prefix, _, found := strings.Cut(entry.Name(), "_")
		if !found {
			continue
		}

		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			continue
		}

		if version > latest {
			latest = version
		}
	}

	if latest == 0 {
		return 0, fmt.Errorf("migrations: no valid migration files found in embedded FS")
	}

	return latest, nil
}
