package postgres

import (
	"context"
	"fmt"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
)

// LoadGrids implements admission.GridLoader by reading every provisioned
// grid from iot_data.grids. Column names (lat, lon) match
// 05-startup-registry.md.
func (store *Store) LoadGrids(context context.Context) ([]admission.Grid, error) {
	rows, err := store.pool.Query(context, `SELECT grid_id, lat, lon FROM iot_data.grids`)
	if err != nil {
		return nil, fmt.Errorf("postgres: load grids query: %w", err)
	}
	defer rows.Close()

	var grids []admission.Grid
	for rows.Next() {
		var g admission.Grid
		if err := rows.Scan(&g.GridID, &g.Lat, &g.Lon); err != nil {
			return nil, fmt.Errorf("postgres: load grids scan: %w", err)
		}
		grids = append(grids, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: load grids rows: %w", err)
	}

	return grids, nil
}
