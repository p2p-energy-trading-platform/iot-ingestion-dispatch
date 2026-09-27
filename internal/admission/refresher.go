package admission

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type GridLoader interface {
	LoadGrids(ctx context.Context) ([]Grid, error)
}

type RefresherConfig struct {
	Interval time.Duration
}

func (c RefresherConfig) withDefaults() RefresherConfig {
	if c.Interval <= 0 {
		c.Interval = 120 * time.Second
	}
	return c
}

type Refresher struct {
	registry *Registry
	loader   GridLoader
	cfg      RefresherConfig
	logger   *slog.Logger
}

func NewRefresher(registry *Registry, loader GridLoader, cfg RefresherConfig, logger *slog.Logger) *Refresher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Refresher{
		registry: registry,
		loader:   loader,
		cfg:      cfg.withDefaults(),
		logger:   logger,
	}
}

func (r *Refresher) Bootstrap(ctx context.Context) error {
	grids, err := r.loader.LoadGrids(ctx)
	if err != nil {
		return fmt.Errorf("admission: initial grid load failed: %w", err)
	}

	byID, err := toGridMap(grids)
	if err != nil {
		return fmt.Errorf("admission: initial grid data invalid: %w", err)
	}

	if err := r.registry.Publish(byID); err != nil {
		return fmt.Errorf("admission: initial grid snapshot rejected: %w", err)
	}

	r.logger.Info("grid registry bootstrapped", slog.Int("grid_count", len(byID)))
	return nil
}

func (r *Refresher) Start(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refreshOnce(ctx)
		}
	}
}

func (r *Refresher) refreshOnce(ctx context.Context) {
	started := time.Now()

	grids, err := r.loader.LoadGrids(ctx)
	if err != nil {
		r.logger.Error("grid registry refresh failed, keeping last known-good snapshot",
			slog.String("error", err.Error()))
		return
	}

	byID, err := toGridMap(grids)
	if err != nil {
		r.logger.Error("grid registry refresh produced invalid data, keeping last known-good snapshot",
			slog.String("error", err.Error()))
		return
	}

	before := r.registry.CurrentSnapshot()
	added, removed := diffGridIDs(before, byID)

	if err := r.registry.Publish(byID); err != nil {
		r.logger.Error("grid registry refresh rejected, keeping last known-good snapshot",
			slog.String("error", err.Error()))
		return
	}

	r.logger.Info("grid registry refreshed",
		slog.Int("grid_count", len(byID)),
		slog.Duration("refresh_duration", time.Since(started)),
		slog.Any("added_grid_ids", added),
		slog.Any("removed_grid_ids", removed),
		slog.Time("grid_registry_last_successful_refresh", time.Now()),
	)
}

func toGridMap(grids []Grid) (map[string]Grid, error) {
	if len(grids) == 0 {
		return nil, ErrEmptySnapshot
	}
	byID := make(map[string]Grid, len(grids))
	for _, g := range grids {
		if g.GridID == "" {
			return nil, fmt.Errorf("admission: grid row with empty grid_id")
		}
		byID[g.GridID] = g
	}
	return byID, nil
}

func diffGridIDs(before *Snapshot, after map[string]Grid) (added, removed []string) {
	if before == nil {
		for id := range after {
			added = append(added, id)
		}
		return added, nil
	}
	for id := range after {
		if _, ok := before.Lookup(id); !ok {
			added = append(added, id)
		}
	}
	for id := range before.grids {
		if _, ok := after[id]; !ok {
			removed = append(removed, id)
		}
	}
	return added, removed
}
