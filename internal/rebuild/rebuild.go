// Package rebuild repopulates the Redis projection from TimescaleDB after
// Redis loss (02-redis-hot-storage.md "Cache role"). It is bounded: houses are
// read in fixed-size keyset pages and latest readings only from a lookback
// window, so it never scans history.
package rebuild

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// Source is satisfied by *postgres.Store.
type Source interface {
	HousesPage(ctx context.Context, after string, limit int) ([]postgres.HouseRow, error)
	LatestReadingsPage(ctx context.Context, since time.Time, after string, limit int) ([]domain.MeterReading, error)
}

// Target is satisfied by *redis.Store.
type Target interface {
	SetLatestIfNewer(ctx context.Context, p redisstore.MeterProjection) (bool, error)
	RestoreHeartbeat(ctx context.Context, gridID, houseID string, status redisstore.HouseStatus, now time.Time) (bool, error)
	AddHouseToGrid(ctx context.Context, gridID, houseID string) error
}

type Options struct {
	// Lookback is how far back a latest reading may be. Zero means 1 hour.
	Lookback time.Duration
	// PageSize bounds each database read. Zero means 500.
	PageSize int
	// DryRun reads and counts but writes nothing to Redis.
	DryRun bool
	Now    func() time.Time
}

type Result struct {
	Houses         int
	Memberships    int
	StatusRestored int
	Latest         int
	LatestApplied  int
}

// Run rebuilds memberships, liveness and latest readings. Every write is
// idempotent and newest-wins, so it is safe to run while ingestion is live:
// it can never regress state the consumer has already refreshed.
func Run(ctx context.Context, src Source, dst Target, opts Options, logger *slog.Logger) (Result, error) {
	if opts.Lookback <= 0 {
		opts.Lookback = time.Hour
	}
	if opts.PageSize <= 0 {
		opts.PageSize = 500
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}

	var res Result
	now := opts.Now().UTC()

	after := ""
	for {
		houses, err := src.HousesPage(ctx, after, opts.PageSize)
		if err != nil {
			return res, err
		}
		if len(houses) == 0 {
			break
		}
		for _, h := range houses {
			res.Houses++
			if opts.DryRun {
				continue
			}
			if err := dst.AddHouseToGrid(ctx, h.GridID, h.HouseID); err != nil {
				return res, err
			}
			res.Memberships++
			if h.LastHeartbeatAt == nil {
				continue
			}
			ok, err := dst.RestoreHeartbeat(ctx, h.GridID, h.HouseID,
				redisstore.HouseStatus{Status: "online", LastHeartbeatAt: h.LastHeartbeatAt.UTC()}, now)
			if err != nil {
				return res, err
			}
			if ok {
				res.StatusRestored++
			}
		}
		after = houses[len(houses)-1].HouseID
	}

	since := now.Add(-opts.Lookback)
	after = ""
	for {
		readings, err := src.LatestReadingsPage(ctx, since, after, opts.PageSize)
		if err != nil {
			return res, err
		}
		if len(readings) == 0 {
			break
		}
		for _, r := range readings {
			res.Latest++
			if opts.DryRun {
				continue
			}
			applied, err := dst.SetLatestIfNewer(ctx, redisstore.ProjectionOf(r))
			if err != nil {
				return res, fmt.Errorf("rebuild latest for %s: %w", r.MeterID, err)
			}
			if applied {
				res.LatestApplied++
			}
		}
		after = readings[len(readings)-1].HouseID
	}

	logger.Info("redis rebuild finished",
		"houses", res.Houses, "memberships", res.Memberships, "status_restored", res.StatusRestored,
		"latest_found", res.Latest, "latest_applied", res.LatestApplied, "dry_run", opts.DryRun)
	return res, nil
}
