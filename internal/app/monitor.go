package app

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const probeInterval = 15 * time.Second

// monitor periodically probes the stores and publishes dependency, pool and
// failure-backlog signals. It never affects readiness: dependency trouble is
// an operational signal, and the consumer already pauses without committing
// when a store is down (11-production-readiness.md "Health and dependency
// policy").
func (app *App) monitor(ctx context.Context) {
	app.probe(ctx)

	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			app.probe(ctx)
		}
	}
}

func (app *App) probe(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pgErr := app.postgres.Ping(probeCtx)
	app.metrics.SetDependencyUp("postgres", pgErr == nil)

	app.metrics.SetDependencyUp("redis", app.redis.Ping(probeCtx) == nil)

	if pgErr == nil {
		if count, oldest, err := app.postgres.FailureBacklog(probeCtx); err == nil {
			app.metrics.SetFailureBacklog(count, oldest)
		} else {
			app.logger.Warn("failure backlog probe failed", "error", err)
		}
	}
}

// registerPoolMetrics exposes pgx pool usage (connections in use, waits).
func (app *App) registerPoolMetrics() {
	gauge := func(name, help string, value func() float64) prometheus.GaugeFunc {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: name, Help: help,
		}, value)
	}
	app.metrics.Registry().MustRegister(
		gauge("postgres_pool_acquired_conns", "Connections currently checked out.",
			func() float64 { return float64(app.postgres.PoolStats().AcquiredConns()) }),
		gauge("postgres_pool_idle_conns", "Idle pooled connections.",
			func() float64 { return float64(app.postgres.PoolStats().IdleConns()) }),
		gauge("postgres_pool_max_conns", "Configured connection limit.",
			func() float64 { return float64(app.postgres.PoolStats().MaxConns()) }),
		gauge("postgres_pool_empty_acquires", "Acquires that had to wait for a connection (cumulative).",
			func() float64 { return float64(app.postgres.PoolStats().EmptyAcquireCount()) }),
	)
}
