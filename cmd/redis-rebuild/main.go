// Command redis-rebuild repopulates the Redis projection from TimescaleDB
// (see docs/runbooks/redis-rebuild.md). It is bounded and idempotent, and
// safe to run while ingestion is live.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/rebuild"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("redis rebuild failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	lookback := flag.Duration("lookback", time.Hour, "how far back to look for each house's latest reading")
	pageSize := flag.Int("page-size", 500, "rows per database read")
	dryRun := flag.Bool("dry-run", false, "count what would be rebuilt without writing to Redis")
	timeout := flag.Duration("timeout", 10*time.Minute, "overall deadline")
	flag.Parse()

	_ = godotenv.Load()
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	pg, err := postgresstore.NewWithPool(ctx, cfg.Postgres.URL, postgresstore.PoolConfig{MaxConns: 2})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pg.Close()

	rd, err := redisstore.New(ctx, redisstore.Config{
		Address: cfg.Redis.Address, Username: cfg.Redis.Username, Password: cfg.Redis.Password, DB: cfg.Redis.DB,
		TLS: cfg.Redis.TLS, TLSInsecureSkipVer: cfg.Redis.TLSSkipVerif,
		DialTimeout: cfg.Redis.DialTimeout, ReadTimeout: cfg.Redis.ReadTimeout, WriteTimeout: cfg.Redis.WriteTimeout,
		PoolSize: 4, MaxRetries: cfg.Redis.MaxRetries,
	})
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer func() { _ = rd.Close() }()

	_, err = rebuild.Run(ctx, pg, rd, rebuild.Options{
		Lookback: *lookback, PageSize: *pageSize, DryRun: *dryRun,
	}, logger)
	return err
}
