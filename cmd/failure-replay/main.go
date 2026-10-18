// Command failure-replay is the operator tool for ingestion_failures
// (see docs/runbooks/failure-replay.md).
//
//	failure-replay list    [-limit N] [-after ID]
//	failure-replay replay  -actor NAME -id ID [-id ID ...]
//	failure-replay discard -actor NAME -id ID [-id ID ...]
//
// Replay selects explicit failure IDs only; there is no "replay everything"
// mode. Each payload is revalidated by the current decoder and written through
// the same idempotent handlers the consumer uses, under a row lock so a failure
// cannot be replayed twice at once. Kafka offsets are never touched.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

const maxBatch = 100

type idList []int64

func (l *idList) String() string { return fmt.Sprint(*l) }
func (l *idList) Set(v string) error {
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid failure id %q", v)
	}
	*l = append(*l, id)
	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger, os.Args[1:]); err != nil {
		logger.Error("failure-replay failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: failure-replay list|replay|discard [flags]")
	}
	command := args[0]

	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	var ids idList
	limit := fs.Int("limit", 50, "list: rows to show")
	after := fs.Int64("after", 0, "list: only ids greater than this")
	actor := fs.String("actor", "", "replay/discard: who is acting (recorded on the row)")
	fs.Var(&ids, "id", "failure id (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	switch command {
	case "list":
	case "replay", "discard":
		if *actor == "" {
			return errors.New("-actor is required")
		}
		if len(ids) == 0 {
			return errors.New("at least one -id is required")
		}
		if len(ids) > maxBatch {
			return fmt.Errorf("at most %d ids per run", maxBatch)
		}
	default:
		return fmt.Errorf("unknown command %q", command)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	pg, err := postgresstore.NewWithPool(ctx, cfg.Postgres.URL, postgresstore.PoolConfig{MaxConns: 4})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pg.Close()

	switch command {
	case "list":
		return list(ctx, pg, *after, *limit)
	case "discard":
		return resolveAll(ctx, pg, ids, *actor, func(postgresstore.FailureRow) (string, error) {
			return "discarded", nil
		})
	}

	// replay
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

	// Replay admits against the *current* provisioned grids, so a failure
	// quarantined for an unknown grid replays only once the grid exists.
	registry := admission.NewRegistry()
	refresher := admission.NewRefresher(registry, pg, admission.RefresherConfig{}, logger)
	if err := refresher.Bootstrap(ctx); err != nil {
		return fmt.Errorf("grid registry: %w", err)
	}

	router := ingestion.NewRouter()
	limits := ingestion.InputLimits{MaxRecordBytes: cfg.Input.MaxRecordBytes, EnforceHouseKey: cfg.Input.EnforceHouseKey}
	router.Register(cfg.Kafka.MeterTopic, ingestion.NewMeterHandler(registry, pg, rd, logger).WithInputLimits(limits))
	router.Register(cfg.Kafka.HeartbeatTopic, ingestion.NewHeartbeatHandler(registry, pg, rd, logger).WithInputLimits(limits))

	return resolveAll(ctx, pg, ids, *actor, func(f postgresstore.FailureRow) (string, error) {
		if err := router.Route(ctx, f.Topic, f.Key, f.RawPayload); err != nil {
			return "", err
		}
		return "replayed", nil
	})
}

func list(ctx context.Context, pg *postgresstore.Store, after int64, limit int) error {
	if limit <= 0 || limit > 500 {
		return errors.New("-limit must be between 1 and 500")
	}
	rows, err := pg.ListUnresolvedFailures(ctx, after, limit)
	if err != nil {
		return err
	}
	fmt.Printf("%-8s %-20s %-5s %-10s %-16s %-4s %-20s %s\n", "ID", "TOPIC", "PART", "OFFSET", "STAGE", "TRY", "LAST FAILED", "SHA256/REASON")
	for _, f := range rows {
		reason := f.Reason
		if len(reason) > 80 {
			reason = reason[:80] + "..."
		}
		fmt.Printf("%-8d %-20s %-5d %-10d %-16s %-4d %-20s %.12s %s\n", f.ID, f.Topic, f.Partition, f.Offset,
			f.Stage, f.Attempts, f.LastFailedAt.UTC().Format("2006-01-02T15:04:05Z"), f.PayloadSHA256, reason)
	}
	fmt.Printf("%d unresolved failure(s) shown\n", len(rows))
	return nil
}

func resolveAll(
	ctx context.Context,
	pg *postgresstore.Store,
	ids idList,
	actor string,
	fn func(postgresstore.FailureRow) (string, error),
) error {
	failed := 0
	for _, id := range ids {
		var replayErr error
		_, resolution, err := pg.WithLockedFailure(ctx, id, actor, func(f postgresstore.FailureRow) (string, error) {
			res, e := fn(f)
			replayErr = e
			return res, e
		})
		switch {
		case errors.Is(err, postgresstore.ErrFailureUnavailable):
			fmt.Printf("id=%d skipped: not found, already resolved, or being replayed\n", id)
			failed++
		case err != nil:
			fmt.Printf("id=%d error: %v\n", id, err)
			failed++
		case resolution == "":
			fmt.Printf("id=%d still unresolved: %v\n", id, replayErr)
			failed++
		default:
			fmt.Printf("id=%d %s by %s\n", id, resolution, actor)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d failure(s) not resolved", failed, len(ids))
	}
	return nil
}
