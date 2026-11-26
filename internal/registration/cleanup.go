package registration

import (
    "context"
    "log/slog"
    "time"
)

const (
    defaultCleanupInterval    = 24 * time.Hour
    defaultRetentionPeriod    = 5 * 24 * time.Hour
    defaultCleanupBatchSize   = 500
)

type CleanupStore interface {
    DeleteStaleMeterRegistrations(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

type CleanupConfig struct {
    Interval        time.Duration
    RetentionPeriod time.Duration
    BatchSize       int
}

func (c CleanupConfig) withDefaults() CleanupConfig {

    if c.Interval <= 0 {
        c.Interval = defaultCleanupInterval
    }

    if c.RetentionPeriod <= 0 {
        c.RetentionPeriod = defaultRetentionPeriod
    }

    if c.BatchSize <= 0 {
        c.BatchSize = defaultCleanupBatchSize
    }

    return c

}

type CleanupWorker struct {
    store  CleanupStore
    config CleanupConfig
    logger *slog.Logger
}

func NewCleanupWorker(store CleanupStore, config CleanupConfig, logger *slog.Logger) *CleanupWorker {
    
	if logger == nil {
        logger = slog.Default()
    }

    return &CleanupWorker{
        store:  store,
        config: config.withDefaults(),
        logger: logger,
    }

}

func (w *CleanupWorker) Start(ctx context.Context) {

    w.cleanup(ctx)

    ticker := time.NewTicker(w.config.Interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            w.cleanup(ctx)
        }
    }

}

func (w *CleanupWorker) cleanup(ctx context.Context) {

    cutoff := time.Now().Add(-w.config.RetentionPeriod)
    batchSize := int64(w.config.BatchSize)
    var deletedTotal int64
    var batchCount int

    for ctx.Err() == nil {
        deleted, err := w.store.DeleteStaleMeterRegistrations(ctx, cutoff, w.config.BatchSize)
        if err != nil {
            w.logger.Error("meter registration cleanup failed", "error", err)
            return
        }

        batchCount++
        deletedTotal += deleted

        if deleted < batchSize {
            break
        }
    }

    w.logger.Info(
        "meter registration cleanup completed",
        "deleted_count", deletedTotal,
        "batch_count", batchCount,
        "cutoff", cutoff,
    )
	
}