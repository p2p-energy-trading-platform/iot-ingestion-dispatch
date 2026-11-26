package registration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeCleanupStore struct {
	results []int64
	err     error

	cutoffs []time.Time
	limits  []int
}

func (s *fakeCleanupStore) DeleteStaleMeterRegistrations(
	_ context.Context,
	cutoff time.Time,
	limit int,
) (int64, error) {
	s.cutoffs = append(s.cutoffs, cutoff)
	s.limits = append(s.limits, limit)
	if s.err != nil {
		return 0, s.err
	}
	index := len(s.cutoffs) - 1
	if index >= len(s.results) {
		return 0, nil
	}
	return s.results[index], nil
}

func TestCleanupProcessedBatchesUntilShortBatch(t *testing.T) {
	const (
		batchSize       = 500
		retentionPeriod = 5 * 24 * time.Hour
	)
	store := &fakeCleanupStore{
		results: []int64{batchSize, batchSize, 3},
	}
	worker := NewCleanupWorker(store, CleanupConfig{
		RetentionPeriod: retentionPeriod,
		BatchSize:       batchSize,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	before := time.Now()
	worker.cleanup(context.Background())
	after := time.Now()
	if got, want := len(store.cutoffs), 3; got != want {
		t.Fatalf("cleanup calls = %d, want %d", got, want)
	}
	for i, cutoff := range store.cutoffs {
		if cutoff.Before(before.Add(-retentionPeriod)) ||
			cutoff.After(after.Add(-retentionPeriod)) {
			t.Errorf("call %d cutoff = %v, outside expected range", i, cutoff)
		}
		if got := store.limits[i]; got != batchSize {
			t.Errorf("call %d limit = %d, want %d", i, got, batchSize)
		}
		if !cutoff.Equal(store.cutoffs[0]) {
			t.Errorf("call %d cutoff = %v, want the same cutoff %v", i, cutoff, store.cutoffs[0])
		}
	}
}

func TestCleanupStopsOnStoreError(t *testing.T) {
	store := &fakeCleanupStore{
		results: []int64{500, 2},
		err:     errors.New("database unavailable"),
	}
	worker := NewCleanupWorker(store, CleanupConfig{
		BatchSize: 500,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	worker.cleanup(context.Background())
	if got, want := len(store.cutoffs), 1; got != want {
		t.Fatalf("cleanup calls = %d, want %d after store error", got, want)
	}
}
