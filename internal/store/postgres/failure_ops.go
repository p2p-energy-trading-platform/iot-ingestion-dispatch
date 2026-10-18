package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// FailureRow is one ingestion_failures row as seen by an operator. The raw
// payload is returned only to the replay path; listings omit it.
type FailureRow struct {
	ID            int64
	Topic         string
	Partition     int32
	Offset        int64
	Key           []byte
	RawPayload    []byte
	PayloadSHA256 string
	Stage         string
	Reason        string
	Attempts      int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	Resolution    string
}

const listUnresolvedFailuresSQL = `
SELECT id, kafka_topic, kafka_partition, kafka_offset, payload_sha256, failure_stage,
       error_reason, attempt_count, first_failed_at, last_failed_at, resolution
FROM iot_data.ingestion_failures
WHERE resolution = 'unresolved' AND id > $1
ORDER BY id
LIMIT $2`

// ListUnresolvedFailures pages unresolved failures by id. limit is required so
// no caller can issue an unbounded query.
func (store *Store) ListUnresolvedFailures(ctx context.Context, afterID int64, limit int) ([]FailureRow, error) {
	if limit <= 0 {
		return nil, errors.New("postgres: list failures: limit must be positive")
	}
	rows, err := store.pool.Query(ctx, listUnresolvedFailuresSQL, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list failures: %w", err)
	}
	defer rows.Close()

	var out []FailureRow
	for rows.Next() {
		var f FailureRow
		if err := rows.Scan(&f.ID, &f.Topic, &f.Partition, &f.Offset, &f.PayloadSHA256, &f.Stage,
			&f.Reason, &f.Attempts, &f.FirstFailedAt, &f.LastFailedAt, &f.Resolution); err != nil {
			return nil, fmt.Errorf("postgres: list failures scan: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

const backlogSQL = `
SELECT count(*), COALESCE(now() - min(first_failed_at), interval '0')
FROM iot_data.ingestion_failures
WHERE resolution = 'unresolved'`

// FailureBacklog returns how many failures are unresolved and the age of the
// oldest, for the growth/age alerts required by 01-kafka-consumption.md.
func (store *Store) FailureBacklog(ctx context.Context) (count int64, oldest time.Duration, err error) {
	if err := store.pool.QueryRow(ctx, backlogSQL).Scan(&count, &oldest); err != nil {
		return 0, 0, fmt.Errorf("postgres: failure backlog: %w", err)
	}
	return count, oldest, nil
}

const lockFailureSQL = `
SELECT id, kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload,
       payload_sha256, failure_stage, error_reason, attempt_count,
       first_failed_at, last_failed_at, resolution
FROM iot_data.ingestion_failures
WHERE id = $1
FOR UPDATE SKIP LOCKED`

const resolveFailureSQL = `
UPDATE iot_data.ingestion_failures
SET resolution = $2, resolved_at = now(), resolved_by = $3
WHERE id = $1`

const noteReplayFailureSQL = `
UPDATE iot_data.ingestion_failures
SET error_reason = $2, last_failed_at = now(), attempt_count = attempt_count + 1
WHERE id = $1`

// ErrFailureUnavailable means the failure does not exist, is already
// resolved, or is locked by another replay.
var ErrFailureUnavailable = errors.New("postgres: failure not found, already resolved, or being replayed")

// WithLockedFailure runs fn while holding a row lock on one unresolved
// failure, so two operators cannot replay it concurrently. fn returns the
// resolution to record ("replayed" or "discarded"), or an empty string to
// leave the row unresolved; in that case any non-nil replayErr is stored as
// the new error reason. Resolution is recorded with the actor and time.
func (store *Store) WithLockedFailure(
	ctx context.Context,
	id int64,
	actor string,
	fn func(FailureRow) (resolution string, replayErr error),
) (FailureRow, string, error) {
	var (
		row        FailureRow
		resolution string
	)
	err := pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, lockFailureSQL, id).Scan(
			&row.ID, &row.Topic, &row.Partition, &row.Offset, &row.Key, &row.RawPayload,
			&row.PayloadSHA256, &row.Stage, &row.Reason, &row.Attempts,
			&row.FirstFailedAt, &row.LastFailedAt, &row.Resolution)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrFailureUnavailable
		}
		if err != nil {
			return fmt.Errorf("postgres: lock failure %d: %w", id, err)
		}
		if row.Resolution != "unresolved" {
			return ErrFailureUnavailable
		}

		var replayErr error
		resolution, replayErr = fn(row)
		if resolution == "" {
			if replayErr != nil {
				if _, err := tx.Exec(ctx, noteReplayFailureSQL, id, sanitizeText(replayErr.Error())); err != nil {
					return fmt.Errorf("postgres: note replay failure %d: %w", id, err)
				}
			}
			return nil
		}
		if _, err := tx.Exec(ctx, resolveFailureSQL, id, resolution, actor); err != nil {
			return fmt.Errorf("postgres: resolve failure %d: %w", id, err)
		}
		return nil
	})
	return row, resolution, err
}
