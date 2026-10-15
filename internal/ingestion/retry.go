package ingestion

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const (
	defaultRetryMaxTries        = 3
	maxRetryTries               = 10
	defaultRetryInitialInterval = 100 * time.Millisecond
	defaultRetryMaxInterval     = 2 * time.Second
)

// Failure stages persisted on ingestion_failures.failure_stage. They must
// match the CHECK constraint (migrations 00007 and 00008).
const (
	StageDecode         = "decode"
	StageGridValidation = "grid_validation"
	StageRedisWrite     = "redis_write"
	StageTimescaleWrite = "timescale_write"
	StageHandlerPanic   = "handler_panic"
)

// RetryPolicy is the bounded exponential-backoff budget for a single Kafka
// record. Permanent errors skip the budget entirely (see isPermanent).
type RetryPolicy struct {
	MaxTries        int // total attempts, including the first
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxTries:        defaultRetryMaxTries,
		InitialInterval: defaultRetryInitialInterval,
		MaxInterval:     defaultRetryMaxInterval,
	}
}

// withDefaults makes the policy safe to use: the attempt count is always in
// 1..maxRetryTries (the library treats 0 as "retry forever"), and the
// intervals are always positive with MaxInterval >= InitialInterval.
func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxTries <= 0 {
		p.MaxTries = defaultRetryMaxTries
	}
	if p.MaxTries > maxRetryTries {
		p.MaxTries = maxRetryTries
	}
	if p.InitialInterval <= 0 {
		p.InitialInterval = defaultRetryInitialInterval
	}
	if p.MaxInterval <= 0 {
		p.MaxInterval = defaultRetryMaxInterval
	}
	if p.MaxInterval < p.InitialInterval {
		p.MaxInterval = p.InitialInterval
	}
	return p
}

func (p RetryPolicy) backoff() *backoff.ExponentialBackOff {
	p = p.withDefaults()
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = p.InitialInterval
	b.MaxInterval = p.MaxInterval
	b.Multiplier = 2
	b.RandomizationFactor = 0
	return b
}

// retryHandle runs op until it succeeds, a permanent error is returned, the
// retry budget is exhausted, or ctx is cancelled. ctx only interrupts the
// waits between attempts; op decides for itself what context it runs under.
// attempts is the number of times op actually ran (1 for a permanent error).
func retryHandle(ctx context.Context, policy RetryPolicy, logger *slog.Logger, op func() error) (attempts int, err error) {
	policy = policy.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}

	_, err = backoff.Retry(ctx, func() (struct{}, error) {
		attempts++
		opErr := op()
		if opErr == nil {
			return struct{}{}, nil
		}
		if isPermanent(opErr) {
			return struct{}{}, backoff.Permanent(opErr)
		}
		return struct{}{}, opErr
	},
		backoff.WithMaxTries(uint(policy.MaxTries)),
		backoff.WithBackOff(policy.backoff()),
		backoff.WithNotify(func(notifyErr error, delay time.Duration) {
			logger.Warn("transient ingestion failure; backing off",
				"error", notifyErr,
				"delay", delay,
				"attempt", attempts,
			)
		}),
	)
	return attempts, err
}

// isPermanent reports errors that cannot succeed on retry: malformed
// payloads, unprovisioned grids, immutable-history conflicts, handler panics
// (a deterministic bug), and a missing handler (a wiring bug, which the worker
// treats as fatal and never records).
func isPermanent(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, domain.ErrValidation),
		errors.Is(err, domain.ErrIntegrityConflict),
		errors.Is(err, domain.ErrGridNotProvisioned),
		errors.Is(err, admission.ErrUnknownGrid),
		errors.Is(err, ErrHandlerPanic),
		errors.Is(err, ErrNoHandler):
		return true
	default:
		return false
	}
}

func failureStage(err error) string {
	switch {
	case errors.Is(err, ErrHandlerPanic):
		return StageHandlerPanic
	case errors.Is(err, domain.ErrValidation):
		return StageDecode
	case errors.Is(err, admission.ErrUnknownGrid), errors.Is(err, domain.ErrGridNotProvisioned):
		return StageGridValidation
	case errors.Is(err, domain.ErrIntegrityConflict):
		return StageTimescaleWrite
	default:
		if isRedisStage(err) {
			return StageRedisWrite
		}
		return StageTimescaleWrite
	}
}

func isRedisStage(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "redis:") ||
		strings.Contains(msg, "update latest state") ||
		strings.Contains(msg, "update heartbeat state")
}
