package ingestion

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

// InputLimits are the abuse limits applied to every Kafka record before and
// right after JSON decoding (01-kafka-consumption.md "Input contract").
type InputLimits struct {
	// MaxRecordBytes rejects a larger value before it is parsed. Zero disables
	// the check.
	MaxRecordBytes int
	// EnforceHouseKey rejects a record whose non-empty Kafka key names a
	// different house than the payload. The key may be the bare house_id, or
	// the MQTT source topic "gridx/{grid_id}/{house_id}/{kind}" that the Kafka
	// Connect MQTT source uses as its key. A missing key is tolerated:
	// consumer-side checks cannot repair ordering, so this only catches
	// identity disagreement.
	EnforceHouseKey bool
}

func (l InputLimits) checkSize(value []byte) error {
	if l.MaxRecordBytes > 0 && len(value) > l.MaxRecordBytes {
		return domain.NewValidationError("payload",
			fmt.Sprintf("record is %d bytes, larger than the %d byte limit", len(value), l.MaxRecordBytes))
	}
	return nil
}

func (l InputLimits) checkKey(key []byte, gridID, houseID string) error {
	if !l.EnforceHouseKey || len(key) == 0 {
		return nil
	}
	k := string(key)
	if k == houseID {
		return nil
	}
	if parts := strings.Split(k, "/"); len(parts) == 4 && parts[0] == "gridx" && parts[1] == gridID && parts[2] == houseID {
		return nil
	}
	return domain.NewValidationError("key", "Kafka record key does not match the payload grid_id/house_id")
}

// CachePolicy decides what a failed Redis projection update means once the
// durable write has succeeded.
type CachePolicy int

const (
	// CacheRequired returns the Redis error, leaving the offset uncommitted
	// so the record is retried until the cache is current.
	CacheRequired CachePolicy = iota
	// CacheAsyncRepair acknowledges after the durable write. The projection is
	// repaired by the next reading/heartbeat or the rebuild command, and the
	// degraded-cache metric records the gap.
	CacheAsyncRepair
)

// Metrics is the ingestion package's view of instrumentation. Implementations
// must not use house, grid, meter, asset or Kafka-key values as labels.
type Metrics interface {
	// ObserveRecord reports one finished handler attempt chain: result is
	// "ok", "retry_exhausted" or "permanent".
	ObserveRecord(topic, result string, took time.Duration)
	ObserveFailureRecorded(topic, stage string)
	ObserveRetry(topic string)
	ObserveCommit(took time.Duration, ok bool)
	ObserveRebalance(kind string)
	ObserveCacheDegraded(topic string)
}

// NopMetrics is the default when no instrumentation is wired.
type NopMetrics struct{}

func (NopMetrics) ObserveRecord(string, string, time.Duration) {}
func (NopMetrics) ObserveFailureRecorded(string, string)       {}
func (NopMetrics) ObserveRetry(string)                         {}
func (NopMetrics) ObserveCommit(time.Duration, bool)           {}
func (NopMetrics) ObserveRebalance(string)                     {}
func (NopMetrics) ObserveCacheDegraded(string)                 {}

var _ Metrics = NopMetrics{}

// absorbCacheError applies the policy to a Redis error. It returns nil when
// the error is tolerated.
func absorbCacheError(policy CachePolicy, metrics Metrics, logger *slog.Logger, topic string, err error) error {
	if err == nil || policy != CacheAsyncRepair {
		return err
	}
	metrics.ObserveCacheDegraded(topic)
	logger.Warn("redis projection update failed; acknowledging after durable write (async-repair policy)",
		"topic", topic, "error", err)
	return nil
}
