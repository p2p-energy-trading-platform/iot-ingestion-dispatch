package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

type countingMetrics struct {
	NopMetrics
	degraded map[string]int
}

func (m *countingMetrics) ObserveCacheDegraded(topic string) { m.degraded[topic]++ }

func TestMeterHandlerRejectsOversizedRecordBeforeAnyWork(t *testing.T) {
	w, l := &fakeWriter{}, &fakeLatest{}
	h := newTestMeterHandler(w, l).WithInputLimits(InputLimits{MaxRecordBytes: 64})

	err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON()))
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("error = %v, want ErrValidation", err)
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Fatalf("error %q does not mention the size limit", err)
	}
	if len(w.calls) != 0 || len(l.calls) != 0 {
		t.Fatal("stores were called for an oversized record")
	}
}

func TestMeterHandlerHouseKeyEnforcement(t *testing.T) {
	limits := InputLimits{EnforceHouseKey: true}
	tests := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{"matching key", []byte("grid01-house0042"), false},
		{"connector topic key", []byte("gridx/grid01/grid01-house0042/meter"), false},
		{"missing key tolerated", nil, false},
		{"different house", []byte("grid01-house0007"), true},
		{"topic key for another house", []byte("gridx/grid01/grid01-house0007/meter"), true},
		{"topic key for another grid", []byte("gridx/grid02/grid01-house0042/meter"), true},
		{"malformed topic key", []byte("gridx/grid01-house0042"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, l := &fakeWriter{inserted: true}, &fakeLatest{}
			h := newTestMeterHandler(w, l).WithInputLimits(limits)

			err := h.HandleRecord(context.Background(), tt.key, []byte(validMeterJSON()))
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("error = %v, want ErrValidation", err)
			}
			if tt.wantErr && (len(w.calls) != 0 || len(l.calls) != 0) {
				t.Fatal("stores were called for a mismatched key")
			}
		})
	}
}

func TestHouseKeyNotEnforcedByDefault(t *testing.T) {
	w, l := &fakeWriter{inserted: true}, &fakeLatest{}
	h := newTestMeterHandler(w, l)
	if err := h.HandleRecord(context.Background(), []byte("some/mqtt/topic"), []byte(validMeterJSON())); err != nil {
		t.Fatalf("a non-house key must be accepted when enforcement is off: %v", err)
	}
}

func TestMeterHandlerAsyncRepairAcknowledgesAfterDurableWrite(t *testing.T) {
	m := &countingMetrics{degraded: map[string]int{}}
	w, l := &fakeWriter{inserted: true}, &fakeLatest{err: errors.New("redis down")}
	h := newTestMeterHandler(w, l).WithCachePolicy(CacheAsyncRepair, "iot.meter-readings", m)

	if err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON())); err != nil {
		t.Fatalf("async-repair must acknowledge after the durable write, got %v", err)
	}
	if len(w.calls) != 1 {
		t.Fatalf("durable write calls = %d, want 1", len(w.calls))
	}
	if m.degraded["iot.meter-readings"] != 1 {
		t.Fatalf("degraded events = %v, want 1", m.degraded)
	}
}

func TestMeterHandlerAsyncRepairNeverMasksDurableFailure(t *testing.T) {
	errPG := errors.New("postgres down")
	w, l := &fakeWriter{err: errPG}, &fakeLatest{err: errors.New("redis down")}
	h := newTestMeterHandler(w, l).WithCachePolicy(CacheAsyncRepair, "iot.meter-readings", nil)

	if err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON())); !errors.Is(err, errPG) {
		t.Fatalf("error = %v, want the postgres error", err)
	}
}

func TestHeartbeatHandlerAsyncRepair(t *testing.T) {
	m := &countingMetrics{degraded: map[string]int{}}
	h, _, cache, _ := newTestHeartbeatHandler(time.Now())
	cache.err = errors.New("redis down")
	h.WithCachePolicy(CacheAsyncRepair, "iot.heartbeats", m)

	if err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON)); err != nil {
		t.Fatalf("error = %v, want nil under async-repair", err)
	}
	if m.degraded["iot.heartbeats"] != 1 {
		t.Fatalf("degraded events = %v, want 1", m.degraded)
	}
}

func TestHeartbeatHandlerRequiredPolicyReturnsRedisError(t *testing.T) {
	h, _, cache, _ := newTestHeartbeatHandler(time.Now())
	cache.err = errors.New("redis down")

	if err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON)); err == nil {
		t.Fatal("required policy must surface the Redis error")
	}
}

func TestHeartbeatHandlerRejectsOversizedRecord(t *testing.T) {
	h, store, cache, _ := newTestHeartbeatHandler(time.Now())
	h.WithInputLimits(InputLimits{MaxRecordBytes: 10})

	err := h.HandleRecord(context.Background(), nil, []byte(validHeartbeatJSON))
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("error = %v, want ErrValidation", err)
	}
	if len(store.calls) != 0 || len(cache.calls) != 0 {
		t.Fatal("stores were called for an oversized heartbeat")
	}
}
