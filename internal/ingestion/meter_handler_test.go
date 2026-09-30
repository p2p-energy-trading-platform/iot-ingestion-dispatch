package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// validMeterJSON builds a payload stamped "now", because the decoder
// rejects timestamps more than 24h from the current time.
func validMeterJSON() string {
	return fmt.Sprintf(`{
  "schema_version": "1.0",
  "meter_id": "meter-grid01-house0042",
  "house_id": "grid01-house0042",
  "grid_id": "grid01",
  "device_class": "residential_prosumer",
  "timestamp": %q,
  "seq": 184231,
  "readings": {
    "solar_kw": 2.41, "consumption_kw": 0.87, "net_kw": 1.54,
    "storage_assets": [
      {"asset_id": "bat_001", "asset_type": "bess", "soc_pct": 63.5, "power_kw": -0.30, "capacity_kwh": 10.0}
    ]
  }
}`, time.Now().UTC().Format(time.RFC3339))
}

type fakeAdmitter struct{ known map[string]bool }

func (f fakeAdmitter) Lookup(id string) (admission.Grid, bool) {
	return admission.Grid{GridID: id}, f.known[id]
}

type fakeWriter struct {
	calls    []domain.MeterReading
	inserted bool
	err      error
}

func (f *fakeWriter) InsertMeterReading(_ context.Context, r domain.MeterReading) (bool, error) {
	f.calls = append(f.calls, r)
	return f.inserted, f.err
}

type fakeLatest struct {
	calls []redisstore.MeterProjection
	err   error
}

func (f *fakeLatest) SetLatestIfNewer(_ context.Context, p redisstore.MeterProjection) (bool, error) {
	f.calls = append(f.calls, p)
	return true, f.err
}

func newTestMeterHandler(w *fakeWriter, l *fakeLatest) *MeterHandler {
	return NewMeterHandler(
		fakeAdmitter{known: map[string]bool{"grid01": true}},
		w, l,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestMeterHandlerHappyPath(t *testing.T) {
	w, l := &fakeWriter{inserted: true}, &fakeLatest{}
	h := newTestMeterHandler(w, l)

	if err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON())); err != nil {
		t.Fatal(err)
	}
	if len(w.calls) != 1 || len(l.calls) != 1 {
		t.Fatalf("writer calls = %d, redis calls = %d, want 1 and 1", len(w.calls), len(l.calls))
	}

	p := l.calls[0]
	if p.MeterID != "meter-grid01-house0042" || p.Seq != 184231 || p.GridID != "grid01" ||
		p.NetKw != 1.54 || len(p.StorageAssets) != 1 || p.StorageAssets[0].AssetID != "bat_001" {
		t.Fatalf("unexpected projection: %+v", p)
	}
	if p.EventTimeUnixMs != w.calls[0].EventTime.UnixMilli() {
		t.Fatalf("projection time %d does not match stored reading time", p.EventTimeUnixMs)
	}
}

func TestMeterHandlerInvalidPayloadTouchesNothing(t *testing.T) {
	w, l := &fakeWriter{}, &fakeLatest{}
	h := newTestMeterHandler(w, l)

	err := h.HandleRecord(context.Background(), nil, []byte(`{"schema_version":"1.0"`))
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("error = %v, want ErrValidation", err)
	}
	if len(w.calls) != 0 || len(l.calls) != 0 {
		t.Fatal("stores were called for an invalid payload")
	}
}

func TestMeterHandlerUnknownGridTouchesNothing(t *testing.T) {
	w, l := &fakeWriter{}, &fakeLatest{}
	h := NewMeterHandler(fakeAdmitter{known: map[string]bool{"grid99": true}}, w, l,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON()))
	if !errors.Is(err, admission.ErrUnknownGrid) {
		t.Fatalf("error = %v, want ErrUnknownGrid", err)
	}
	if len(w.calls) != 0 || len(l.calls) != 0 {
		t.Fatal("stores were called for an unknown grid")
	}
}

func TestMeterHandlerWriteFailureSkipsRedis(t *testing.T) {
	w := &fakeWriter{err: domain.ErrIntegrityConflict}
	l := &fakeLatest{}
	h := newTestMeterHandler(w, l)

	err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON()))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}
	if len(l.calls) != 0 {
		t.Fatal("redis was updated after a failed write")
	}
}

func TestMeterHandlerReplayStillUpdatesRedis(t *testing.T) {
	w, l := &fakeWriter{inserted: false}, &fakeLatest{}
	h := newTestMeterHandler(w, l)

	if err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON())); err != nil {
		t.Fatal(err)
	}
	if len(l.calls) != 1 {
		t.Fatalf("redis calls = %d, want 1", len(l.calls))
	}
}

func TestMeterHandlerRedisFailureIsReturned(t *testing.T) {
	errRedis := errors.New("redis down")
	w, l := &fakeWriter{inserted: true}, &fakeLatest{err: errRedis}
	h := newTestMeterHandler(w, l)

	err := h.HandleRecord(context.Background(), nil, []byte(validMeterJSON()))
	if !errors.Is(err, errRedis) {
		t.Fatalf("error = %v, want it to wrap errRedis", err)
	}
}
