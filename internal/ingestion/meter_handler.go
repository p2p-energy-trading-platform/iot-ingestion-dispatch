package ingestion

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// GridAdmitter is satisfied by *admission.Registry.
type GridAdmitter interface {
	Lookup(gridID string) (admission.Grid, bool)
}

// MeterWriter is satisfied by *postgres.Store. inserted is false for an
// identical replay.
type MeterWriter interface {
	InsertMeterReading(ctx context.Context, r domain.MeterReading) (inserted bool, err error)
}

// LatestWriter is satisfied by *redis.Store.
type LatestWriter interface {
	SetLatestIfNewer(ctx context.Context, p redisstore.MeterProjection) (applied bool, err error)
}

// MeterHandler runs the meter-reading workflow. It returns nil only when
// every required step succeeded, which is the signal for the partition
// worker to commit the Kafka offset.
type MeterHandler struct {
	admission GridAdmitter
	writer    MeterWriter
	latest    LatestWriter
	logger    *slog.Logger
}

var _ RecordHandler = (*MeterHandler)(nil)

func NewMeterHandler(admission GridAdmitter, writer MeterWriter, latest LatestWriter, logger *slog.Logger) *MeterHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &MeterHandler{admission: admission, writer: writer, latest: latest, logger: logger}
}

func (h *MeterHandler) HandleRecord(ctx context.Context, _, value []byte) error {
	// 1. Decode and validate.
	reading, err := DecodeMeterReading(value)
	if err != nil {
		return fmt.Errorf("decode meter reading: %w", err)
	}

	// 2. Admission: the grid must be provisioned.
	if _, ok := h.admission.Lookup(reading.GridID); !ok {
		return fmt.Errorf("%w: %q", admission.ErrUnknownGrid, reading.GridID)
	}

	// 3-6. Transactional, immutable insert (parent + storage assets).
	inserted, err := h.writer.InsertMeterReading(ctx, reading)
	if err != nil {
		return fmt.Errorf("persist meter reading %s seq %d: %w", reading.MeterID, reading.Seq, err)
	}
	if !inserted {
		h.logger.Debug("meter reading replay; history unchanged",
			"meter_id", reading.MeterID, "seq", reading.Seq)
	}

	// 7. Latest state, only if newer. Runs on replays as well, in case a
	// previous attempt committed to Postgres but failed here.
	if _, err := h.latest.SetLatestIfNewer(ctx, toMeterProjection(reading)); err != nil {
		return fmt.Errorf("update latest state for %s: %w", reading.MeterID, err)
	}

	// 8. The caller commits the Kafka offset on nil.
	return nil
}

func toMeterProjection(r domain.MeterReading) redisstore.MeterProjection {
	p := redisstore.MeterProjection{
		EventTimeUnixMs: r.EventTime.UnixMilli(),
		Seq:             r.Seq,
		GridID:          r.GridID,
		HouseID:         r.HouseID,
		MeterID:         r.MeterID,
		DeviceClass:     string(r.DeviceClass),
		SolarKw:         r.SolarKw,
		ConsumptionKw:   r.ConsumptionKw,
		NetKw:           r.NetKw,
	}
	for _, a := range r.StorageAssets {
		p.StorageAssets = append(p.StorageAssets, redisstore.StorageAssetProjection{
			AssetID:     a.AssetID,
			AssetType:   string(a.AssetType),
			SocPct:      a.SocPct,
			PowerKw:     a.PowerKw,
			CapacityKwh: a.CapacityKwh,
			PluggedIn:   a.PluggedIn,
		})
	}
	return p
}
