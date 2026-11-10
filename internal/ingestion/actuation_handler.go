package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

type MeterRegisteredPayload struct {
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
	Data      struct {
		HouseID      string  `json:"house_id"`
		MeterID      string  `json:"meter_id"`
		GridID       string  `json:"grid_id"`
		DeviceClass  string  `json:"device_class"`
		RatedSolarKw float64 `json:"rated_solar_kw"`
	} `json:"data"`
}

type HouseRegistrar interface {
	RegisterHouse(ctx context.Context, reg domain.HouseRegistration) (string, error)
}

type ActuationHandler struct {
	store  HouseRegistrar
	logger *slog.Logger
}

var _ RecordHandler = (*ActuationHandler)(nil)

func NewActuationHandler(store HouseRegistrar, logger *slog.Logger) *ActuationHandler {
	return &ActuationHandler{
		store:  store,
		logger: logger,
	}
}

func (h *ActuationHandler) HandleRecord(ctx context.Context, key, value []byte) error {
	var payload MeterRegisteredPayload
	if err := json.Unmarshal(value, &payload); err != nil {
		return fmt.Errorf("decode meter registered payload: %w", err)
	}

	if payload.EventType != "METER_REGISTERED" {
		h.logger.Debug("Skipping unhandled event type", "event_type", payload.EventType)
		return nil
	}

	h.logger.Info("Received METER_REGISTERED event from simulation",
		"house_id", payload.Data.HouseID,
		"meter_id", payload.Data.MeterID,
		"grid_id", payload.Data.GridID,
	)

	solar := payload.Data.RatedSolarKw

	outcome, err := h.store.RegisterHouse(
		ctx,
		domain.HouseRegistration{
			HouseID:      payload.Data.HouseID,
			GridID:       payload.Data.GridID,
			MeterID:      payload.Data.MeterID,
			DeviceClass:  domain.DeviceClass(payload.Data.DeviceClass),
			RatedSolarKw: &solar,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to register house %q from simulation response: %w", payload.Data.HouseID, err)
	}

	h.logger.Info("Successfully processed house registration from simulation",
		"house_id", payload.Data.HouseID,
		"outcome", outcome,
	)

	return nil
}
