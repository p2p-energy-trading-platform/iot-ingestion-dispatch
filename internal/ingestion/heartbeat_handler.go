package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

// HeartbeatStore is satisfied by *postgres.Store.
type HeartbeatStore interface {
	ApplyHeartbeat(ctx context.Context, hb domain.Heartbeat, receivedAt time.Time) error
}

// HeartbeatCache is satisfied by *redis.Store.
type HeartbeatCache interface {
	UpdateHeartbeat(ctx context.Context, gridID, houseID string, status redisstore.HouseStatus) error
}

// HeartbeatHandler runs the heartbeat workflow. It returns nil only when
// every required step succeeded, which is the signal for the partition
// worker to commit the Kafka offset.
type HeartbeatHandler struct {
	admission GridAdmitter
	store     HeartbeatStore
	cache     HeartbeatCache
	logger    *slog.Logger
	now       func() time.Time
}

var _ RecordHandler = (*HeartbeatHandler)(nil)

func NewHeartbeatHandler(admission GridAdmitter, store HeartbeatStore, cache HeartbeatCache, logger *slog.Logger) *HeartbeatHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HeartbeatHandler{
		admission: admission,
		store:     store,
		cache:     cache,
		logger:    logger,
		now:       time.Now,
	}
}

func (h *HeartbeatHandler) HandleRecord(ctx context.Context, _, value []byte) error {
	// One receive timestamp, shared by Postgres liveness and Redis state.
	// Microsecond precision matches what timestamptz can store.
	receivedAt := h.now().UTC().Truncate(time.Microsecond)

	// 1. Decode and validate.
	hb, err := DecodeHeartbeat(value)
	if err != nil {
		return fmt.Errorf("decode heartbeat: %w", err)
	}

	// 2. Admission: the grid must be provisioned.
	if _, ok := h.admission.Lookup(hb.GridID); !ok {
		return fmt.Errorf("%w: %q", admission.ErrUnknownGrid, hb.GridID)
	}

	// 3. No duplicate asset IDs within the payload.
	if err := checkDuplicateAssetIDs(hb); err != nil {
		return fmt.Errorf("heartbeat for %s: %w", hb.HouseID, err)
	}

	// 4-8. Transactional house identity, liveness, asset ownership and
	// changed-only capability writes.
	if err := h.store.ApplyHeartbeat(ctx, hb, receivedAt); err != nil {
		return fmt.Errorf("persist heartbeat for %s: %w", hb.HouseID, err)
	}

	// 9. Atomic Redis heartbeat update, with the same receive timestamp.
	status := redisstore.HouseStatus{Status: hb.Status, LastHeartbeatAt: receivedAt}
	if err := h.cache.UpdateHeartbeat(ctx, hb.GridID, hb.HouseID, status); err != nil {
		return fmt.Errorf("update heartbeat state for %s: %w", hb.HouseID, err)
	}

	// 10. The caller commits the Kafka offset on nil.
	return nil
}

func checkDuplicateAssetIDs(hb domain.Heartbeat) error {
	seen := make(map[string]struct{}, len(hb.FlexibleAssets))
	for i, a := range hb.FlexibleAssets {
		if _, dup := seen[a.AssetID]; dup {
			return domain.NewValidationError(
				fmt.Sprintf("flexible_assets[%d].asset_id", i), "duplicate asset_id in flexible_assets")
		}
		seen[a.AssetID] = struct{}{}
	}
	return nil
}
