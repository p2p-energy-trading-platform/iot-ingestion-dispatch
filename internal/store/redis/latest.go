package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by read adapters when a key doesn't exist —
// either nothing has ever been cached for it, or (for keys with a TTL)
// it has expired.
var ErrNotFound = errors.New("redis: not found")

// StorageAssetProjection is the flexible-asset slice inside a
// MeterProjection.
type StorageAssetProjection struct {
	AssetID     string  `json:"asset_id"`
	AssetType   string  `json:"asset_type"`
	SocPct      float64 `json:"soc_pct"`
	PowerKw     float64 `json:"power_kw"`
	CapacityKwh float64 `json:"capacity_kwh"`
	PluggedIn   *bool   `json:"plugged_in,omitempty"`
}

// MeterProjection is the internal Redis projection stored (as a single
// JSON string) at meter:{grid_id}:{house_id}:latest.
//
// This is deliberately a DISTINCT type from the Kafka wire payload, which
// lives unexported inside internal/ingestion/decoder.go. Per
// 02-redis-hot-storage.md: "Redis key values are internal projections and
// must not reuse the external JSON wire struct by accident." Callers
// (meter_handler.go) are responsible for mapping their decoded domain
// reading into this shape — never for forwarding raw Kafka bytes here.
//
// EventTimeUnixMs and Seq are duplicated as top-level fields (rather than
// nested) specifically so set_latest_if_newer.lua can read them directly
// via cjson.decode without needing to know about any other field in this
// struct — that script must keep working even if the rest of this type
// changes shape later.
type MeterProjection struct {
	EventTimeUnixMs int64                    `json:"event_time_unix_ms"`
	Seq             int64                    `json:"seq"`
	GridID          string                   `json:"grid_id"`
	HouseID         string                   `json:"house_id"`
	MeterID         string                   `json:"meter_id"`
	DeviceClass     string                   `json:"device_class"`
	SolarKw         float64                  `json:"solar_kw"`
	ConsumptionKw   float64                  `json:"consumption_kw"`
	NetKw           float64                  `json:"net_kw"`
	StorageAssets   []StorageAssetProjection `json:"storage_assets,omitempty"`
}

// SetLatestIfNewer writes p to meter:{p.GridID}:{p.HouseID}:latest,
// applying newest-reading-wins semantics via set_latest_if_newer.lua: the
// write is skipped (applied=false) if the currently cached projection
// has an event_time_unix_ms/seq that is already >= p's. This is what
// makes an out-of-order or redelivered Kafka record safe to write
// blindly, without a separate read-then-compare round trip from the
// caller.
func (c *Client) SetLatestIfNewer(ctx context.Context, p MeterProjection) (applied bool, err error) {
	key := MeterLatestKey(p.GridID, p.HouseID)

	body, err := json.Marshal(p)
	if err != nil {
		return false, fmt.Errorf("redis: marshal meter projection for %s: %w", key, err)
	}

	result, err := c.setLatestIfNewer.Run(ctx, c.rdb, []string{key},
		p.EventTimeUnixMs,
		p.Seq,
		string(body),
	).Int()
	if err != nil {
		return false, fmt.Errorf("redis: set_latest_if_newer for %s: %w", key, err)
	}
	return result == 1, nil
}

// GetLatest returns the cached latest reading projection for a house.
// Returns ErrNotFound if nothing is cached yet — a normal state right
// after a Redis rebuild, before the first reading for this house has
// arrived and been reconciled.
func (c *Client) GetLatest(ctx context.Context, gridID, houseID string) (MeterProjection, error) {
	key := MeterLatestKey(gridID, houseID)

	raw, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return MeterProjection{}, ErrNotFound
	}
	if err != nil {
		return MeterProjection{}, fmt.Errorf("redis: get latest for %s: %w", key, err)
	}

	var p MeterProjection
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return MeterProjection{}, fmt.Errorf("redis: decode latest projection for %s: %w", key, err)
	}
	return p, nil
}
