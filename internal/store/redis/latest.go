package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

var ErrNotFound = errors.New("redis: not found")

type StorageAssetProjection struct {
	AssetID     string  `json:"asset_id"`
	AssetType   string  `json:"asset_type"`
	SocPct      float64 `json:"soc_pct"`
	PowerKw     float64 `json:"power_kw"`
	CapacityKwh float64 `json:"capacity_kwh"`
	PluggedIn   *bool   `json:"plugged_in,omitempty"`
}

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

func (store *Store) SetLatestIfNewer(context context.Context, p MeterProjection) (applied bool, err error) {
	key := MeterLatestKey(p.GridID, p.HouseID)

	body, err := json.Marshal(p)
	if err != nil {
		return false, fmt.Errorf("redis: marshal meter projection for %s: %w", key, err)
	}

	result, err := store.setLatestIfNewer.Run(context, store.rdb, []string{key},
		p.EventTimeUnixMs,
		p.Seq,
		string(body),
	).Int()
	if err != nil {
		return false, fmt.Errorf("redis: set_latest_if_newer for %s: %w", key, err)
	}
	return result == 1, nil
}

func (store *Store) GetLatest(context context.Context, gridID, houseID string) (MeterProjection, error) {
	key := MeterLatestKey(gridID, houseID)

	raw, err := store.rdb.Get(context, key).Result()
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

// ProjectionOf maps a domain reading to its Redis projection. The projection
// is a separate shape from both the wire JSON and the domain type.
func ProjectionOf(r domain.MeterReading) MeterProjection {
	p := MeterProjection{
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
		p.StorageAssets = append(p.StorageAssets, StorageAssetProjection{
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
