package redis

import (
	"fmt"
	"time"
)

// HeartbeatTTL is the documented, team-confirmed TTL applied to a house's
// status hash (see 02-redis-hot-storage.md, "TTL behavior"). If no
// heartbeat refreshes house:{house_id}:status within this window, Redis
// expires the key and the house is treated as having gone quiet.
const HeartbeatTTL = 10 * time.Minute

// MeterLatestKey builds the STRING key holding the latest cached meter
// reading projection for a house: meter:{grid_id}:{house_id}:latest
// No TTL applies to this key — it is always overwritten, never expired.
func MeterLatestKey(gridID, houseID string) string {
	return fmt.Sprintf("meter:%s:%s:latest", gridID, houseID)
}

// HouseStatusKey builds the HASH key holding a house's heartbeat-derived
// status: house:{house_id}:status ("status", "last_heartbeat_at").
// Carries HeartbeatTTL, refreshed on every heartbeat.
func HouseStatusKey(houseID string) string {
	return fmt.Sprintf("house:%s:status", houseID)
}

// GridHousesKey builds the SET key holding every house_id that has ever
// reported for a grid: grid:{grid_id}:houses
// No TTL — grid membership must not silently expire just because a house
// has been quiet for a while.
func GridHousesKey(gridID string) string {
	return fmt.Sprintf("grid:%s:houses", gridID)
}
