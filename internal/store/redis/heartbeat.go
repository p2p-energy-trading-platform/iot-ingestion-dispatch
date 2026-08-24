package redis

import (
	"context"
	"fmt"
	"time"
)

// HouseStatus is the two-field record cached at house:{house_id}:status,
// matching 02-redis-hot-storage.md exactly: "status" and
// "last_heartbeat_at". Nothing else belongs in this hash — nothing here
// duplicates the heartbeat wire payload's other fields.
type HouseStatus struct {
	Status          string
	LastHeartbeatAt time.Time
}

// UpdateHeartbeat atomically refreshes house:{houseID}:status (HSET +
// EXPIRE with HeartbeatTTL) and adds houseID to grid:{gridID}:houses
// (SADD), via update_heartbeat.lua. Both effects land together — a crash
// partway through can never leave a status hash without its TTL, or
// leave the grid's house set out of sync with a status update.
func (c *Client) UpdateHeartbeat(ctx context.Context, gridID, houseID string, status HouseStatus) error {
	statusKey := HouseStatusKey(houseID)
	gridHousesKey := GridHousesKey(gridID)

	_, err := c.updateHeartbeat.Run(ctx, c.rdb,
		[]string{statusKey, gridHousesKey},
		int(HeartbeatTTL.Seconds()),
		status.Status,
		status.LastHeartbeatAt.Format(time.RFC3339),
		houseID,
	).Result()
	if err != nil {
		return fmt.Errorf("redis: update_heartbeat for house %s: %w", houseID, err)
	}
	return nil
}

// GetHouseStatus reads the cached status hash for a house. Returns
// ErrNotFound if the house has never sent a heartbeat, or its TTL has
// expired — both cases mean the same thing to a caller: "not currently
// known to be online."
func (c *Client) GetHouseStatus(ctx context.Context, houseID string) (HouseStatus, error) {
	key := HouseStatusKey(houseID)

	vals, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return HouseStatus{}, fmt.Errorf("redis: get house status for %s: %w", key, err)
	}
	if len(vals) == 0 {
		return HouseStatus{}, ErrNotFound
	}

	lastHeartbeatAt, err := time.Parse(time.RFC3339, vals["last_heartbeat_at"])
	if err != nil {
		return HouseStatus{}, fmt.Errorf("redis: parse last_heartbeat_at for %s: %w", key, err)
	}

	return HouseStatus{
		Status:          vals["status"],
		LastHeartbeatAt: lastHeartbeatAt,
	}, nil
}

// GridHouseIDs returns every house_id ever registered against a grid's
// house set. This set is NOT expired by HeartbeatTTL — membership means
// "has reported at least once for this grid," not "is currently online."
// Use GetHouseStatus per house_id for current online/offline state.
func (c *Client) GridHouseIDs(ctx context.Context, gridID string) ([]string, error) {
	key := GridHousesKey(gridID)

	ids, err := c.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: get grid houses for %s: %w", key, err)
	}
	return ids, nil
}

// RemoveHouseFromGrid removes houseID from grid:{gridID}:houses (SREM).
// Not part of update_heartbeat.lua — heartbeats only ever add membership.
// This exists as a separate, deliberate operation for whatever
// administrative/decommissioning path needs to retract a house later;
// per 02-redis-hot-storage.md, grid membership is explicitly NOT
// something that should silently expire on its own.
func (c *Client) RemoveHouseFromGrid(ctx context.Context, gridID, houseID string) error {
	key := GridHousesKey(gridID)

	if err := c.rdb.SRem(ctx, key, houseID).Err(); err != nil {
		return fmt.Errorf("redis: srem house %s from grid %s: %w", houseID, gridID, err)
	}
	return nil
}
