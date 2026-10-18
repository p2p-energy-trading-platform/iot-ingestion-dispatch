package redis

import (
	"context"
	"fmt"
	"time"
)

type HouseStatus struct {
	Status          string
	LastHeartbeatAt time.Time
}

func (store *Store) UpdateHeartbeat(context context.Context, gridID, houseID string, status HouseStatus) error {
	return store.updateHeartbeatWithTTL(context, gridID, houseID, status, HeartbeatTTL)
}

// RestoreHeartbeat re-creates a house's status for the rebuild command. The
// key lives only for what is left of the liveness window, so a rebuild never
// makes a quiet house look more recently alive than it was. A house whose last
// heartbeat is already outside the window gets no status key at all.
func (store *Store) RestoreHeartbeat(context context.Context, gridID, houseID string, status HouseStatus, now time.Time) (restored bool, err error) {
	remaining := HeartbeatTTL - now.Sub(status.LastHeartbeatAt)
	if remaining < time.Second {
		return false, nil
	}
	if err := store.updateHeartbeatWithTTL(context, gridID, houseID, status, remaining); err != nil {
		return false, err
	}
	return true, nil
}

// AddHouseToGrid records membership only. SADD is idempotent.
func (store *Store) AddHouseToGrid(context context.Context, gridID, houseID string) error {
	key := GridHousesKey(gridID)
	if err := store.rdb.SAdd(context, key, houseID).Err(); err != nil {
		return fmt.Errorf("redis: sadd house %s to grid %s: %w", houseID, gridID, err)
	}
	return nil
}

func (store *Store) updateHeartbeatWithTTL(context context.Context, gridID, houseID string, status HouseStatus, ttl time.Duration) error {
	statusKey := HouseStatusKey(houseID)
	gridHousesKey := GridHousesKey(gridID)

	_, err := store.updateHeartbeat.Run(context, store.rdb,
		[]string{statusKey, gridHousesKey},
		int(ttl.Seconds()),
		status.Status,
		status.LastHeartbeatAt.Format(time.RFC3339),
		houseID,
	).Result()
	if err != nil {
		return fmt.Errorf("redis: update_heartbeat for house %s: %w", houseID, err)
	}
	return nil
}

func (store *Store) GetHouseStatus(context context.Context, houseID string) (HouseStatus, error) {
	key := HouseStatusKey(houseID)

	vals, err := store.rdb.HGetAll(context, key).Result()
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

func (store *Store) GridHouseIDs(context context.Context, gridID string) ([]string, error) {
	key := GridHousesKey(gridID)

	ids, err := store.rdb.SMembers(context, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: get grid houses for %s: %w", key, err)
	}
	return ids, nil
}

func (store *Store) RemoveHouseFromGrid(context context.Context, gridID, houseID string) error {
	key := GridHousesKey(gridID)

	if err := store.rdb.SRem(context, key, houseID).Err(); err != nil {
		return fmt.Errorf("redis: srem house %s from grid %s: %w", houseID, gridID, err)
	}
	return nil
}
