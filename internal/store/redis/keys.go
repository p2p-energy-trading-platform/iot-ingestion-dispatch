package redis

import (
	"fmt"
	"time"
)

const HeartbeatTTL = 10 * time.Minute

func MeterLatestKey(gridID, houseID string) string {
	return fmt.Sprintf("meter:%s:%s:latest", gridID, houseID)
}

func HouseStatusKey(houseID string) string {
	return fmt.Sprintf("house:%s:status", houseID)
}

func GridHousesKey(gridID string) string {
	return fmt.Sprintf("grid:%s:houses", gridID)
}
