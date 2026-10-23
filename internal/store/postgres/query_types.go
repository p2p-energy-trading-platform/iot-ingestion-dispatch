package postgres

import "time"

// HouseInfoRow is a registered house as returned by the read RPCs. It is
// distinct from HouseRow, which carries only what the Redis rebuild needs.
type HouseInfoRow struct {
	HouseID         string
	GridID          string
	GridDisplayName string
	MeterID         string
	DeviceClass     string
	RatedSolarKw    *float64
	OwnerUserID     *string
}

type FlexibleAssetRow struct {
	AssetID        string
	AssetType      string
	CapacityKwh    float64
	MaxChargeKw    float64
	MaxDischargeKw float64
	V2GCapable     *bool
}

// HouseFilter selects houses for ListHouses. Empty strings mean "any";
// AfterHouseID and Limit give keyset paging in house_id order.
type HouseFilter struct {
	OwnerUserID  string
	GridID       string
	AfterHouseID string
	Limit        int
}

type EnergyTotals struct {
	GeneratedKwh         float64
	ConsumedKwh          float64
	ExportedKwh          float64
	ImportedKwh          float64
	StorageChargedKwh    float64
	StorageDischargedKwh float64
}

type EnergyFlows struct {
	SolarToHomeKwh    float64
	SolarToStorageKwh float64
	SolarToGridKwh    float64
	GridToHomeKwh     float64
	GridToStorageKwh  float64
	StorageToHomeKwh  float64
	StorageToGridKwh  float64
}

// EnergyBucketRow is one time bucket of a series. CoveredHours is the total
// time the bucket's readings account for, so averages ignore meter gaps.
type EnergyBucketRow struct {
	Start        time.Time
	Energy       EnergyTotals
	CoveredHours float64
}

type EnergySummaryRow struct {
	Totals            EnergyTotals
	Flows             EnergyFlows
	PeakConsumptionKw *float64
	PeakTime          *time.Time
}
