package domain

// FlexibleAsset is the decoded, validated form of one entry in a
// heartbeat's flexible_assets array.
type FlexibleAsset struct {
	AssetID        string
	AssetType      AssetType
	CapacityKwh    float64
	MaxChargeKw    float64
	MaxDischargeKw float64
	// V2GCapable is set only for AssetTypeEV entries - nil for bess.
	V2GCapable *bool
}

// Heartbeat is the decoded, validated internal representation of a
// HeartbeatPayload.
type Heartbeat struct {
	SchemaVersion  string
	GridID         string
	HouseID        string
	MeterID        string
	Status         string
	DeviceClass    DeviceClass
	RatedSolarKw   float64
	FlexibleAssets []FlexibleAsset
}
