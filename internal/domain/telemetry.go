package domain

import "time"

// DeviceClass mirrors the simulator's device_class enum.
type DeviceClass string

const (
	DeviceClassConsumer            DeviceClass = "consumer"
	DeviceClassResidentialProsumer DeviceClass = "residential_prosumer"
	DeviceClassCommercial          DeviceClass = "commercial"
)

// AssetType mirrors the simulator's storage/flexible asset_type enum.
type AssetType string

const (
	AssetTypeBESS AssetType = "bess"
	AssetTypeEV   AssetType = "ev"
)

// StorageAssetReading is the decoded, validated form of one entry in a
// meter reading's storage_assets array.
type StorageAssetReading struct {
	AssetID     string
	AssetType   AssetType
	SocPct      float64
	PowerKw     float64
	CapacityKwh float64
	// PluggedIn is set only for AssetTypeEV entries - nil for bess,
	// matching the wire payload's shape (the field is absent for bess).
	PluggedIn *bool
}

// MeterReading is the decoded, validated internal representation of a
// MeterReadingPayload. Deliberately distinct from the wire JSON struct
// (private to internal/ingestion/decoder.go) and from the Redis
// projection type (internal/store/redis.MeterProjection) - each layer
// keeps its own shape rather than reusing another layer's struct.
type MeterReading struct {
	SchemaVersion string
	MeterID       string
	HouseID       string
	GridID        string
	DeviceClass   DeviceClass
	EventTime     time.Time
	Seq           int64

	SolarKw       float64
	ConsumptionKw float64
	NetKw         float64
	StorageAssets []StorageAssetReading

	// WeatherIrradianceWm2 and CloudCoverPct are debug-only fields per
	// simulation_plan.md ("nothing downstream should depend on this
	// block existing") - both optional, may be nil.
	WeatherIrradianceWm2 *float64
	CloudCoverPct        *float64
}
