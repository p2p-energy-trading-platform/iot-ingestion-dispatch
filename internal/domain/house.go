package domain

// HouseRegistration is one house as provisioned by an administrator or, later,
// by the registration flow. It is the only way a house enters iot_data.houses:
// heartbeats never create or move houses (06-heartbeat-processing.md).
type HouseRegistration struct {
	HouseID      string
	GridID       string
	MeterID      string
	DeviceClass  DeviceClass
	RatedSolarKw *float64
}
