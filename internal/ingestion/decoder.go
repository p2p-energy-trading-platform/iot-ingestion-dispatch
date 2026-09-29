// Package ingestion's decoder.go is the single point of contact with the
// simulator's raw JSON wire format. Kafka Connect's MQTT source connector
// uses ByteArrayConverter, forwarding the simulator's raw JSON unchanged
// (confirmed against the running system, not assumed) - so decoding here
// is plain encoding/json, always. Generated protobuf types (go-sdk) are
// reserved for the internal gRPC API only and MUST NOT appear in this
// file or be used as a Kafka ingestion model.
package ingestion

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const supportedSchemaVersion = "1.0"

// Clock-skew tolerance for a reading's reported timestamp, checked
// against wall-clock time at decode time. Generous on both sides
// deliberately - this catches a genuinely broken device clock, not
// normal network/processing lag. Not a substitute for the
// (time, meter_id, seq) idempotent PK, which is what actually protects
// against legitimate at-least-once Kafka replay of an already-valid,
// older record.
const (
	maxClockSkewFuture = 5 * time.Minute
	maxClockSkewPast   = 24 * time.Hour

	maxGridIDLen  = 32
	maxAssetIDLen = 64
	maxHouseNum   = 10 // max digits in the {N} of {grid_id}-house{N}
)

// --- private wire structs: JSON shape exactly as the simulator sends it ---
//
// Pointer fields below are deliberate, not incidental - see the PR
// description for the full field-by-field justification. In short: every
// pointer here exists because 0 is a legitimate value for that field, so
// only a pointer can distinguish "field present with value 0" from
// "field missing entirely." capacity_kwh does NOT need one, since our
// validation already rejects <= 0, so "missing" and "invalid zero"
// collapse into the same outcome - see decodeStorageAssets/
// decodeFlexibleAssets.

type meterReadingWire struct {
	SchemaVersion string             `json:"schema_version"`
	MeterID       string             `json:"meter_id"`
	HouseID       string             `json:"house_id"`
	GridID        string             `json:"grid_id"`
	DeviceClass   string             `json:"device_class"`
	Timestamp     string             `json:"timestamp"`
	Seq           *int64             `json:"seq"`
	Readings      *meterReadingsWire `json:"readings"`
	Meta          *meterMetaWire     `json:"meta"`
}

type meterReadingsWire struct {
	SolarKw       *float64           `json:"solar_kw"`
	ConsumptionKw *float64           `json:"consumption_kw"`
	NetKw         *float64           `json:"net_kw"`
	StorageAssets []storageAssetWire `json:"storage_assets"`
}

type storageAssetWire struct {
	AssetID     string   `json:"asset_id"`
	AssetType   string   `json:"asset_type"`
	SocPct      *float64 `json:"soc_pct"`
	PowerKw     *float64 `json:"power_kw"`
	CapacityKwh float64  `json:"capacity_kwh"` // no pointer needed: must be > 0, so missing (0) and invalid (0) reject identically
	PluggedIn   *bool    `json:"plugged_in,omitempty"`
}

type meterMetaWire struct {
	WeatherIrradianceWm2 *float64 `json:"weather_irradiance_wm2"`
	CloudCoverPct        *float64 `json:"cloud_cover_pct"`
}

type heartbeatWire struct {
	SchemaVersion  string              `json:"schema_version"`
	GridID         string              `json:"grid_id"`
	HouseID        string              `json:"house_id"`
	MeterID        string              `json:"meter_id"`
	Status         string              `json:"status"`
	DeviceClass    string              `json:"device_class"`
	RatedSolarKw   *float64            `json:"rated_solar_kw"`
	FlexibleAssets []flexibleAssetWire `json:"flexible_assets"`
}

type flexibleAssetWire struct {
	AssetID        string   `json:"asset_id"`
	AssetType      string   `json:"asset_type"`
	CapacityKwh    float64  `json:"capacity_kwh"` // see storageAssetWire.CapacityKwh
	MaxChargeKw    *float64 `json:"max_charge_kw"`
	MaxDischargeKw *float64 `json:"max_discharge_kw"`
	V2GCapable     *bool    `json:"v2g_capable,omitempty"`
}

// DecodeMeterReading decodes and fully validates raw Kafka record bytes
// from the iot.meter-readings topic into a domain.MeterReading. All
// validation happens before any field is copied into the returned value
// - callers must never act on a partially-valid result, and none is ever
// returned alongside an error.
func DecodeMeterReading(raw []byte) (domain.MeterReading, error) {
	var wire meterReadingWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return domain.MeterReading{}, domain.NewValidationError("payload", fmt.Sprintf("invalid JSON: %v", err))
	}

	if err := validateSchemaVersion(wire.SchemaVersion); err != nil {
		return domain.MeterReading{}, err
	}
	if err := validateGridID(wire.GridID); err != nil {
		return domain.MeterReading{}, err
	}
	if err := validateHouseID(wire.HouseID, wire.GridID); err != nil {
		return domain.MeterReading{}, err
	}
	if err := validateMeterID(wire.MeterID, wire.HouseID); err != nil {
		return domain.MeterReading{}, err
	}
	deviceClass, err := validateDeviceClass(wire.DeviceClass)
	if err != nil {
		return domain.MeterReading{}, err
	}

	if wire.Timestamp == "" {
		return domain.MeterReading{}, domain.NewValidationError("timestamp", "required")
	}
	eventTime, err := time.Parse(time.RFC3339, wire.Timestamp)
	if err != nil {
		return domain.MeterReading{}, domain.NewValidationError("timestamp", fmt.Sprintf("not RFC3339: %v", err))
	}
	if err := validateClockSkew(eventTime); err != nil {
		return domain.MeterReading{}, err
	}

	seq, err := requireNonNegativeInt("seq", wire.Seq)
	if err != nil {
		return domain.MeterReading{}, err
	}

	if wire.Readings == nil {
		return domain.MeterReading{}, domain.NewValidationError("readings", "required")
	}

	solarKw, err := requireNonNegativeFinite("readings.solar_kw", wire.Readings.SolarKw)
	if err != nil {
		return domain.MeterReading{}, err
	}
	consumptionKw, err := requireNonNegativeFinite("readings.consumption_kw", wire.Readings.ConsumptionKw)
	if err != nil {
		return domain.MeterReading{}, err
	}
	netKw, err := requireFinite("readings.net_kw", wire.Readings.NetKw)
	if err != nil {
		return domain.MeterReading{}, err
	}

	storageAssets, err := decodeStorageAssets(wire.Readings.StorageAssets)
	if err != nil {
		return domain.MeterReading{}, err
	}

	// A stricter net_kw reconciliation against simulation_plan.md
	// section 5.4's formula was tried and removed: the doc's own worked
	// example (solar=2.41, consumption=0.87, two charging batteries,
	// net_kw=1.54) does not satisfy that formula (would require ~3.34).
	// Section 5.4's formula likely describes convergence specifically
	// after a dispatch/actuation command, not the general steady-state
	// case - but that's not confirmed. net_kw is only checked for
	// finiteness, not cross-field correctness, until this is clarified.

	var weatherIrradiance, cloudCover *float64
	if wire.Meta != nil {
		weatherIrradiance, err = optionalNonNegativeFinite("meta.weather_irradiance_wm2", wire.Meta.WeatherIrradianceWm2)
		if err != nil {
			return domain.MeterReading{}, err
		}
		cloudCover, err = optionalPercent("meta.cloud_cover_pct", wire.Meta.CloudCoverPct)
		if err != nil {
			return domain.MeterReading{}, err
		}
	}

	return domain.MeterReading{
		SchemaVersion:        wire.SchemaVersion,
		MeterID:              wire.MeterID,
		HouseID:              wire.HouseID,
		GridID:               wire.GridID,
		DeviceClass:          deviceClass,
		EventTime:            eventTime,
		Seq:                  seq,
		SolarKw:              solarKw,
		ConsumptionKw:        consumptionKw,
		NetKw:                netKw,
		StorageAssets:        storageAssets,
		WeatherIrradianceWm2: weatherIrradiance,
		CloudCoverPct:        cloudCover,
	}, nil
}

func decodeStorageAssets(wireAssets []storageAssetWire) ([]domain.StorageAssetReading, error) {
	seen := make(map[string]bool, len(wireAssets))
	assets := make([]domain.StorageAssetReading, 0, len(wireAssets))

	for i, a := range wireAssets {
		field := fmt.Sprintf("readings.storage_assets[%d]", i)

		if err := validateAssetID(field+".asset_id", a.AssetID); err != nil {
			return nil, err
		}
		if seen[a.AssetID] {
			return nil, domain.NewValidationError(field+".asset_id", "duplicate asset_id in storage_assets")
		}
		seen[a.AssetID] = true

		assetType, err := validateAssetType(field+".asset_type", a.AssetType)
		if err != nil {
			return nil, err
		}

		socPct, err := requirePercent(field+".soc_pct", a.SocPct)
		if err != nil {
			return nil, err
		}
		powerKw, err := requireFinite(field+".power_kw", a.PowerKw)
		if err != nil {
			return nil, err
		}
		if err := validateFinite(field+".capacity_kwh", a.CapacityKwh); err != nil {
			return nil, err
		}
		if a.CapacityKwh <= 0 {
			return nil, domain.NewValidationError(field+".capacity_kwh", "required, must be > 0")
		}

		if err := validateCapabilityField(field+".plugged_in", assetType, a.PluggedIn != nil); err != nil {
			return nil, err
		}

		// Reallocate rather than reuse the wire struct's pointer, so the
		// domain type never holds memory owned by a temporary decode
		// struct.
		var pluggedIn *bool
		if a.PluggedIn != nil {
			v := *a.PluggedIn
			pluggedIn = &v
		}

		assets = append(assets, domain.StorageAssetReading{
			AssetID:     a.AssetID,
			AssetType:   assetType,
			SocPct:      socPct,
			PowerKw:     powerKw,
			CapacityKwh: a.CapacityKwh,
			PluggedIn:   pluggedIn,
		})
	}

	return assets, nil
}

// DecodeHeartbeat decodes and fully validates raw Kafka record bytes from
// the iot.heartbeats topic into a domain.Heartbeat.
func DecodeHeartbeat(raw []byte) (domain.Heartbeat, error) {
	var wire heartbeatWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return domain.Heartbeat{}, domain.NewValidationError("payload", fmt.Sprintf("invalid JSON: %v", err))
	}

	if err := validateSchemaVersion(wire.SchemaVersion); err != nil {
		return domain.Heartbeat{}, err
	}
	if err := validateGridID(wire.GridID); err != nil {
		return domain.Heartbeat{}, err
	}
	if err := validateHouseID(wire.HouseID, wire.GridID); err != nil {
		return domain.Heartbeat{}, err
	}
	if err := validateMeterID(wire.MeterID, wire.HouseID); err != nil {
		return domain.Heartbeat{}, err
	}

	if wire.Status == "" {
		return domain.Heartbeat{}, domain.NewValidationError("status", "required")
	}
	// Only "online" is documented/observed on the wire today. Anything
	// else is rejected rather than silently accepted, so a future new
	// status value gets a deliberate decision (widen this check) instead
	// of silently flowing through unvalidated.
	if wire.Status != "online" {
		return domain.Heartbeat{}, domain.NewValidationError("status", fmt.Sprintf("unrecognized value %q", wire.Status))
	}

	deviceClass, err := validateDeviceClass(wire.DeviceClass)
	if err != nil {
		return domain.Heartbeat{}, err
	}

	ratedSolarKw, err := requireNonNegativeFinite("rated_solar_kw", wire.RatedSolarKw)
	if err != nil {
		return domain.Heartbeat{}, err
	}

	flexibleAssets, err := decodeFlexibleAssets(wire.FlexibleAssets)
	if err != nil {
		return domain.Heartbeat{}, err
	}

	return domain.Heartbeat{
		SchemaVersion:  wire.SchemaVersion,
		GridID:         wire.GridID,
		HouseID:        wire.HouseID,
		MeterID:        wire.MeterID,
		Status:         wire.Status,
		DeviceClass:    deviceClass,
		RatedSolarKw:   ratedSolarKw,
		FlexibleAssets: flexibleAssets,
	}, nil
}

func decodeFlexibleAssets(wireAssets []flexibleAssetWire) ([]domain.FlexibleAsset, error) {
	seen := make(map[string]bool, len(wireAssets))
	assets := make([]domain.FlexibleAsset, 0, len(wireAssets))

	for i, a := range wireAssets {
		field := fmt.Sprintf("flexible_assets[%d]", i)

		if err := validateAssetID(field+".asset_id", a.AssetID); err != nil {
			return nil, err
		}
		if seen[a.AssetID] {
			return nil, domain.NewValidationError(field+".asset_id", "duplicate asset_id in flexible_assets")
		}
		seen[a.AssetID] = true

		assetType, err := validateAssetType(field+".asset_type", a.AssetType)
		if err != nil {
			return nil, err
		}

		if err := validateFinite(field+".capacity_kwh", a.CapacityKwh); err != nil {
			return nil, err
		}
		if a.CapacityKwh <= 0 {
			return nil, domain.NewValidationError(field+".capacity_kwh", "required, must be > 0")
		}

		maxChargeKw, err := requireNonNegativeFinite(field+".max_charge_kw", a.MaxChargeKw)
		if err != nil {
			return nil, err
		}
		maxDischargeKw, err := requireNonNegativeFinite(field+".max_discharge_kw", a.MaxDischargeKw)
		if err != nil {
			return nil, err
		}

		if err := validateCapabilityField(field+".v2g_capable", assetType, a.V2GCapable != nil); err != nil {
			return nil, err
		}

		var v2gCapable *bool
		if a.V2GCapable != nil {
			v := *a.V2GCapable
			v2gCapable = &v
		}

		assets = append(assets, domain.FlexibleAsset{
			AssetID:        a.AssetID,
			AssetType:      assetType,
			CapacityKwh:    a.CapacityKwh,
			MaxChargeKw:    maxChargeKw,
			MaxDischargeKw: maxDischargeKw,
			V2GCapable:     v2gCapable,
		})
	}

	return assets, nil
}

// --- shared field validators ---
// Each validator owns its own "required" check - callers never need a
// separate empty-string guard before calling one of these.

func validateSchemaVersion(v string) error {
	if v == "" {
		return domain.NewValidationError("schema_version", "required")
	}
	if v != supportedSchemaVersion {
		return domain.NewValidationError("schema_version", fmt.Sprintf("unsupported version %q (expected %q)", v, supportedSchemaVersion))
	}
	return nil
}

func validateGridID(gridID string) error {
	if gridID == "" {
		return domain.NewValidationError("grid_id", "required")
	}
	if len(gridID) > maxGridIDLen {
		return domain.NewValidationError("grid_id", fmt.Sprintf("must be at most %d characters", maxGridIDLen))
	}
	for i := 0; i < len(gridID); i++ {
		if !isLowerAlnum(gridID[i]) {
			return domain.NewValidationError("grid_id", "must be lowercase alphanumeric")
		}
	}
	return nil
}

func validateHouseID(houseID, gridID string) error {
	if houseID == "" {
		return domain.NewValidationError("house_id", "required")
	}
	// gridID is validated separately (validateGridID); here we only
	// check houseID's own shape relative to it. Slicing below never
	// allocates - a string slice is just a new header over the same
	// bytes.
	if !strings.HasPrefix(houseID, gridID) || len(houseID) <= len(gridID) || houseID[len(gridID)] != '-' {
		return domain.NewValidationError("house_id", "must start with grid_id prefix followed by '-'")
	}
	suffix := houseID[len(gridID)+1:]
	if !strings.HasPrefix(suffix, "house") {
		return domain.NewValidationError("house_id", "must match {grid_id}-house{N}")
	}
	digits := suffix[len("house"):]
	if digits == "" || len(digits) > maxHouseNum {
		return domain.NewValidationError("house_id", "must match {grid_id}-house{N}")
	}
	for i := 0; i < len(digits); i++ {
		if !isDigit(digits[i]) {
			return domain.NewValidationError("house_id", "must match {grid_id}-house{N}")
		}
	}
	return nil
}

func validateMeterID(meterID, houseID string) error {
	if meterID == "" {
		return domain.NewValidationError("meter_id", "required")
	}
	const prefix = "meter-"
	if !strings.HasPrefix(meterID, prefix) || meterID[len(prefix):] != houseID {
		return domain.NewValidationError("meter_id", `must equal "meter-"+house_id`)
	}
	return nil
}

func validateAssetID(field, assetID string) error {
	if assetID == "" {
		return domain.NewValidationError(field, "required")
	}
	if len(assetID) > maxAssetIDLen {
		return domain.NewValidationError(field, fmt.Sprintf("must be at most %d characters", maxAssetIDLen))
	}
	for i := 0; i < len(assetID); i++ {
		if !isAssetIDChar(assetID[i]) {
			return domain.NewValidationError(field, "invalid characters")
		}
	}
	return nil
}

func validateDeviceClass(v string) (domain.DeviceClass, error) {
	if v == "" {
		return "", domain.NewValidationError("device_class", "required")
	}
	switch domain.DeviceClass(v) {
	case domain.DeviceClassConsumer, domain.DeviceClassResidentialProsumer, domain.DeviceClassCommercial:
		return domain.DeviceClass(v), nil
	default:
		return "", domain.NewValidationError("device_class", fmt.Sprintf("unrecognized value %q", v))
	}
}

func validateAssetType(field, v string) (domain.AssetType, error) {
	if v == "" {
		return "", domain.NewValidationError(field, "required")
	}
	switch domain.AssetType(v) {
	case domain.AssetTypeBESS, domain.AssetTypeEV:
		return domain.AssetType(v), nil
	default:
		return "", domain.NewValidationError(field, fmt.Sprintf("unrecognized value %q", v))
	}
}

// validateCapabilityField enforces that an EV-only capability field
// (plugged_in on a storage_asset reading, v2g_capable on a heartbeat
// flexible_asset) is present if and only if assetType is EV - present on
// a bess entry is rejected as a schema violation, not silently ignored.
func validateCapabilityField(field string, assetType domain.AssetType, present bool) error {
	switch assetType {
	case domain.AssetTypeEV:
		if !present {
			return domain.NewValidationError(field, "required for asset_type ev")
		}
	case domain.AssetTypeBESS:
		if present {
			return domain.NewValidationError(field, "must not be present for asset_type bess")
		}
	}
	return nil
}

func validateFinite(field string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return domain.NewValidationError(field, "must be a finite number")
	}
	return nil
}

func validateClockSkew(eventTime time.Time) error {
	now := time.Now().UTC()
	if eventTime.After(now.Add(maxClockSkewFuture)) {
		return domain.NewValidationError("timestamp", fmt.Sprintf("too far in the future (max skew %s)", maxClockSkewFuture))
	}
	if eventTime.Before(now.Add(-maxClockSkewPast)) {
		return domain.NewValidationError("timestamp", fmt.Sprintf("too far in the past (max skew %s)", maxClockSkewPast))
	}
	return nil
}

// --- required/optional numeric helpers ---
// Each pairs presence-checking with the finite/range check, so call
// sites collapse from several lines to one.

func requireFinite(field string, v *float64) (float64, error) {
	if v == nil {
		return 0, domain.NewValidationError(field, "required")
	}
	if err := validateFinite(field, *v); err != nil {
		return 0, err
	}
	return *v, nil
}

func requireNonNegativeFinite(field string, v *float64) (float64, error) {
	val, err := requireFinite(field, v)
	if err != nil {
		return 0, err
	}
	if val < 0 {
		return 0, domain.NewValidationError(field, "must be >= 0")
	}
	return val, nil
}

func requirePercent(field string, v *float64) (float64, error) {
	val, err := requireFinite(field, v)
	if err != nil {
		return 0, err
	}
	if val < 0 || val > 100 {
		return 0, domain.NewValidationError(field, "must be between 0 and 100")
	}
	return val, nil
}

func requireNonNegativeInt(field string, v *int64) (int64, error) {
	if v == nil {
		return 0, domain.NewValidationError(field, "required")
	}
	if *v < 0 {
		return 0, domain.NewValidationError(field, "must be >= 0")
	}
	return *v, nil
}

func optionalNonNegativeFinite(field string, v *float64) (*float64, error) {
	if v == nil {
		return nil, nil
	}
	val, err := requireNonNegativeFinite(field, v)
	if err != nil {
		return nil, err
	}
	return &val, nil // &val addresses this function's own local copy, not v
}

func optionalPercent(field string, v *float64) (*float64, error) {
	if v == nil {
		return nil, nil
	}
	val, err := requirePercent(field, v)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// --- byte-level character checks (no regexp) ---

func isLowerAlnum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isAssetIDChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}
