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
	"regexp"
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
)

var (
	gridIDPattern = regexp.MustCompile(`^[a-z0-9]{1,32}$`)
	houseIDSuffix = regexp.MustCompile(`^house[0-9]{1,10}$`)
	assetIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
)

// --- private wire structs: JSON shape exactly as the simulator sends it ---

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
	CapacityKwh *float64 `json:"capacity_kwh"`
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
	CapacityKwh    *float64 `json:"capacity_kwh"`
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

	if wire.GridID == "" {
		return domain.MeterReading{}, domain.NewValidationError("grid_id", "required")
	}
	if err := validateGridID(wire.GridID); err != nil {
		return domain.MeterReading{}, err
	}

	if wire.HouseID == "" {
		return domain.MeterReading{}, domain.NewValidationError("house_id", "required")
	}
	if err := validateHouseID(wire.HouseID, wire.GridID); err != nil {
		return domain.MeterReading{}, err
	}

	if wire.MeterID == "" {
		return domain.MeterReading{}, domain.NewValidationError("meter_id", "required")
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

	if wire.Seq == nil {
		return domain.MeterReading{}, domain.NewValidationError("seq", "required")
	}
	if *wire.Seq < 0 {
		return domain.MeterReading{}, domain.NewValidationError("seq", "must be >= 0")
	}

	if wire.Readings == nil {
		return domain.MeterReading{}, domain.NewValidationError("readings", "required")
	}
	if wire.Readings.SolarKw == nil {
		return domain.MeterReading{}, domain.NewValidationError("readings.solar_kw", "required")
	}
	if err := validateFinite("readings.solar_kw", *wire.Readings.SolarKw); err != nil {
		return domain.MeterReading{}, err
	}
	if *wire.Readings.SolarKw < 0 {
		return domain.MeterReading{}, domain.NewValidationError("readings.solar_kw", "must be >= 0")
	}

	if wire.Readings.ConsumptionKw == nil {
		return domain.MeterReading{}, domain.NewValidationError("readings.consumption_kw", "required")
	}
	if err := validateFinite("readings.consumption_kw", *wire.Readings.ConsumptionKw); err != nil {
		return domain.MeterReading{}, err
	}
	if *wire.Readings.ConsumptionKw < 0 {
		return domain.MeterReading{}, domain.NewValidationError("readings.consumption_kw", "must be >= 0")
	}

	if wire.Readings.NetKw == nil {
		return domain.MeterReading{}, domain.NewValidationError("readings.net_kw", "required")
	}
	if err := validateFinite("readings.net_kw", *wire.Readings.NetKw); err != nil {
		return domain.MeterReading{}, err
	}

	storageAssets, err := decodeStorageAssets(wire.Readings.StorageAssets)
	if err != nil {
		return domain.MeterReading{}, err
	}

	// net_kw reconciliation against simulation_plan.md section 5.4's
	// documented formula:
	//   net_kw = solar_kw - consumption_kw
	//          - sum(power_kw for charging assets, power_kw < 0)
	//          + sum(power_kw for discharging assets, power_kw > 0)
	// A generous epsilon absorbs floating-point noise from the
	// simulator's own rounding. Genuine cross-field sanity check, not
	// just a bounds check - loosen or remove if real traffic trips false
	// positives.
	expectedNet := *wire.Readings.SolarKw - *wire.Readings.ConsumptionKw
	for _, asset := range storageAssets {
		if asset.PowerKw < 0 {
			expectedNet -= asset.PowerKw
		} else {
			expectedNet += asset.PowerKw
		}
	}
	const netKwEpsilon = 0.05
	if math.Abs(*wire.Readings.NetKw-expectedNet) > netKwEpsilon {
		return domain.MeterReading{}, domain.NewValidationError(
			"readings.net_kw",
			fmt.Sprintf("does not reconcile with solar/consumption/storage: got %.3f, expected ~%.3f", *wire.Readings.NetKw, expectedNet),
		)
	}

	var weatherIrradiance, cloudCover *float64
	if wire.Meta != nil {
		if wire.Meta.WeatherIrradianceWm2 != nil {
			if err := validateFinite("meta.weather_irradiance_wm2", *wire.Meta.WeatherIrradianceWm2); err != nil {
				return domain.MeterReading{}, err
			}
			if *wire.Meta.WeatherIrradianceWm2 < 0 {
				return domain.MeterReading{}, domain.NewValidationError("meta.weather_irradiance_wm2", "must be >= 0")
			}
			weatherIrradiance = wire.Meta.WeatherIrradianceWm2
		}
		if wire.Meta.CloudCoverPct != nil {
			if err := validatePercent("meta.cloud_cover_pct", *wire.Meta.CloudCoverPct); err != nil {
				return domain.MeterReading{}, err
			}
			cloudCover = wire.Meta.CloudCoverPct
		}
	}

	return domain.MeterReading{
		SchemaVersion:        wire.SchemaVersion,
		MeterID:              wire.MeterID,
		HouseID:              wire.HouseID,
		GridID:               wire.GridID,
		DeviceClass:          deviceClass,
		EventTime:            eventTime,
		Seq:                  *wire.Seq,
		SolarKw:              *wire.Readings.SolarKw,
		ConsumptionKw:        *wire.Readings.ConsumptionKw,
		NetKw:                *wire.Readings.NetKw,
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

		if a.AssetID == "" {
			return nil, domain.NewValidationError(field+".asset_id", "required")
		}
		if !assetIDPattern.MatchString(a.AssetID) {
			return nil, domain.NewValidationError(field+".asset_id", "invalid characters")
		}
		if seen[a.AssetID] {
			return nil, domain.NewValidationError(field+".asset_id", "duplicate asset_id in storage_assets")
		}
		seen[a.AssetID] = true

		assetType, err := validateAssetType(field+".asset_type", a.AssetType)
		if err != nil {
			return nil, err
		}

		if a.SocPct == nil {
			return nil, domain.NewValidationError(field+".soc_pct", "required")
		}
		if err := validatePercent(field+".soc_pct", *a.SocPct); err != nil {
			return nil, err
		}

		if a.PowerKw == nil {
			return nil, domain.NewValidationError(field+".power_kw", "required")
		}
		if err := validateFinite(field+".power_kw", *a.PowerKw); err != nil {
			return nil, err
		}

		if a.CapacityKwh == nil {
			return nil, domain.NewValidationError(field+".capacity_kwh", "required")
		}
		if err := validateFinite(field+".capacity_kwh", *a.CapacityKwh); err != nil {
			return nil, err
		}
		if *a.CapacityKwh <= 0 {
			return nil, domain.NewValidationError(field+".capacity_kwh", "must be > 0")
		}

		if err := validateCapabilityField(field+".plugged_in", assetType, a.PluggedIn != nil); err != nil {
			return nil, err
		}

		assets = append(assets, domain.StorageAssetReading{
			AssetID:     a.AssetID,
			AssetType:   assetType,
			SocPct:      *a.SocPct,
			PowerKw:     *a.PowerKw,
			CapacityKwh: *a.CapacityKwh,
			PluggedIn:   a.PluggedIn,
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

	if wire.GridID == "" {
		return domain.Heartbeat{}, domain.NewValidationError("grid_id", "required")
	}
	if err := validateGridID(wire.GridID); err != nil {
		return domain.Heartbeat{}, err
	}

	if wire.HouseID == "" {
		return domain.Heartbeat{}, domain.NewValidationError("house_id", "required")
	}
	if err := validateHouseID(wire.HouseID, wire.GridID); err != nil {
		return domain.Heartbeat{}, err
	}

	if wire.MeterID == "" {
		return domain.Heartbeat{}, domain.NewValidationError("meter_id", "required")
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

	if wire.RatedSolarKw == nil {
		return domain.Heartbeat{}, domain.NewValidationError("rated_solar_kw", "required")
	}
	if err := validateFinite("rated_solar_kw", *wire.RatedSolarKw); err != nil {
		return domain.Heartbeat{}, err
	}
	if *wire.RatedSolarKw < 0 {
		return domain.Heartbeat{}, domain.NewValidationError("rated_solar_kw", "must be >= 0")
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
		RatedSolarKw:   *wire.RatedSolarKw,
		FlexibleAssets: flexibleAssets,
	}, nil
}

func decodeFlexibleAssets(wireAssets []flexibleAssetWire) ([]domain.FlexibleAsset, error) {
	seen := make(map[string]bool, len(wireAssets))
	assets := make([]domain.FlexibleAsset, 0, len(wireAssets))

	for i, a := range wireAssets {
		field := fmt.Sprintf("flexible_assets[%d]", i)

		if a.AssetID == "" {
			return nil, domain.NewValidationError(field+".asset_id", "required")
		}
		if !assetIDPattern.MatchString(a.AssetID) {
			return nil, domain.NewValidationError(field+".asset_id", "invalid characters")
		}
		if seen[a.AssetID] {
			return nil, domain.NewValidationError(field+".asset_id", "duplicate asset_id in flexible_assets")
		}
		seen[a.AssetID] = true

		assetType, err := validateAssetType(field+".asset_type", a.AssetType)
		if err != nil {
			return nil, err
		}

		if a.CapacityKwh == nil {
			return nil, domain.NewValidationError(field+".capacity_kwh", "required")
		}
		if err := validateFinite(field+".capacity_kwh", *a.CapacityKwh); err != nil {
			return nil, err
		}
		if *a.CapacityKwh <= 0 {
			return nil, domain.NewValidationError(field+".capacity_kwh", "must be > 0")
		}

		if a.MaxChargeKw == nil {
			return nil, domain.NewValidationError(field+".max_charge_kw", "required")
		}
		if err := validateFinite(field+".max_charge_kw", *a.MaxChargeKw); err != nil {
			return nil, err
		}
		if *a.MaxChargeKw < 0 {
			return nil, domain.NewValidationError(field+".max_charge_kw", "must be >= 0")
		}

		if a.MaxDischargeKw == nil {
			return nil, domain.NewValidationError(field+".max_discharge_kw", "required")
		}
		if err := validateFinite(field+".max_discharge_kw", *a.MaxDischargeKw); err != nil {
			return nil, err
		}
		if *a.MaxDischargeKw < 0 {
			return nil, domain.NewValidationError(field+".max_discharge_kw", "must be >= 0")
		}

		if err := validateCapabilityField(field+".v2g_capable", assetType, a.V2GCapable != nil); err != nil {
			return nil, err
		}

		assets = append(assets, domain.FlexibleAsset{
			AssetID:        a.AssetID,
			AssetType:      assetType,
			CapacityKwh:    *a.CapacityKwh,
			MaxChargeKw:    *a.MaxChargeKw,
			MaxDischargeKw: *a.MaxDischargeKw,
			V2GCapable:     a.V2GCapable,
		})
	}

	return assets, nil
}

// --- shared field validators ---

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
	if !gridIDPattern.MatchString(gridID) {
		return domain.NewValidationError("grid_id", "must be lowercase alphanumeric")
	}
	return nil
}

func validateHouseID(houseID, gridID string) error {
	prefix := gridID + "-"
	if !strings.HasPrefix(houseID, prefix) {
		return domain.NewValidationError("house_id", fmt.Sprintf("must start with grid_id prefix %q", prefix))
	}
	suffix := strings.TrimPrefix(houseID, prefix)
	if !houseIDSuffix.MatchString(suffix) {
		return domain.NewValidationError("house_id", "must match {grid_id}-house{N}")
	}
	return nil
}

func validateMeterID(meterID, houseID string) error {
	expected := "meter-" + houseID
	if meterID != expected {
		return domain.NewValidationError("meter_id", fmt.Sprintf("must equal %q", expected))
	}
	return nil
}

func validateDeviceClass(v string) (domain.DeviceClass, error) {
	switch domain.DeviceClass(v) {
	case domain.DeviceClassConsumer, domain.DeviceClassResidentialProsumer, domain.DeviceClassCommercial:
		return domain.DeviceClass(v), nil
	default:
		return "", domain.NewValidationError("device_class", fmt.Sprintf("unrecognized value %q", v))
	}
}

func validateAssetType(field, v string) (domain.AssetType, error) {
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

func validatePercent(field string, v float64) error {
	if err := validateFinite(field, v); err != nil {
		return err
	}
	if v < 0 || v > 100 {
		return domain.NewValidationError(field, "must be between 0 and 100")
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
