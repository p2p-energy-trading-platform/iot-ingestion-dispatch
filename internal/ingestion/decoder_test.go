package ingestion

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

// --- fixtures, built from the project's own documented examples ---

// validMeterReadingPayload mirrors simulation_plan.md section 2.1's
// worked example exactly (net_kw included as-is, unreconciled against
// section 5.4's formula - see the comment in decoder.go for why).
func validMeterReadingPayload() map[string]any {
	return map[string]any{
		"schema_version": "1.0",
		"meter_id":       "meter-grid01-house0042",
		"house_id":       "grid01-house0042",
		"grid_id":        "grid01",
		"device_class":   "residential_prosumer",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"seq":            184231,
		"readings": map[string]any{
			"solar_kw":       2.41,
			"consumption_kw": 0.87,
			"net_kw":         1.54,
			"storage_assets": []any{
				map[string]any{
					"asset_id":     "bat_001",
					"asset_type":   "bess",
					"soc_pct":      63.5,
					"power_kw":     -0.30,
					"capacity_kwh": 10.0,
				},
				map[string]any{
					"asset_id":     "ev_001",
					"asset_type":   "ev",
					"soc_pct":      41.0,
					"power_kw":     -1.5,
					"capacity_kwh": 40.0,
					"plugged_in":   true,
				},
			},
		},
		"meta": map[string]any{
			"weather_irradiance_wm2": 612.0,
			"cloud_cover_pct":        18,
		},
	}
}

// validHeartbeatPayload mirrors gridx_iot_handoff_summary.md's exact
// HeartbeatPayload example.
func validHeartbeatPayload() map[string]any {
	return map[string]any{
		"schema_version": "1.0",
		"grid_id":        "grid01",
		"house_id":       "grid01-house0042",
		"meter_id":       "meter-grid01-house0042",
		"status":         "online",
		"device_class":   "residential_prosumer",
		"rated_solar_kw": 5.0,
		"flexible_assets": []any{
			map[string]any{
				"asset_id":         "bat_001",
				"asset_type":       "bess",
				"capacity_kwh":     10.0,
				"max_charge_kw":    3.5,
				"max_discharge_kw": 3.5,
			},
			map[string]any{
				"asset_id":         "ev_001",
				"asset_type":       "ev",
				"capacity_kwh":     40.0,
				"max_charge_kw":    7.2,
				"max_discharge_kw": 3.6,
				"v2g_capable":      true,
			},
		},
	}
}

// deepCopyMap gives each table-driven case its own independent fixture,
// so one test's mutation never leaks into another's.
func deepCopyMap(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal fixture for copy: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal fixture for copy: %v", err)
	}
	return out
}

func readingsOf(m map[string]any) map[string]any {
	return m["readings"].(map[string]any)
}

func storageAssetOf(m map[string]any, i int) map[string]any {
	return readingsOf(m)["storage_assets"].([]any)[i].(map[string]any)
}

func flexAssetOf(m map[string]any, i int) map[string]any {
	return m["flexible_assets"].([]any)[i].(map[string]any)
}

// assertValidationField checks that err is a *domain.ValidationError
// (both via errors.As and errors.Is against the sentinel) whose Field
// contains wantSubstr.
func assertValidationField(t *testing.T, err error, wantSubstr string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *domain.ValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("error = %q, want substring %q", err.Error(), wantSubstr)
	}
	if !errors.Is(err, domain.ErrValidation) {
		t.Error("errors.Is(err, domain.ErrValidation) = false, want true")
	}
}

// --- meter reading: valid case ---

func TestDecodeMeterReading_Valid(t *testing.T) {
	raw, err := json.Marshal(validMeterReadingPayload())
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	reading, err := DecodeMeterReading(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reading.MeterID != "meter-grid01-house0042" {
		t.Errorf("meter_id = %q, want %q", reading.MeterID, "meter-grid01-house0042")
	}
	if reading.GridID != "grid01" {
		t.Errorf("grid_id = %q, want %q", reading.GridID, "grid01")
	}
	if reading.DeviceClass != domain.DeviceClassResidentialProsumer {
		t.Errorf("device_class = %q, want %q", reading.DeviceClass, domain.DeviceClassResidentialProsumer)
	}
	if reading.Seq != 184231 {
		t.Errorf("seq = %d, want 184231", reading.Seq)
	}
	if len(reading.StorageAssets) != 2 {
		t.Fatalf("storage assets = %d, want 2", len(reading.StorageAssets))
	}
	if reading.StorageAssets[0].PluggedIn != nil {
		t.Errorf("bess asset plugged_in should be nil, got %v", *reading.StorageAssets[0].PluggedIn)
	}
	if reading.StorageAssets[1].PluggedIn == nil || !*reading.StorageAssets[1].PluggedIn {
		t.Errorf("ev asset plugged_in = %v, want true", reading.StorageAssets[1].PluggedIn)
	}
	if reading.WeatherIrradianceWm2 == nil || *reading.WeatherIrradianceWm2 != 612.0 {
		t.Errorf("weather_irradiance_wm2 = %v, want 612.0", reading.WeatherIrradianceWm2)
	}
}

func TestDecodeMeterReading_MalformedJSON(t *testing.T) {
	_, err := DecodeMeterReading([]byte(`{not valid json`))
	assertValidationField(t, err, "payload")
}

// --- meter reading: invalid cases, one per ticket validation bullet ---

func TestDecodeMeterReading_Invalid(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(m map[string]any)
		wantField string
	}{
		{"missing schema_version", func(m map[string]any) { delete(m, "schema_version") }, "schema_version"},
		{"unsupported schema_version", func(m map[string]any) { m["schema_version"] = "9.9" }, "schema_version"},
		{"missing grid_id", func(m map[string]any) { delete(m, "grid_id") }, "grid_id"},
		{"bad grid_id grammar", func(m map[string]any) { m["grid_id"] = "GRID_01!" }, "grid_id"},
		{"house_id wrong grid prefix", func(m map[string]any) { m["house_id"] = "grid02-house0042" }, "house_id"},
		{"house_id bad suffix", func(m map[string]any) { m["house_id"] = "grid01-apartment42" }, "house_id"},
		{"meter_id mismatch", func(m map[string]any) { m["meter_id"] = "meter-wrong-house" }, "meter_id"},
		{"missing device_class", func(m map[string]any) { delete(m, "device_class") }, "device_class"},
		{"unrecognized device_class", func(m map[string]any) { m["device_class"] = "toaster" }, "device_class"},
		{"missing timestamp", func(m map[string]any) { delete(m, "timestamp") }, "timestamp"},
		{"malformed timestamp", func(m map[string]any) { m["timestamp"] = "16-06-2026" }, "timestamp"},
		{"timestamp too far future", func(m map[string]any) {
			m["timestamp"] = time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339)
		}, "timestamp"},
		{"timestamp too far past", func(m map[string]any) {
			m["timestamp"] = time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
		}, "timestamp"},
		{"missing seq", func(m map[string]any) { delete(m, "seq") }, "seq"},
		{"negative seq", func(m map[string]any) { m["seq"] = -1 }, "seq"},
		{"missing readings", func(m map[string]any) { delete(m, "readings") }, "readings"},
		{"missing solar_kw", func(m map[string]any) { delete(readingsOf(m), "solar_kw") }, "readings.solar_kw"},
		{"negative solar_kw", func(m map[string]any) { readingsOf(m)["solar_kw"] = -1.0 }, "readings.solar_kw"},
		{"negative consumption_kw", func(m map[string]any) { readingsOf(m)["consumption_kw"] = -1.0 }, "readings.consumption_kw"},
		{"missing net_kw", func(m map[string]any) { delete(readingsOf(m), "net_kw") }, "readings.net_kw"},
		{"bad asset_type", func(m map[string]any) { storageAssetOf(m, 0)["asset_type"] = "solar" }, "asset_type"},
		{"duplicate asset_id in storage_assets", func(m map[string]any) {
			storageAssetOf(m, 1)["asset_id"] = storageAssetOf(m, 0)["asset_id"]
		}, "duplicate"},
		{"bess asset with plugged_in present", func(m map[string]any) {
			storageAssetOf(m, 0)["plugged_in"] = true
		}, "plugged_in"},
		{"ev asset missing plugged_in", func(m map[string]any) {
			delete(storageAssetOf(m, 1), "plugged_in")
		}, "plugged_in"},
		{"soc_pct out of range", func(m map[string]any) { storageAssetOf(m, 0)["soc_pct"] = 150.0 }, "soc_pct"},
		{"capacity_kwh zero", func(m map[string]any) { storageAssetOf(m, 0)["capacity_kwh"] = 0.0 }, "capacity_kwh"},
		{"cloud_cover_pct out of range", func(m map[string]any) {
			m["meta"].(map[string]any)["cloud_cover_pct"] = 150
		}, "cloud_cover_pct"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := deepCopyMap(t, validMeterReadingPayload())
			tt.mutate(payload)

			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal mutated fixture: %v", err)
			}

			_, err = DecodeMeterReading(raw)
			assertValidationField(t, err, tt.wantField)
		})
	}
}

// --- heartbeat: valid case ---

func TestDecodeHeartbeat_Valid(t *testing.T) {
	raw, err := json.Marshal(validHeartbeatPayload())
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	hb, err := DecodeHeartbeat(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hb.Status != "online" {
		t.Errorf("status = %q, want online", hb.Status)
	}
	if len(hb.FlexibleAssets) != 2 {
		t.Fatalf("flexible assets = %d, want 2", len(hb.FlexibleAssets))
	}
	if hb.FlexibleAssets[0].V2GCapable != nil {
		t.Errorf("bess asset v2g_capable should be nil, got %v", *hb.FlexibleAssets[0].V2GCapable)
	}
	if hb.FlexibleAssets[1].V2GCapable == nil || !*hb.FlexibleAssets[1].V2GCapable {
		t.Errorf("ev asset v2g_capable = %v, want true", hb.FlexibleAssets[1].V2GCapable)
	}
}

func TestDecodeHeartbeat_MalformedJSON(t *testing.T) {
	_, err := DecodeHeartbeat([]byte(`not json at all`))
	assertValidationField(t, err, "payload")
}

// --- heartbeat: invalid cases ---

func TestDecodeHeartbeat_Invalid(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(m map[string]any)
		wantField string
	}{
		{"missing schema_version", func(m map[string]any) { delete(m, "schema_version") }, "schema_version"},
		{"missing grid_id", func(m map[string]any) { delete(m, "grid_id") }, "grid_id"},
		{"house_id wrong grid prefix", func(m map[string]any) { m["house_id"] = "grid02-house0042" }, "house_id"},
		{"meter_id mismatch", func(m map[string]any) { m["meter_id"] = "meter-wrong-house" }, "meter_id"},
		{"missing status", func(m map[string]any) { delete(m, "status") }, "status"},
		{"unrecognized status", func(m map[string]any) { m["status"] = "sleeping" }, "status"},
		{"unrecognized device_class", func(m map[string]any) { m["device_class"] = "toaster" }, "device_class"},
		{"missing rated_solar_kw", func(m map[string]any) { delete(m, "rated_solar_kw") }, "rated_solar_kw"},
		{"negative rated_solar_kw", func(m map[string]any) { m["rated_solar_kw"] = -1.0 }, "rated_solar_kw"},
		{"bad asset_type", func(m map[string]any) { flexAssetOf(m, 0)["asset_type"] = "solar" }, "asset_type"},
		{"duplicate asset_id in flexible_assets", func(m map[string]any) {
			flexAssetOf(m, 1)["asset_id"] = flexAssetOf(m, 0)["asset_id"]
		}, "duplicate"},
		{"bess asset with v2g_capable present", func(m map[string]any) {
			flexAssetOf(m, 0)["v2g_capable"] = true
		}, "v2g_capable"},
		{"ev asset missing v2g_capable", func(m map[string]any) {
			delete(flexAssetOf(m, 1), "v2g_capable")
		}, "v2g_capable"},
		{"capacity_kwh zero", func(m map[string]any) { flexAssetOf(m, 0)["capacity_kwh"] = 0.0 }, "capacity_kwh"},
		{"negative max_charge_kw", func(m map[string]any) { flexAssetOf(m, 0)["max_charge_kw"] = -1.0 }, "max_charge_kw"},
		{"negative max_discharge_kw", func(m map[string]any) { flexAssetOf(m, 0)["max_discharge_kw"] = -1.0 }, "max_discharge_kw"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := deepCopyMap(t, validHeartbeatPayload())
			tt.mutate(payload)

			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal mutated fixture: %v", err)
			}

			_, err = DecodeHeartbeat(raw)
			assertValidationField(t, err, tt.wantField)
		})
	}
}

// --- white-box: validateFinite directly ---
//
// NaN/Inf can't actually appear in valid JSON text (the JSON spec has no
// literal for them - encoding/json fails at the syntax level first), so
// this can't be exercised through DecodeMeterReading/DecodeHeartbeat.
// Testing the helper directly instead, as defense-in-depth documentation
// of intent rather than a reachable-through-JSON code path.
func TestValidateFinite(t *testing.T) {
	if err := validateFinite("x", math.NaN()); err == nil {
		t.Error("expected error for NaN")
	}
	if err := validateFinite("x", math.Inf(1)); err == nil {
		t.Error("expected error for +Inf")
	}
	if err := validateFinite("x", math.Inf(-1)); err == nil {
		t.Error("expected error for -Inf")
	}
	if err := validateFinite("x", 42.0); err != nil {
		t.Errorf("expected no error for finite value, got %v", err)
	}
}
