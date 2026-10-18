package ingestion

import (
	"math"
	"strings"
	"testing"
)

// The decoders face untrusted bytes. Whatever they are fed, they must not
// panic, and a nil error must mean the value satisfies the documented
// invariants (decoder.go: nothing partially valid is ever returned).

func FuzzDecodeMeterReading(f *testing.F) {
	f.Add([]byte(validMeterJSON()))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"schema_version":"1.0"`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"readings":{"solar_kw":1e999}}`))
	f.Add([]byte(validMeterJSON() + `{"extra":1}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := DecodeMeterReading(raw)
		if err != nil {
			return
		}
		if r.MeterID == "" || r.HouseID == "" || r.GridID == "" || r.SchemaVersion == "" {
			t.Fatalf("accepted a reading with an empty identity: %+v", r)
		}
		if r.Seq < 0 {
			t.Fatalf("accepted negative seq %d", r.Seq)
		}
		for name, v := range map[string]float64{"solar": r.SolarKw, "consumption": r.ConsumptionKw, "net": r.NetKw} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				t.Fatalf("accepted non-finite %s: %v", name, v)
			}
		}
		if r.SolarKw < 0 || r.ConsumptionKw < 0 {
			t.Fatalf("accepted negative generation/consumption: %+v", r)
		}
		for _, a := range r.StorageAssets {
			if a.CapacityKwh <= 0 || a.SocPct < 0 || a.SocPct > 100 || a.AssetID == "" {
				t.Fatalf("accepted an invalid storage asset: %+v", a)
			}
		}
	})
}

func FuzzDecodeHeartbeat(f *testing.F) {
	f.Add([]byte(validHeartbeatJSON))
	f.Add([]byte(`{}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(strings.Replace(validHeartbeatJSON, `"bat_001"`, `"ev_001"`, 1)))

	f.Fuzz(func(t *testing.T, raw []byte) {
		hb, err := DecodeHeartbeat(raw)
		if err != nil {
			return
		}
		if hb.GridID == "" || hb.HouseID == "" || hb.MeterID == "" {
			t.Fatalf("accepted a heartbeat with an empty identity: %+v", hb)
		}
		for _, a := range hb.FlexibleAssets {
			if a.AssetID == "" || a.CapacityKwh <= 0 || a.MaxChargeKw < 0 || a.MaxDischargeKw < 0 ||
				math.IsNaN(a.MaxChargeKw) || math.IsInf(a.MaxChargeKw, 0) {
				t.Fatalf("accepted an invalid flexible asset: %+v", a)
			}
		}
	})
}

func FuzzValidateIdentifiers(f *testing.F) {
	f.Add("grid01", "grid01-house0042", "meter-grid01-house0042")
	f.Add("", "", "")
	f.Add("GRID", "grid01-house", "meter-")
	f.Add(strings.Repeat("a", 500), "x", "y")

	f.Fuzz(func(t *testing.T, grid, house, meter string) {
		// Must never panic; accepted IDs must be non-empty and bounded.
		if validateGridID(grid) == nil && (grid == "" || len(grid) > maxGridIDLen) {
			t.Fatalf("accepted grid id %q", grid)
		}
		if validateHouseID(house, grid) == nil && house == "" {
			t.Fatal("accepted empty house id")
		}
		if validateMeterID(meter, house) == nil && meter == "" {
			t.Fatal("accepted empty meter id")
		}
	})
}
