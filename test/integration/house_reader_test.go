//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

// User IDs are UUIDs, as in iot_data.meter_registrations.user_id.
const (
	u1 = "11111111-1111-1111-1111-111111111111"
	u2 = "22222222-2222-2222-2222-222222222222"
	u3 = "33333333-3333-3333-3333-333333333333"
	u9 = "99999999-9999-9999-9999-999999999999"
)

// newHouseEnv adds three houses to newEnv's grid01-house0042, names grid01
// (grid02's display name is cleared) and gives house 0060 a battery and a
// V2G-capable EV. Ownership comes from DEWA meter registrations: u1 is the
// approved primary owner of 0060 and 0061, u2 of 0062 (where u9 is only an
// approved viewer), and u3's claim on 0042 is still pending.
func newHouseEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	if _, err := openDB(t, e.dsn).ExecContext(context.Background(), `
		UPDATE iot_data.grids SET display_name = 'Colombo Zone 1' WHERE grid_id = 'grid01';
		UPDATE iot_data.grids SET display_name = NULL WHERE grid_id = 'grid02';
		INSERT INTO iot_data.houses (house_id, grid_id, device_class, meter_id, rated_solar_kw) VALUES
			('grid01-house0060', 'grid01', 'residential_prosumer', 'meter-grid01-house0060', 6),
			('grid02-house0061', 'grid02', 'consumer', 'meter-grid02-house0061', NULL),
			('grid01-house0062', 'grid01', 'commercial', 'meter-grid01-house0062', 20);
		INSERT INTO iot_data.flexible_assets (asset_id, house_id, asset_type, capacity_kwh, max_charge_kw, max_discharge_kw, v2g_capable) VALUES
			('ev_a', 'grid01-house0060', 'ev', 75, 11, 11, true),
			('bat_b', 'grid01-house0060', 'bess', 14, 5, 5, NULL);
		INSERT INTO iot_data.meter_registrations (registration_id, meter_id, user_id, approval_status, role) VALUES
			('reg-1', 'meter-grid01-house0060', '`+u1+`', 'APPROVED', 'PRIMARY_OWNER'),
			('reg-2', 'meter-grid02-house0061', '`+u1+`', 'APPROVED', 'PRIMARY_OWNER'),
			('reg-3', 'meter-grid01-house0062', '`+u2+`', 'APPROVED', 'PRIMARY_OWNER'),
			('reg-4', 'meter-grid01-house0062', '`+u9+`', 'APPROVED', 'VIEWER'),
			('reg-5', 'meter-grid01-house0042', '`+u3+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER');`); err != nil {
		t.Fatal(err)
	}
	return e
}

func houseIDs(rows []postgresstore.HouseInfoRow) []string {
	ids := []string{}
	for _, r := range rows {
		ids = append(ids, r.HouseID)
	}
	return ids
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestListHousesFilters(t *testing.T) {
	e := newHouseEnv(t)
	ctx := context.Background()

	cases := map[string]struct {
		filter postgresstore.HouseFilter
		want   []string
	}{
		"all":            {postgresstore.HouseFilter{Limit: 10}, []string{"grid01-house0042", "grid01-house0060", "grid01-house0062", "grid02-house0061"}},
		"owner":          {postgresstore.HouseFilter{OwnerUserID: u1, Limit: 10}, []string{"grid01-house0060", "grid02-house0061"}},
		"grid":           {postgresstore.HouseFilter{GridID: "grid01", Limit: 10}, []string{"grid01-house0042", "grid01-house0060", "grid01-house0062"}},
		"owner and grid": {postgresstore.HouseFilter{OwnerUserID: u1, GridID: "grid02", Limit: 10}, []string{"grid02-house0061"}},
		"unknown owner":  {postgresstore.HouseFilter{OwnerUserID: "nobody", Limit: 10}, []string{}},
		"pending claim":  {postgresstore.HouseFilter{OwnerUserID: u3, Limit: 10}, []string{}},
		"viewer only":    {postgresstore.HouseFilter{OwnerUserID: u9, Limit: 10}, []string{}},
		"primary owner":  {postgresstore.HouseFilter{OwnerUserID: u2, Limit: 10}, []string{"grid01-house0062"}},
		"page 1":         {postgresstore.HouseFilter{Limit: 2}, []string{"grid01-house0042", "grid01-house0060"}},
		"page 2":         {postgresstore.HouseFilter{AfterHouseID: "grid01-house0060", Limit: 2}, []string{"grid01-house0062", "grid02-house0061"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			rows, err := e.pg.ListHouses(ctx, c.filter)
			if err != nil {
				t.Fatal(err)
			}
			if got := houseIDs(rows); !equalIDs(got, c.want) {
				t.Fatalf("houses = %v, want %v", got, c.want)
			}
		})
	}

	rows, err := e.pg.ListHouses(ctx, postgresstore.HouseFilter{OwnerUserID: u1, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := rows[0]
	if h.GridDisplayName != "Colombo Zone 1" || h.MeterID != "meter-grid01-house0060" || h.DeviceClass != "residential_prosumer" ||
		h.RatedSolarKw == nil || *h.RatedSolarKw != 6 || h.OwnerUserID == nil || *h.OwnerUserID != u1 {
		t.Errorf("house 0060 = %+v", h)
	}
	if rows[1].GridDisplayName != "" || rows[1].RatedSolarKw != nil {
		t.Errorf("house 0061 = %+v, want empty display name and nil rated solar", rows[1])
	}
}

func TestGetHouseWithAssets(t *testing.T) {
	e := newHouseEnv(t)
	row, assets, err := e.pg.GetHouse(context.Background(), "grid01-house0060")
	if err != nil {
		t.Fatal(err)
	}
	if row.HouseID != "grid01-house0060" || row.GridID != "grid01" || row.GridDisplayName != "Colombo Zone 1" ||
		row.OwnerUserID == nil || *row.OwnerUserID != u1 {
		t.Fatalf("house = %+v", row)
	}
	if len(assets) != 2 {
		t.Fatalf("assets = %+v, want 2", assets)
	}
	bat, ev := assets[0], assets[1]
	if bat.AssetID != "bat_b" || bat.AssetType != "bess" || bat.CapacityKwh != 14 || bat.MaxChargeKw != 5 || bat.MaxDischargeKw != 5 || bat.V2GCapable != nil {
		t.Errorf("battery = %+v", bat)
	}
	if ev.AssetID != "ev_a" || ev.AssetType != "ev" || ev.V2GCapable == nil || !*ev.V2GCapable {
		t.Errorf("ev = %+v", ev)
	}

	_, none, err := e.pg.GetHouse(context.Background(), "grid01-house0042")
	if err != nil || len(none) != 0 {
		t.Errorf("house without assets: %v %v", none, err)
	}
}

func TestGetHouseUnknown(t *testing.T) {
	e := newHouseEnv(t)
	if _, _, err := e.pg.GetHouse(context.Background(), "grid01-house9999"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestHouseOwnerIsTheApprovedPrimaryOwner(t *testing.T) {
	e := newHouseEnv(t)
	ctx := context.Background()

	owned, _, err := e.pg.GetHouse(ctx, "grid01-house0062")
	if err != nil {
		t.Fatal(err)
	}
	if owned.OwnerUserID == nil || *owned.OwnerUserID != u2 {
		t.Errorf("house 0062 owner = %v, want the primary owner, not the viewer", owned.OwnerUserID)
	}
}

func TestHouseOwnerIgnoresUnapprovedRegistrations(t *testing.T) {
	e := newHouseEnv(t)
	ctx := context.Background()
	db := openDB(t, e.dsn)

	for _, status := range []string{"DEWA_APPROVAL_PENDING", "REJECTED", "EXPIRED"} {
		t.Run(status, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, `UPDATE iot_data.meter_registrations SET approval_status = $1 WHERE registration_id = 'reg-5'`, status); err != nil {
				t.Fatal(err)
			}
			row, _, err := e.pg.GetHouse(ctx, "grid01-house0042")
			if err != nil {
				t.Fatal(err)
			}
			if row.OwnerUserID != nil {
				t.Errorf("owner = %q, want none", *row.OwnerUserID)
			}
			rows, err := e.pg.ListHouses(ctx, postgresstore.HouseFilter{OwnerUserID: u3, Limit: 10})
			if err != nil || len(rows) != 0 {
				t.Errorf("houses for u3 = %v, %v; want none", houseIDs(rows), err)
			}
		})
	}
}
