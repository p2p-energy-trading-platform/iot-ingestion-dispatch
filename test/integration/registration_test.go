//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

func TestRegisterHouseThenHeartbeatIsAccepted(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	reg := postgresstore.HouseRegistration{
		HouseID: "grid01-house0007", GridID: "grid01", MeterID: "meter-grid01-house0007",
		DeviceClass: domain.DeviceClassResidentialProsumer,
	}

	// Before registration the heartbeat is an identity failure.
	if err := route(e, heartbeatTopic, heartbeatJSON(7, "grid01", "bat_0007", 10)); !errors.Is(err, domain.ErrHouseIdentity) {
		t.Fatalf("unregistered: error = %v, want ErrHouseIdentity", err)
	}

	if out, err := e.pg.RegisterHouse(ctx, reg); err != nil || out != postgresstore.HouseCreated {
		t.Fatalf("register: %v %v", out, err)
	}
	if out, err := e.pg.RegisterHouse(ctx, reg); err != nil || out != postgresstore.HouseUnchanged {
		t.Fatalf("re-register: %v %v; want unchanged", out, err)
	}
	if err := route(e, heartbeatTopic, heartbeatJSON(7, "grid01", "bat_0007", 10)); err != nil {
		t.Fatalf("registered house heartbeat: %v", err)
	}
}

func TestRegisterHouseNeverMovesOrRebinds(t *testing.T) {
	e := newEnv(t) // registers grid01-house0042 with meter-grid01-house0042
	ctx := context.Background()

	tests := map[string]postgresstore.HouseRegistration{
		"different grid":         {HouseID: "grid01-house0042", GridID: "grid02", MeterID: "meter-grid01-house0042", DeviceClass: domain.DeviceClassResidentialProsumer},
		"different class":        {HouseID: "grid01-house0042", GridID: "grid01", MeterID: "meter-grid01-house0042", DeviceClass: domain.DeviceClassConsumer},
		"meter of another house": {HouseID: "grid01-house0043", GridID: "grid01", MeterID: "meter-grid01-house0042", DeviceClass: domain.DeviceClassConsumer},
	}
	for name, reg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := e.pg.RegisterHouse(ctx, reg); !errors.Is(err, domain.ErrHouseIdentity) {
				t.Fatalf("error = %v, want ErrHouseIdentity", err)
			}
		})
	}
	if g := scalar[string](t, e.dsn, `SELECT grid_id || '/' || meter_id || '/' || device_class FROM iot_data.houses WHERE house_id='grid01-house0042'`); g != "grid01/meter-grid01-house0042/residential_prosumer" {
		t.Errorf("registration changed to %q", g)
	}

	_, err := e.pg.RegisterHouse(ctx, postgresstore.HouseRegistration{
		HouseID: "grid09-house0001", GridID: "grid09", MeterID: "meter-grid09-house0001", DeviceClass: domain.DeviceClassConsumer,
	})
	if !errors.Is(err, domain.ErrGridNotProvisioned) {
		t.Fatalf("unknown grid: error = %v, want ErrGridNotProvisioned", err)
	}
}
