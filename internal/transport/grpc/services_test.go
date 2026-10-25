package grpc

import (
	"testing"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/test/helpers"
)

func TestNewRegistersServices(t *testing.T) {
	server := New(":0", helpers.TestLogger(), Services{Iot: iotv1.UnimplementedIotServiceServer{}})

	for _, name := range []string{"gridx.iot.v1.IotService", "grpc.health.v1.Health"} {
		if _, ok := server.server.GetServiceInfo()[name]; !ok {
			t.Errorf("%s is not registered", name)
		}
	}
}
