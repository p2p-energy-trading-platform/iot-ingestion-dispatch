package grpc

import (
	"testing"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/test/helpers"
)

func TestServerRegistersServices(t *testing.T) {
	server := New(":0", helpers.TestLogger())

	var registrar grpc.ServiceRegistrar = server
	iotv1.RegisterIotServiceServer(registrar, iotv1.UnimplementedIotServiceServer{})

	if _, ok := server.server.GetServiceInfo()["gridx.iot.v1.IotService"]; !ok {
		t.Fatal("IotService is not registered")
	}
	if _, ok := server.server.GetServiceInfo()["grpc.health.v1.Health"]; !ok {
		t.Fatal("health service is no longer registered")
	}
}
