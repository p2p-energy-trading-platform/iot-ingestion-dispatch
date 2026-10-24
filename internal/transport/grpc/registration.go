
package grpc

import (
	"log/slog"

	registration "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
	"google.golang.org/grpc"
)


type RegistrationHandler struct {

	logger              *slog.Logger
	registrationService *registration.Service

}


func NewRegistrationHandler(

	registrationService *registration.Service,
	logger *slog.Logger,

) *RegistrationHandler {

	if logger == nil {
		logger = slog.Default()
	}

	return &RegistrationHandler{
		logger:              logger,
		registrationService: registrationService,
	}

}


func RegisterRegistrationService(
	server *grpc.Server,
	handler *RegistrationHandler,
) {

	// Register the generated DeviceRegistrationService here
	// once we identify its protobuf package.
	
}
