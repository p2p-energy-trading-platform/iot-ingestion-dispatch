
package grpc

import (
	"log/slog"

	"google.golang.org/grpc"
)


type RegistrationHandler struct {
	logger *slog.Logger
}


func NewRegistrationHandler(logger *slog.Logger) *RegistrationHandler {

	if logger == nil {
		logger = slog.Default()
	}

	return &RegistrationHandler{
		logger: logger,
	}

}



func RegisterRegistrationService(
	server *grpc.Server,
	handler *RegistrationHandler,
) {
	// TODO: Register the generated DeviceRegistrationService server.
}
