package grpc

import (
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health" 
	"google.golang.org/grpc/health/grpc_health_v1" 
)

const gracefulStopTimeout = 5 * time.Second

type Server struct {
	address string
	server  *grpc.Server
	health  *health.Server 
}

func New(address string) *Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(4 << 20),
	)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	healthServer.SetServingStatus(
		"",
		grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	)

	return &Server{
		address: address,
		server:  server,
		health:  healthServer, 
	}
}

func (server *Server) Run() error {
	listener, err := net.Listen("tcp", server.address)

	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	if err := server.server.Serve(listener); err != nil {
		return fmt.Errorf("serve grpc: %w", err)
	}

	return nil
}

func (server *Server) SetReady(ready bool) {
	status := grpc_health_v1.HealthCheckResponse_NOT_SERVING

	if ready {
		status = grpc_health_v1.HealthCheckResponse_SERVING
	}

	server.health.SetServingStatus("", status)
}


func (server *Server) Stop() {

	stopped := make(chan struct{})

	go func() {
		server.server.GracefulStop()
		close(stopped)
	}()

	
	timer := time.NewTimer(gracefulStopTimeout)
	defer timer.Stop()

	select {
		case <-stopped:
			
			return
		case <-timer.C:
			
			server.server.Stop()
			<-stopped
	}

}
