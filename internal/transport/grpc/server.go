package grpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
)

type Server struct {
	address string
	server  *grpc.Server
}

func New(address string) *Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(4 << 20),
	)

	return &Server{
		address: address,
		server:  server,
	}
}

func (server *Server) Run() error {
	listener, err := net.Listen("tcp", server.address)

	if err != nil {
		return fmt.Errorf("Listen grpc: %w", err)
	}

	if err := server.server.Serve(listener); err != nil {
		return fmt.Errorf("Serve grpc: %w", err)
	}

	return nil
}

func (server *Server) Stop() {
	server.server.GracefulStop()
}
