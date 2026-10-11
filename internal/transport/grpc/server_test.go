package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// TestNewServerNotServing checks that a new server is not ready initially.
func TestNewServerNotServing(t *testing.T) {
	server := New(":50051")

	response, err := server.health.Check(
		context.Background(),
		&grpc_health_v1.HealthCheckRequest{},
	)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	if response.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf(
			"expected NOT_SERVING, got %v",
			response.Status,
		)
	}
}

// TestSetReady checks that SetReady updates the health status correctly.
func TestSetReady(t *testing.T) {
	server := New(":50051")

	// Test that setting ready to true reports SERVING.
	server.SetReady(true)

	response, err := server.health.Check(
		context.Background(),
		&grpc_health_v1.HealthCheckRequest{},
	)
	if err != nil {
		t.Fatalf("health check failed after setting ready: %v", err)
	}

	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf(
			"expected SERVING after SetReady(true), got %v",
			response.Status,
		)
	}

	// Test that setting ready to false reports NOT_SERVING again.
	server.SetReady(false)

	response, err = server.health.Check(
		context.Background(),
		&grpc_health_v1.HealthCheckRequest{},
	)
	if err != nil {
		t.Fatalf("health check failed after clearing readiness: %v", err)
	}

	if response.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf(
			"expected NOT_SERVING after SetReady(false), got %v",
			response.Status,
		)
	}
}
