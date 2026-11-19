package grpc

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"google.golang.org/grpc/status"
)

func TestToGRPCErrorContextErrors(t *testing.T) {
	cases := map[error]codes.Code{
		fmt.Errorf("energy summary: %w", context.Canceled):         codes.Canceled,
		fmt.Errorf("energy summary: %w", context.DeadlineExceeded): codes.DeadlineExceeded,
	}
	for in, want := range cases {
		if got := status.Code(ToGRPCError(in)); got != want {
			t.Errorf("%v: code = %v, want %v", in, got, want)
		}
	}
}

func TestToGRPCErrorMeterAlreadyRegistered(t *testing.T) {
	in := fmt.Errorf("register house: %w", domain.ErrMeterAlreadyRegistered)
	if got := status.Code(ToGRPCError(in)); got != codes.AlreadyExists {
		t.Errorf("code = %v, want %v", got, codes.AlreadyExists)
	}
}
