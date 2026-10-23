package grpc

import (
	"context"
	"errors"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	if errors.Is(err, domain.ErrValidation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}

	if errors.Is(err, domain.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}

	if errors.Is(err, domain.ErrIntegrityConflict) {
		return status.Error(codes.AlreadyExists, err.Error())
	}

	return status.Error(codes.Internal, "Internal Server Error")
}
