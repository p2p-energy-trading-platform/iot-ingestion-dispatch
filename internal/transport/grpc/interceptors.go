package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryServerLogger(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		duration := time.Since(start)
		st, _ := status.FromError(err)
		code := st.Code()

		logFn := logger.Info
		msg := "gRPC request completed"

		if code != codes.OK {
			switch code {
			case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated, codes.PermissionDenied:
				// Client-side errors
				logFn = logger.Warn
				msg = "gRPC client error"
			default:
				// Internal, Unknown
				logFn = logger.Error
				msg = "gRPC server error"
			}
		}

		attrs := []any{
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"code", code.String(),
		}
		if err != nil {
			attrs = append(attrs, "error", err)
		}

		logFn(msg, attrs...)

		return resp, err
	}
}

func UnaryServerRecovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in gRPC server",
					"method", info.FullMethod,
					"panic", r,
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// StreamServerLogger logs one line when a stream ends. A stream that reached
// its maximum age (DEADLINE_EXCEEDED), that the client left (CANCELED) or
// that ended because this server is shutting down (UNAVAILABLE) ended
// normally: callers reopen it.
func StreamServerLogger(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		code := status.Code(err)

		logFn, msg := logger.Info, "gRPC stream ended"
		switch code {
		case codes.OK, codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated, codes.PermissionDenied:
			logFn, msg = logger.Warn, "gRPC stream ended with a client error"
		default:
			logFn, msg = logger.Error, "gRPC stream ended with a server error"
		}

		attrs := []any{
			"method", info.FullMethod,
			"duration_ms", time.Since(start).Milliseconds(),
			"code", code.String(),
		}
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		logFn(msg, attrs...)
		return err
	}
}

// StreamServerRecovery turns a panic in a stream handler into INTERNAL
// instead of crashing the process.
func StreamServerRecovery(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in gRPC server",
					"method", info.FullMethod,
					"panic", r,
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}
}
