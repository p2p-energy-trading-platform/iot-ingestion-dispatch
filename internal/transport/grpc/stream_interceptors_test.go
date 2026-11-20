package grpc

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// streamingIot answers StreamHouseLive with the configured behaviour.
type streamingIot struct {
	iotv1.UnimplementedIotServiceServer
	run func() error
}

func (s streamingIot) StreamHouseLive(*iotv1.StreamHouseLiveRequest, grpc.ServerStreamingServer[iotv1.StreamHouseLiveResponse]) error {
	return s.run()
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// streamCode serves run behind the real server (with its interceptors) and
// returns the code the client sees and what was logged.
func streamCode(t *testing.T, run func() error) (codes.Code, string) {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	server := New(":0", logger, Services{Iot: streamingIot{run: run}})

	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.server.Serve(listener) }()
	t.Cleanup(server.server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := iotv1.NewIotServiceClient(conn).StreamHouseLive(context.Background(), &iotv1.StreamHouseLiveRequest{HouseId: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	server.server.GracefulStop() // waits for the handler, so its log line is written
	return status.Code(err), logs.String()
}

func TestStreamPanicIsRecoveredAsInternal(t *testing.T) {
	code, logs := streamCode(t, func() error { panic("boom") })

	if code != codes.Internal {
		t.Fatalf("code = %v, want Internal", code)
	}
	if !strings.Contains(logs, "panic recovered in gRPC server") || !strings.Contains(logs, "StreamHouseLive") {
		t.Fatalf("logs = %q, want the recovered panic with its method", logs)
	}
}

func TestStreamEndIsLoggedByOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantLevel string
	}{
		{"maximum age reached", status.Error(codes.DeadlineExceeded, "max age"), "level=INFO"},
		{"server shutting down", status.Error(codes.Unavailable, "shutting down"), "level=INFO"},
		{"client error", status.Error(codes.NotFound, "no such house"), "level=WARN"},
		{"server error", status.Error(codes.Internal, "broken"), "level=ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, logs := streamCode(t, func() error { return tc.err })

			var line string
			for _, l := range strings.Split(logs, "\n") {
				if strings.Contains(l, "/gridx.iot.v1.IotService/StreamHouseLive") {
					line = l
				}
			}
			if !strings.Contains(line, tc.wantLevel) || !strings.Contains(line, "code="+status.Code(tc.err).String()) || !strings.Contains(line, "duration_ms=") {
				t.Fatalf("log line = %q, want %s with code and duration", line, tc.wantLevel)
			}
		})
	}
}
