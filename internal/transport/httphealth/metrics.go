package httphealth

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// MetricsServer serves Prometheus metrics on its own listener, so /metrics is
// never exposed through the health port that load balancers probe.
type MetricsServer struct {
	server *http.Server
}

func NewMetrics(address string, handler http.Handler) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)
	return &MetricsServer{server: &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}}
}

func (s *MetricsServer) Run() error {
	err := s.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *MetricsServer) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }
