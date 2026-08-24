package httphealth

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

type Server struct {
	server *http.Server
	ready  atomic.Bool
}

func New(address string) *Server {
	instance := &Server{}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !instance.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	instance.server = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return instance
}

// SetReady flips /readyz's reported state. Call with true only after
// admission bootstrap (grid registry load) has succeeded and the Kafka
// consumer has started — per 05-startup-registry.md step 7: "only then
// allow ingestion readiness."
func (server *Server) SetReady(ready bool) {
	server.ready.Store(ready)
}

func (server *Server) Run() error {
	err := server.server.ListenAndServe()

	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}

func (server *Server) Shutdown(context context.Context) error {
	return server.server.Shutdown(context)
}
