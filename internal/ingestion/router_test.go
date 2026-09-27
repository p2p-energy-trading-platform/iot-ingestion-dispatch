package ingestion

import (
	"context"
	"errors"
	"testing"
)

type stubHandler struct {
	calls  int
	gotKey []byte
	gotVal []byte
	err    error
}

func (h *stubHandler) HandleRecord(_ context.Context, key, value []byte) error {
	h.calls++
	h.gotKey = key
	h.gotVal = value
	return h.err
}

func TestRouter_RoutesToRegisteredHandler(t *testing.T) {
	handler := &stubHandler{}
	router := NewRouter()
	router.Register("iot.meter-readings", handler)

	err := router.Route(context.Background(), "iot.meter-readings", []byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler.calls != 1 {
		t.Errorf("handler calls = %d, want 1", handler.calls)
	}
	if string(handler.gotKey) != "key1" || string(handler.gotVal) != "val1" {
		t.Errorf("handler got key=%q value=%q, want key=%q value=%q", handler.gotKey, handler.gotVal, "key1", "val1")
	}
}

func TestRouter_DispatchesByTopicIndependently(t *testing.T) {
	meterHandler := &stubHandler{}
	heartbeatHandler := &stubHandler{}
	router := NewRouter()
	router.Register("iot.meter-readings", meterHandler)
	router.Register("iot.heartbeats", heartbeatHandler)

	_ = router.Route(context.Background(), "iot.heartbeats", nil, []byte("hb"))

	if meterHandler.calls != 0 {
		t.Errorf("meter handler calls = %d, want 0", meterHandler.calls)
	}
	if heartbeatHandler.calls != 1 {
		t.Errorf("heartbeat handler calls = %d, want 1", heartbeatHandler.calls)
	}
}

func TestRouter_UnknownTopicIsAnError(t *testing.T) {
	router := NewRouter()
	router.Register("iot.meter-readings", &stubHandler{})

	err := router.Route(context.Background(), "some.other.topic", nil, nil)
	if err == nil {
		t.Fatal("expected an error for unregistered topic, got nil")
	}
}

func TestRouter_PropagatesHandlerError(t *testing.T) {
	wantErr := errors.New("boom")
	handler := &stubHandler{err: wantErr}
	router := NewRouter()
	router.Register("iot.meter-readings", handler)

	err := router.Route(context.Background(), "iot.meter-readings", nil, nil)
	if !errors.Is(err, wantErr) {
		t.Errorf("Route() error = %v, want %v", err, wantErr)
	}
}
