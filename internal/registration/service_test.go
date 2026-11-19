package registration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

type mockStore struct {
	saveFunc          func(ctx context.Context, reg RegistrationRecord) error
	getFunc           func(ctx context.Context, registrationID string) (*RegistrationRecord, error)
	updateFunc        func(ctx context.Context, registrationID string, status string) error
	getRegister       func(ctx context.Context, meterID string) (*RegistrationRecord, error)
	getRegisterByUser func(ctx context.Context, userReference string) (*RegistrationRecord, error)
	getActiveByUser   func(ctx context.Context, userID string) (*RegistrationRecord, error)
}

type mockActuationProducer struct {
	sendFunc func(ctx context.Context, meterID string) error
}

func (m *mockActuationProducer) SendActivateMeter(ctx context.Context, meterID string) error {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, meterID)
	}
	return nil
}

func (m *mockStore) SaveMeterRegistration(ctx context.Context, reg RegistrationRecord) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, reg)
	}
	return nil
}

func (m *mockStore) GetByRegistrationID(ctx context.Context, registrationID string) (*RegistrationRecord, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, registrationID)
	}
	return nil, nil
}

func (m *mockStore) UpdateMeterRegistrationStatus(ctx context.Context, registrationID string, status string) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, registrationID, status)
	}
	return nil
}

func (m *mockStore) GetRegistrationByMeterID(ctx context.Context, meterID string) (*RegistrationRecord, error) {
	if m.getRegister != nil {
		return m.getRegister(ctx, meterID)
	}
	return nil, nil
}

func (m *mockStore) GetRegistrationByUserID(ctx context.Context, userReference string) (*RegistrationRecord, error) {
	if m.getRegisterByUser != nil {
		return m.getRegisterByUser(ctx, userReference)
	}
	return nil, nil
}

func (m *mockStore) GetActiveRegistrationByUserID(ctx context.Context, userID string) (*RegistrationRecord, error) {
	if m.getActiveByUser != nil {
		return m.getActiveByUser(ctx, userID)
	}
	return nil, nil
}

func TestRegisterHouseSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			if r.Method == http.MethodPost {
				if r.URL.Path != "/api/link-requests" {
					t.Errorf("unexpected POST path: %s", r.URL.Path)
				}

				var request LinkRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("failed to decode request: %v", err)
					return
				}

				if request.RegistrationID != "reg-123" {
					t.Errorf("unexpected registration ID: %s", request.RegistrationID)
				}

				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(
					`{"requestId":"server-uuid-123","status":"PENDING","message":"Link request created"}`,
				))
				return
			}

			if r.Method == http.MethodGet {
				if r.URL.Path != "/api/link-requests/server-uuid-123" {
					t.Errorf("unexpected GET path: %s", r.URL.Path)
				}

				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(
					`{"registrationId":"reg-123","meterId":"meter-001","status":"APPROVED"}`,
				))
				return
			}

			t.Errorf("unexpected method: %s", r.Method)
		},
	))
	defer server.Close()

	client := NewClient(server.URL)
	poller := NewPoller(client)
	store := &mockStore{
		saveFunc: func(ctx context.Context, reg RegistrationRecord) error {
			return nil
		},
		updateFunc: func(ctx context.Context, registrationID string, status string) error {
			return nil
		},
	}

	producer := &mockActuationProducer{
		sendFunc: func(ctx context.Context, meterID string) error {
			if meterID != "meter-001" {
				t.Errorf("expected meterID 'meter-001', got %s", meterID)
			}
			return nil
		},
	}

	service := NewService(client, poller, store, producer, slog.Default())

	request := LinkRequest{
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		UserReference:  "user-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)
	if err != nil {
		t.Fatalf("RegisterHouse returned an error: %v", err)
	}

	if response.Status != "PENDING" {
		t.Errorf("expected status PENDING, got %s", response.Status)
	}

	if response.Message != "Link request created" {
		t.Errorf("unexpected message: %s", response.Message)
	}

	if response.RequestID != "server-uuid-123" {
		t.Errorf("expected requestId 'server-uuid-123', got %s", response.RequestID)
	}
}

func TestRegisterHouseMissingRegistrationID(t *testing.T) {
	client := NewClient("http://localhost")
	store := &mockStore{}
	producer := &mockActuationProducer{}
	service := NewService(client, nil, store, producer, slog.Default())

	request := LinkRequest{
		MeterID:       "meter-001",
		UserReference: "user-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)

	if err == nil {
		t.Fatal("expected an error for missing registration ID")
	}

	if response != nil {
		t.Errorf("expected nil response, got %+v", response)
	}
}

func TestRegisterHouseMissingMeterID(t *testing.T) {
	client := NewClient("http://localhost")
	store := &mockStore{}
	producer := &mockActuationProducer{}
	service := NewService(client, nil, store, producer, slog.Default())

	request := LinkRequest{
		RegistrationID: "reg-123",
		UserReference:  "user-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)

	if err == nil {
		t.Fatal("expected an error for missing meter ID")
	}

	if response != nil {
		t.Errorf("expected nil response, got %+v", response)
	}
}

func TestRegisterHouseMissingUserReference(t *testing.T) {
	client := NewClient("http://localhost")
	store := &mockStore{}
	producer := &mockActuationProducer{}
	service := NewService(client, nil, store, producer, slog.Default())

	request := LinkRequest{
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)

	if err == nil {
		t.Fatal("expected an error for missing user reference")
	}

	if response != nil {
		t.Errorf("expected nil response, got %+v", response)
	}
}

func TestRegisterHouseNilClientOrStore(t *testing.T) {
	// Testing with nil client/store configuration
	service := NewService(nil, nil, nil, nil, slog.Default())

	request := LinkRequest{
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		UserReference:  "user-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)

	if err == nil {
		t.Fatal("expected an error for nil client or store")
	}

	if response != nil {
		t.Errorf("expected nil response, got %+v", response)
	}
}

func TestGetRegistrationByMeterIDSuccess(t *testing.T) {
	client := NewClient("http://localhost")
	poller := NewPoller(client)

	expectedRecord := &RegistrationRecord{
		ID:             "uuid-1",
		UserID:         "user-001",
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "APPROVED",
	}

	store := &mockStore{
		getRegister: func(ctx context.Context, meterID string) (*RegistrationRecord, error) {
			if meterID != "meter-001" {
				t.Errorf("expected meterID 'meter-001', got %s", meterID)
			}
			return expectedRecord, nil
		},
	}

	producer := &mockActuationProducer{}

	service := NewService(client, poller, store, producer, slog.Default())

	record, err := service.GetByMeterID(context.Background(), "meter-001")
	if err != nil {
		fmt.Println(err) // or handle error
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record == nil || record.MeterID != "meter-001" {
		t.Errorf("expected meter-001 registration record, got %+v", record)
	}
}

func TestGetRegistrationByUserIDSuccess(t *testing.T) {
	client := NewClient("http://localhost")
	poller := NewPoller(client)

	expectedRecord := &RegistrationRecord{
		ID:             "uuid-1",
		UserID:         "user-001",
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "DEWA_APPROVAL_PENDING",
	}

	store := &mockStore{
		getRegisterByUser: func(ctx context.Context, userReference string) (*RegistrationRecord, error) {
			if userReference != "user-001" {
				t.Errorf("expected userReference 'user-001', got %s", userReference)
			}
			return expectedRecord, nil
		},
	}
	producer := &mockActuationProducer{}

	service := NewService(client, poller, store, producer, slog.Default())

	record, err := service.GetRegistrationByUser(context.Background(), "user-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if record == nil || record.UserID != "user-001" {
		t.Errorf("expected user-001 registration record, got %+v", record)
	}

	if record.ApprovalStatus != "DEWA_APPROVAL_PENDING" {
		t.Errorf("expected approval status DEWA_APPROVAL_PENDING, got %s", record.ApprovalStatus)
	}
}

// dewaMustNotBeCalled fails the test if the service contacts DEWA.
func dewaMustNotBeCalled(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("DEWA was called: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	return server
}

func activeRegistration(registrationID string) func(context.Context, string) (*RegistrationRecord, error) {
	return func(context.Context, string) (*RegistrationRecord, error) {
		return &RegistrationRecord{
			RegistrationID: registrationID,
			MeterID:        "meter-first",
			UserID:         "user-001",
			Role:           "PRIMARY_OWNER",
			ApprovalStatus: "APPROVED",
		}, nil
	}
}

func TestRegisterHouseRefusesAccountThatAlreadyHasAMeter(t *testing.T) {
	server := dewaMustNotBeCalled(t)
	client := NewClient(server.URL)
	store := &mockStore{
		getActiveByUser: activeRegistration("reg-first"),
		saveFunc: func(context.Context, RegistrationRecord) error {
			t.Error("a second registration was saved")
			return nil
		},
	}
	service := NewService(client, NewPoller(client), store, nil, slog.Default())

	_, err := service.RegisterHouse(context.Background(), LinkRequest{
		RegistrationID: "reg-second",
		MeterID:        "meter-second",
		UserReference:  "user-001",
	})
	if !errors.Is(err, domain.ErrMeterAlreadyRegistered) {
		t.Fatalf("err = %v, want ErrMeterAlreadyRegistered", err)
	}
}

func TestReverifyRefusesNewRegistrationWhenAccountHasAMeter(t *testing.T) {
	server := dewaMustNotBeCalled(t)
	client := NewClient(server.URL)
	store := &mockStore{
		getActiveByUser: activeRegistration("reg-first"),
		getFunc: func(context.Context, string) (*RegistrationRecord, error) {
			return nil, fmt.Errorf("not found")
		},
		saveFunc: func(context.Context, RegistrationRecord) error {
			t.Error("a second registration was saved")
			return nil
		},
	}
	service := NewService(client, NewPoller(client), store, nil, slog.Default())

	_, err := service.ReverifyRegistration(context.Background(), "reg-second", "user-001", "meter-second")
	if !errors.Is(err, domain.ErrMeterAlreadyRegistered) {
		t.Fatalf("err = %v, want ErrMeterAlreadyRegistered", err)
	}
}

func TestReverifyRefusesOldRegistrationWhenAccountHasAnotherMeter(t *testing.T) {
	server := dewaMustNotBeCalled(t)
	client := NewClient(server.URL)
	store := &mockStore{
		getActiveByUser: activeRegistration("reg-first"),
		getFunc: func(context.Context, string) (*RegistrationRecord, error) {
			return &RegistrationRecord{RegistrationID: "reg-old", MeterID: "meter-old", UserID: "user-001", ApprovalStatus: "REJECTED"}, nil
		},
		updateFunc: func(context.Context, string, string) error {
			t.Error("the old registration was made pending again")
			return nil
		},
	}
	service := NewService(client, NewPoller(client), store, nil, slog.Default())

	_, err := service.ReverifyRegistration(context.Background(), "reg-old", "user-001", "meter-old")
	if !errors.Is(err, domain.ErrMeterAlreadyRegistered) {
		t.Fatalf("err = %v, want ErrMeterAlreadyRegistered", err)
	}
}

func TestReverifyAllowsTheAccountsOwnRegistration(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"PENDING","message":"Link request created"}`))
	}))
	defer server.Close()
	client := NewClient(server.URL)
	store := &mockStore{
		getActiveByUser: activeRegistration("reg-first"),
		getFunc: func(context.Context, string) (*RegistrationRecord, error) {
			return &RegistrationRecord{RegistrationID: "reg-first", MeterID: "meter-first", UserID: "user-001", ApprovalStatus: "DEWA_APPROVAL_PENDING"}, nil
		},
	}
	service := NewService(client, nil, store, nil, slog.Default())

	if _, err := service.ReverifyRegistration(context.Background(), "reg-first", "user-001", "meter-first"); err != nil {
		t.Fatalf("err = %v, want the account's own registration to be re-verified", err)
	}
	if !called {
		t.Error("DEWA was not asked to re-verify")
	}
}
