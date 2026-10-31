package registration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockStore struct {
	saveFunc          func(ctx context.Context, reg RegistrationRecord) error
	getFunc           func(ctx context.Context, registrationID string) (*RegistrationRecord, error)
	updateFunc        func(ctx context.Context, registrationID string, status string) error
	getRegister       func(ctx context.Context, meterID string) (*RegistrationRecord, error)
	getRegisterByUser func(ctx context.Context, userReference string) (*RegistrationRecord, error)
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

func TestRegisterHouseSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}

			if r.URL.Path != "/api/link-requests" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}

			var request LinkRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("failed to decode request: %v", err)
				return
			}

			if request.RegistrationID != "reg-123" {
				t.Errorf("unexpected registration ID: %s", request.RegistrationID)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)

			// Updated to include requestId matching the modified DEWA mock contract
			_, _ = w.Write([]byte(
				`{"requestId":"server-uuid-123","status":"PENDING","message":"Link request created"}`,
			))
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

	service := NewService(client, poller, store)

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
	service := NewService(client, nil, store)

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
	service := NewService(client, nil, store)

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
	service := NewService(client, nil, store)

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
	service := NewService(nil, nil, nil)

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

	service := NewService(client, poller, store)

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

	service := NewService(client, poller, store)

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
