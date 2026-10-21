package registration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

			_, _ = w.Write([]byte(
				`{"status":"PENDING","message":"Link request created"}`,
			))
		},
	))
	defer server.Close()

	client := NewClient(server.URL)
	service := NewService(client)

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
}

func TestRegisterHouseMissingRegistrationID(t *testing.T) {
	service := NewService(NewClient("http://localhost"))

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
	service := NewService(NewClient("http://localhost"))

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
	service := NewService(NewClient("http://localhost"))

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

func TestRegisterHouseNilClient(t *testing.T) {
	service := NewService(nil)

	request := LinkRequest{
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		UserReference:  "user-001",
	}

	response, err := service.RegisterHouse(context.Background(), request)

	if err == nil {
		t.Fatal("expected an error for nil client")
	}

	if response != nil {
		t.Errorf("expected nil response, got %+v", response)
	}
}
