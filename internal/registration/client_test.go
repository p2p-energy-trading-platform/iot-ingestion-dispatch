package registration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateLinkRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}

			if r.URL.Path != "/api/link-requests" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}

			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected Content-Type application/json")
			}

			if r.Header.Get("Accept") != "application/json" {
				t.Errorf("expected Accept application/json")
			}

			var request LinkRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("failed to decode request body: %v", err)
				return
			}

			if request.RegistrationID != "reg-123" {
				t.Errorf("unexpected registrationId: %s", request.RegistrationID)
			}

			if request.MeterID != "meter-001" {
				t.Errorf("unexpected meterId: %s", request.MeterID)
			}

			if request.UserReference != "user-001" {
				t.Errorf("unexpected userReference: %s", request.UserReference)
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

	request := LinkRequest{
		RegistrationID: "reg-123",
		MeterID:        "meter-001",
		UserReference:  "user-001",
	}

	response, err := client.CreateLinkRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("CreateLinkRequest returned an error: %v", err)
	}

	if response.Status != "PENDING" {
		t.Errorf("expected status PENDING, got %s", response.Status)
	}

	if response.Message != "Link request created" {
		t.Errorf("unexpected message: %s", response.Message)
	}
}
