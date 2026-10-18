
package registration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateLinkRequestPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Check HTTP method.
			if r.Method != http.MethodPost {
				t.Errorf("expected method POST, got %s", r.Method)
			}

			// Check endpoint.
			if r.URL.Path != "/api/link-requests" {
				t.Errorf("expected endpoint /api/link-requests, got %s", r.URL.Path)
			}

			// Check content type.
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("expected Content-Type application/json, got %s", got)
			}

			// Decode the request JSON.
			var payload map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("failed to decode request body: %v", err)
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}

			// Check that exactly three JSON fields are sent.
			if len(payload) != 3 {
				t.Errorf("expected exactly 3 JSON fields, got %d", len(payload))
			}

			// Check the exact JSON field names and values.
			expected := map[string]string{
				"registrationId": "REG-123",
				"meterId":        "METER-001",
				"userReference":  "USER-123",
			}

			for key, want := range expected {
				got, ok := payload[key]
				if !ok {
					t.Errorf("missing JSON field %q", key)
					continue
				}

				if got != want {
					t.Errorf("field %q: expected %q, got %v", key, want, got)
				}
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(
				`{"status":"PENDING","message":"Link request created"}`,
			))
		},
	))
	defer server.Close()

	client := NewDEWAClient(server.URL)

	payload := ClaimPayload{
		RegistrationID: "REG-123",
		MeterID:        "METER-001",
		UserReference:  "USER-123",
	}

	result, err := client.CreateLinkRequest(context.Background(), payload)
	if err != nil {
		t.Fatalf("CreateLinkRequest returned error: %v", err)
	}

	if result.Status != "PENDING" {
		t.Errorf("expected status PENDING, got %s", result.Status)
	}

	if result.Message != "Link request created" {
		t.Errorf("unexpected response message: %s", result.Message)
	}
}
