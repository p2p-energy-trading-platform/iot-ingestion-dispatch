
func TestCreateLinkRequestHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "DEWA service unavailable", http.StatusInternalServerError)
		},
	))
	defer server.Close()

	client := NewDEWAClient(server.URL)

	_, err := client.CreateLinkRequest(context.Background(), ClaimPayload{
		RegistrationID: "REG-123",
		MeterID:        "METER-001",
		UserReference:  "USER-123",
	})

	if err == nil {
		t.Fatal("expected error for unsuccessful HTTP response")
	}
}

func TestCreateLinkRequestMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":`))
		},
	))
	defer server.Close()

	client := NewDEWAClient(server.URL)

	_, err := client.CreateLinkRequest(context.Background(), ClaimPayload{
		RegistrationID: "REG-123",
		MeterID:        "METER-001",
		UserReference:  "USER-123",
	})

	if err == nil {
		t.Fatal("expected error for malformed response JSON")
	}
}
