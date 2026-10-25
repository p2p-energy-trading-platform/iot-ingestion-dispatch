package config

import "testing"

func TestRetryMaxTriesBoundsViaLoad(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"zero", "0", true},
		{"negative", "-1", true},
		{"above limit", "11", true},
		{"minimum", "1", false},
		{"maximum", "10", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POSTGRES_URL", "postgres://u:p@localhost:5432/db")
			t.Setenv("DEWA_MOCK_APP_URL", "http://localhost:8081")
			t.Setenv("INGESTION_RETRY_MAX_TRIES", tt.value)

			_, err := Load()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
