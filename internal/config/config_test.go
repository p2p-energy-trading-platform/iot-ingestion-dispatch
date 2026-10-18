package config

import (
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid configuration",
			config: Config{
				Kafka: KafkaConfig{
					Brokers: []string{
						"localhost:9092",
					},
				},
				Postgres: PostgresConfig{
					URL: "postgres://user:password@localhost:5432/test",
				},
				Retry: RetryConfig{
					MaxTries:        3,
					InitialInterval: 100,
					MaxInterval:     200,
				},
			},
			wantErr: false,
		},
		{
			name: "missing kafka brokers",
			config: Config{
				Postgres: PostgresConfig{
					URL: "postgres://user:password@localhost:5432/test",
				},
			},
			wantErr: true,
		},
		{
			name: "missing postgres url",
			config: Config{
				Kafka: KafkaConfig{
					Brokers: []string{
						"localhost:9092",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "retry max tries zero",
			config: Config{
				Kafka: KafkaConfig{
					Brokers: []string{"localhost:9092"},
				},
				Postgres: PostgresConfig{
					URL: "postgres://user:password@localhost:5432/test",
				},
				Retry: RetryConfig{
					MaxTries:        0,
					InitialInterval: 100,
					MaxInterval:     200,
				},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				err := test.config.Validate()

				if test.wantErr && err == nil {
					t.Fatal(
						"expected validation error, got nil",
					)
				}

				if !test.wantErr && err != nil {
					t.Fatalf(
						"expected no validation error, got %v",
						err,
					)
				}
			},
		)
	}
}

func validLoadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_URL", "postgres://u:p@localhost:5432/db")
}

func TestLoadRejectsMalformedValues(t *testing.T) {
	tests := []struct{ key, value string }{
		{"REDIS_DB", "abc"},
		{"ADMISSION_REFRESH_INTERVAL", "soon"},
		{"INGESTION_RETRY_INITIAL_INTERVAL", "100"},
		{"INGESTION_MAX_RECORD_BYTES", "1MB"},
		{"KAFKA_TLS", "maybe"},
		{"SHUTDOWN_TIMEOUT", "-"},
		{"POSTGRES_MAX_CONNS", "4294967296"},
		{"KAFKA_FETCH_MAX_BYTES", "99999999999"},
		{"POSTGRES_EXPECTED_MIGRATION", "v9"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			validLoadEnv(t)
			t.Setenv(tt.key, tt.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() accepted %s=%q; a malformed value must fail startup, not fall back", tt.key, tt.value)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("error %q does not name %s", err, tt.key)
			}
		})
	}
}

func TestLoadReportsEveryMalformedValue(t *testing.T) {
	validLoadEnv(t)
	t.Setenv("REDIS_DB", "x")
	t.Setenv("KAFKA_TLS", "y")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "REDIS_DB") || !strings.Contains(err.Error(), "KAFKA_TLS") {
		t.Fatalf("error = %v, want both settings reported", err)
	}
}

func TestLoadDefaults(t *testing.T) {
	validLoadEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admission.RefreshInterval != 120*time.Second {
		t.Errorf("refresh interval = %v, want 120s", cfg.Admission.RefreshInterval)
	}
	if cfg.Redis.AckPolicy != RedisAckRequired {
		t.Errorf("ack policy = %q, want required", cfg.Redis.AckPolicy)
	}
	if cfg.Input.MaxRecordBytes <= 0 {
		t.Errorf("max record bytes = %d, want a positive default", cfg.Input.MaxRecordBytes)
	}
}

func TestProductionRules(t *testing.T) {
	base := func() Config {
		return Config{
			Service:  ServiceConfig{Environment: "production"},
			Kafka:    KafkaConfig{Brokers: []string{"k:9093"}, TLS: true},
			Postgres: PostgresConfig{URL: "postgres://u:p@db/x?sslmode=verify-full"},
			Redis:    RedisConfig{TLS: true, AckPolicy: RedisAckRequired},
			Retry:    RetryConfig{MaxTries: 3, InitialInterval: 1, MaxInterval: 2},
		}
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("a fully secured production config must validate: %v", err)
	}

	mutations := map[string]func(*Config){
		"plaintext kafka":      func(c *Config) { c.Kafka.TLS = false },
		"plaintext redis":      func(c *Config) { c.Redis.TLS = false },
		"skip redis verify":    func(c *Config) { c.Redis.TLSSkipVerif = true },
		"postgres sslmode off": func(c *Config) { c.Postgres.URL = "postgres://u:p@db/x?sslmode=disable" },
		"implicit ack policy":  func(c *Config) { c.Redis.AckPolicy = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := base()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected a production validation error")
			}
		})
	}
}

func TestValidateRejectsContradictions(t *testing.T) {
	ok := Config{
		Kafka:    KafkaConfig{Brokers: []string{"k:9092"}},
		Postgres: PostgresConfig{URL: "postgres://u:p@db/x"},
		Retry:    RetryConfig{MaxTries: 3, InitialInterval: 1, MaxInterval: 2},
	}
	mutations := map[string]func(*Config){
		"unknown ack policy":     func(c *Config) { c.Redis.AckPolicy = "maybe" },
		"min conns over max":     func(c *Config) { c.Postgres.MinConns, c.Postgres.MaxConns = 5, 2 },
		"idle conns over pool":   func(c *Config) { c.Redis.PoolSize, c.Redis.MinIdleConns = 2, 5 },
		"sasl mech without user": func(c *Config) { c.Kafka.SASLMech = "PLAIN" },
		"unknown sasl mech":      func(c *Config) { c.Kafka.SASLMech, c.Kafka.SASLUser = "GSSAPI", "u" },
		"heartbeat vs session":   func(c *Config) { c.Kafka.SessionTime, c.Kafka.Heartbeat = 6*time.Second, 5*time.Second },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := ok
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}
