package config

import (
	"testing"
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
