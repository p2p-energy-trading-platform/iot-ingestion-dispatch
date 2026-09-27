package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Service   ServiceConfig
	Kafka     KafkaConfig
	Postgres  PostgresConfig
	Redis     RedisConfig
	GRPC      GRPCConfig
	Health    HealthConfig
	Admission AdmissionConfig
}

type ServiceConfig struct {
	Name            string
	Environment     string
	LogLevel        string
	ShutdownTimeout time.Duration
}

type KafkaConfig struct {
	Brokers        []string
	ConsumerGroup  string
	MeterTopic     string
	HeartbeatTopic string
}

type PostgresConfig struct {
	URL string
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
}

type GRPCConfig struct {
	Address string
}

type HealthConfig struct {
	Address string
}

// AdmissionConfig controls the grid-registry startup bootstrap and
// periodic refresher - see 05-startup-registry.md.
type AdmissionConfig struct {
	// RefreshInterval between periodic full-snapshot grid registry
	// refreshes. Defaults to 120s per the documented recommendation;
	// overridable via ADMISSION_REFRESH_INTERVAL (Go duration syntax,
	// e.g. "120s", "2m").
	RefreshInterval time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	config := Config{
		Service: ServiceConfig{
			Name:            env("SERVICE_NAME", "iot-ingestion-dispatch"),
			Environment:     env("SERVICE_ENV", "development"),
			LogLevel:        env("LOG_LEVEL", "info"),
			ShutdownTimeout: 30 * time.Second,
		},
		Kafka: KafkaConfig{
			Brokers: []string{
				env("KAFKA_BROKER", "localhost:9092"),
			},
			ConsumerGroup:  env("KAFKA_CONSUMER_GROUP", "iot-ingestion"),
			MeterTopic:     env("KAFKA_METER_TOPIC", "iot.meter-readings"),
			HeartbeatTopic: env("KAFKA_HEARTBEAT_TOPIC", "iot.heartbeats"),
		},
		Postgres: PostgresConfig{
			URL: os.Getenv("POSTGRES_URL"),
		},

		Redis: RedisConfig{
			Address:  env("REDIS_ADDRESS", "localhost:6379"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       envInt("REDIS_DB", 0),
		},

		GRPC: GRPCConfig{
			Address: env("GRPC_ADDRESS", ":50051"),
		},

		Health: HealthConfig{
			Address: env("HEALTH_ADDRESS", ":8080"),
		},

		Admission: AdmissionConfig{
			RefreshInterval: envDuration("ADMISSION_REFRESH_INTERVAL", 120*time.Second),
		},
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value != "" {
		return value
	}

	return fallback
}

// envInt reads key as an integer, falling back to fallback if unset or
// unparsable.
func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

// envDuration reads key using Go duration syntax (e.g. "120s", "2m"),
// falling back to fallback if unset or unparsable.
func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}
