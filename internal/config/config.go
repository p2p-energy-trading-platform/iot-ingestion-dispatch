package config

import (
	"os"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Service  ServiceConfig
	Kafka    KafkaConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	GRPC     GRPCConfig
	Health   HealthConfig
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
			Address: env("REDIS_ADDRESS", "localhost:6379"),
		},

		GRPC: GRPCConfig{
			Address: env("GRPC_ADDRESS", ":50051"),
		},

		Health: HealthConfig{
			Address: env("HEALTH_ADDRESS", ":8080"),
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
