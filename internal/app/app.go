package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/redis"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/httphealth"
)

type App struct {
	config   *config.Config
	logger   *slog.Logger
	postgres *postgresstore.Store
	redis    *redisstore.Store
	consumer *ingestion.Consumer
	grpc     *grpctransport.Server
	health   *httphealth.Server
}

func New(
	context context.Context,
	config *config.Config,
	logger *slog.Logger,
) (*App, error) {
	postgres, err := postgresstore.New(context, config.Postgres.URL)

	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}

	redis, err := redisstore.New(
		context,
		redisstore.Config{
			Address:  config.Redis.Address,
			Password: config.Redis.Password,
			DB:       config.Redis.DB,
		},
	)

	if err != nil {
		postgres.Close()

		return nil, fmt.Errorf("redis: %w", err)
	}

	consumer, err := ingestion.NewConsumer(
		ingestion.Config{
			Brokers:        config.Kafka.Brokers,
			ConsumerGroup:  config.Kafka.ConsumerGroup,
			MeterTopic:     config.Kafka.MeterTopic,
			HeartbeatTopic: config.Kafka.HeartbeatTopic,
		},
	)

	if err != nil {
		_ = redis.Close()
		postgres.Close()

		return nil, fmt.Errorf("kafka: %w", err)
	}

	grpcServer := grpctransport.New(
		config.GRPC.Address,
	)

	healthServer := httphealth.New(
		config.Health.Address,
	)

	return &App{
		config:   config,
		logger:   logger,
		postgres: postgres,
		redis:    redis,
		consumer: consumer,
		grpc:     grpcServer,
		health:   healthServer,
	}, nil
}
