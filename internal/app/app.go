package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/httphealth"
)

type App struct {
	config       *config.Config
	logger       *slog.Logger
	postgres     *postgresstore.Store
	redis        *redisstore.Store
	consumer     *ingestion.Consumer
	consumerDone chan struct{}
	router       *ingestion.Router
	grpc         *grpctransport.Server
	health       *httphealth.Server

	admissionRegistry  *admission.Registry
	admissionRefresher *admission.Refresher
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

	// postgres satisfies admission.GridLoader directly (see
	// internal/store/postgres/grid_loader.go's LoadGrids method) - no
	// separate loader type needed.
	admissionRegistry := admission.NewRegistry()
	admissionRefresher := admission.NewRefresher(
		admissionRegistry,
		postgres,
		admission.RefresherConfig{
			Interval: config.Admission.RefreshInterval,
		},
		logger,
	)

	// Handlers are registered before the consumer exists, so the router is
	// complete before anything can dispatch to it. A shared topic name would
	// make the second Register silently replace the first, so reject it.
	if config.Kafka.MeterTopic == config.Kafka.HeartbeatTopic {
		_ = redis.Close()
		postgres.Close()
		return nil, fmt.Errorf("kafka: meter and heartbeat topics must differ, both are %q",
			config.Kafka.MeterTopic)
	}

	router := ingestion.NewRouter()
	router.Register(
		config.Kafka.MeterTopic,
		ingestion.NewMeterHandler(admissionRegistry, postgres, redis, logger),
	)
	router.Register(
		config.Kafka.HeartbeatTopic,
		ingestion.NewHeartbeatHandler(admissionRegistry, postgres, redis, logger),
	)

	consumer, err := ingestion.NewConsumer(
		ingestion.Config{
			Brokers:        config.Kafka.Brokers,
			ConsumerGroup:  config.Kafka.ConsumerGroup,
			MeterTopic:     config.Kafka.MeterTopic,
			HeartbeatTopic: config.Kafka.HeartbeatTopic,
		},
		router,
		logger,
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
		router:   router,
		grpc:     grpcServer,
		health:   healthServer,

		admissionRegistry:  admissionRegistry,
		admissionRefresher: admissionRefresher,
	}, nil
}
