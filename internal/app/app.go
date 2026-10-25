package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/observability"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/query"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"
	grpcservices "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc/services"
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
	metrics      *observability.Metrics
	metricsHTTP  *httphealth.MetricsServer
	stopTracing  func(context.Context) error

	admissionRegistry  *admission.Registry
	admissionRefresher *admission.Refresher
}

func New(
	context context.Context,
	config *config.Config,
	logger *slog.Logger,
) (*App, error) {
	metrics := observability.NewMetrics()

	stopTracing, err := observability.SetupTracing(
		context, config.Service.Name, config.Tracing.Endpoint, config.Tracing.Insecure,
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}

	postgres, err := postgresstore.NewWithPool(context, config.Postgres.URL, postgresstore.PoolConfig{
		MaxConns:         config.Postgres.MaxConns,
		MinConns:         config.Postgres.MinConns,
		ConnectTimeout:   config.Postgres.ConnectTimeout,
		StatementTimeout: config.Postgres.StatementTimeout,
		MaxConnLifetime:  config.Postgres.MaxConnLifetime,
	})
	if err != nil {
		_ = stopTracing(context)
		return nil, fmt.Errorf("postgres: %w", err)
	}

	redis, err := redisstore.New(
		context,
		redisstore.Config{
			Address:            config.Redis.Address,
			Username:           config.Redis.Username,
			Password:           config.Redis.Password,
			DB:                 config.Redis.DB,
			TLS:                config.Redis.TLS,
			TLSInsecureSkipVer: config.Redis.TLSSkipVerif,
			DialTimeout:        config.Redis.DialTimeout,
			ReadTimeout:        config.Redis.ReadTimeout,
			WriteTimeout:       config.Redis.WriteTimeout,
			PoolSize:           config.Redis.PoolSize,
			MinIdleConns:       config.Redis.MinIdleConns,
			MaxRetries:         config.Redis.MaxRetries,
		},
	)
	if err != nil {
		postgres.Close()
		_ = stopTracing(context)
		return nil, fmt.Errorf("redis: %w", err)
	}

	if err := checkRedisEviction(context, redis, config, logger); err != nil {
		_ = redis.Close()
		postgres.Close()
		_ = stopTracing(context)
		return nil, err
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
			Metrics:  metrics,
		},
		logger,
	)

	// Handlers are registered before the consumer exists, so the router is
	// complete before anything can dispatch to it. A shared topic name would
	// make the second Register silently replace the first, so reject it.
	if config.Kafka.MeterTopic == config.Kafka.HeartbeatTopic {
		_ = redis.Close()
		postgres.Close()
		_ = stopTracing(context)
		return nil, fmt.Errorf("kafka: meter and heartbeat topics must differ, both are %q",
			config.Kafka.MeterTopic)
	}

	limits := ingestion.InputLimits{
		MaxRecordBytes:  config.Input.MaxRecordBytes,
		EnforceHouseKey: config.Input.EnforceHouseKey,
	}
	cachePolicy := ingestion.CacheRequired
	if config.Redis.AckPolicy.IsAsyncRepair() {
		cachePolicy = ingestion.CacheAsyncRepair
	}

	router := ingestion.NewRouter()
	router.Register(
		config.Kafka.MeterTopic,
		ingestion.NewMeterHandler(admissionRegistry, postgres, redis, logger).
			WithInputLimits(limits).
			WithCachePolicy(cachePolicy, config.Kafka.MeterTopic, metrics),
	)
	router.Register(
		config.Kafka.HeartbeatTopic,
		ingestion.NewHeartbeatHandler(admissionRegistry, postgres, redis, logger).
			WithInputLimits(limits).
			WithCachePolicy(cachePolicy, config.Kafka.HeartbeatTopic, metrics),
	)

	securityOpts, err := kafkaSecurityOpts(config.Kafka)
	if err != nil {
		_ = redis.Close()
		postgres.Close()
		_ = stopTracing(context)
		return nil, fmt.Errorf("kafka: %w", err)
	}

	consumer, err := ingestion.NewConsumer(
		ingestion.Config{
			Brokers:        config.Kafka.Brokers,
			ConsumerGroup:  config.Kafka.ConsumerGroup,
			MeterTopic:     config.Kafka.MeterTopic,
			HeartbeatTopic: config.Kafka.HeartbeatTopic,
			Retry: ingestion.RetryPolicy{
				MaxTries:        config.Retry.MaxTries,
				InitialInterval: config.Retry.InitialInterval,
				MaxInterval:     config.Retry.MaxInterval,
			},
			MaxConcurrentHandlers: config.Input.MaxConcurrentHandlers,
			Metrics:               metrics,
			SessionTimeout:        config.Kafka.SessionTime,
			HeartbeatInterval:     config.Kafka.Heartbeat,
			FetchMaxBytes:         config.Kafka.FetchMaxBytes,
			MaxPartitionBytes:     config.Kafka.MaxPartitionBytes,
			ExtraOpts:             securityOpts,
		},
		router,
		postgres,
		logger,
	)
	if err != nil {
		_ = redis.Close()
		postgres.Close()
		_ = stopTracing(context)
		return nil, fmt.Errorf("kafka: %w", err)
	}

	grpcServer := grpctransport.New(
		config.GRPC.Address,
		logger,
		grpctransport.Services{
			Iot: grpcservices.NewIotHandler(query.NewService(postgres, postgres, redis, redis), logger),
		},
	)

	healthServer := httphealth.New(
		config.Health.Address,
	)

	application := &App{
		config:      config,
		logger:      logger,
		postgres:    postgres,
		redis:       redis,
		consumer:    consumer,
		router:      router,
		grpc:        grpcServer,
		health:      healthServer,
		metrics:     metrics,
		stopTracing: stopTracing,

		admissionRegistry:  admissionRegistry,
		admissionRefresher: admissionRefresher,
	}
	if config.Metrics.Address != "" {
		application.metricsHTTP = httphealth.NewMetrics(config.Metrics.Address, metrics.Handler())
	}
	application.registerPoolMetrics()

	return application, nil
}

// checkRedisEviction enforces the 02-redis-hot-storage.md rule that an
// allkeys-* eviction policy must not silently drop liveness or latest-state
// keys. A server that forbids CONFIG GET (common on managed Redis) cannot be
// checked, which is logged instead of failing startup.
func checkRedisEviction(ctx context.Context, redis *redisstore.Store, cfg *config.Config, logger *slog.Logger) error {
	policy, err := redis.EvictionPolicy(ctx)
	if err != nil {
		logger.Warn("could not read redis maxmemory-policy; verify it is noeviction or volatile-*", "error", err)
		return nil
	}
	if redisstore.LivenessAndLatestSafe(policy) {
		return nil
	}
	if cfg.Service.IsProduction() {
		return fmt.Errorf("redis maxmemory-policy %q can evict liveness/latest keys; use noeviction or a volatile-* policy", policy)
	}
	logger.Warn("redis eviction policy may drop liveness or latest-state keys", "policy", policy)
	return nil
}
