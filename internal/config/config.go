package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	Retry     RetryConfig
	Input     InputConfig
	Metrics   MetricsConfig
	Tracing   TracingConfig
}

// InputConfig bounds what a single Kafka record may be before it is decoded.
type InputConfig struct {
	// MaxRecordBytes rejects larger record values before JSON decoding.
	MaxRecordBytes int
	// EnforceHouseKey requires the Kafka record key, when present, to name the
	// payload's house: either the bare house_id or the connector's MQTT topic
	// key gridx/{grid_id}/{house_id}/{kind} (01-kafka-consumption.md).
	EnforceHouseKey bool
	// MaxConcurrentHandlers bounds partition workers running handlers at once.
	MaxConcurrentHandlers int
}

type MetricsConfig struct {
	// Address serves Prometheus /metrics. Empty disables the listener.
	Address string
}

type TracingConfig struct {
	// Endpoint is the OTLP/gRPC collector address. Empty disables tracing.
	Endpoint string
	Insecure bool
}

// RetryConfig is the bounded exponential-backoff budget applied to each
// Kafka record. Permanent validation/provisioning errors skip it.
type RetryConfig struct {
	MaxTries        int
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

type ServiceConfig struct {
	Name            string
	Environment     string
	LogLevel        string
	ShutdownTimeout time.Duration
}

// IsProduction reports whether production-only safety rules apply.
func (s ServiceConfig) IsProduction() bool {
	switch strings.ToLower(s.Environment) {
	case "production", "prod":
		return true
	}
	return false
}

type KafkaConfig struct {
	Brokers        []string
	ConsumerGroup  string
	MeterTopic     string
	HeartbeatTopic string

	TLS         bool
	SASLUser    string
	SASLPass    string
	SASLMech    string // "", "PLAIN", "SCRAM-SHA-256" or "SCRAM-SHA-512"
	SessionTime time.Duration
	Heartbeat   time.Duration
	// FetchMaxBytes / MaxPartitionBytes cap broker fetch sizes. Zero keeps
	// the client defaults.
	FetchMaxBytes     int32
	MaxPartitionBytes int32
}

type PostgresConfig struct {
	URL string

	MaxConns          int32
	MinConns          int32
	ConnectTimeout    time.Duration
	StatementTimeout  time.Duration
	MaxConnLifetime   time.Duration
	ExpectedMigration int64 // zero means "whatever this binary embeds"
}

// RedisAckPolicy decides what happens to a record when TimescaleDB succeeded
// but the Redis projection update failed (01-kafka-consumption.md rule 4).
type RedisAckPolicy string

const (
	// RedisAckRequired leaves the record uncommitted until Redis succeeds.
	RedisAckRequired RedisAckPolicy = "required"
	// RedisAckAsyncRepair acknowledges after the durable write, counts a
	// degraded-cache event, and relies on the rebuild command / the next
	// reading to repair the projection.
	RedisAckAsyncRepair RedisAckPolicy = "async-repair"
)

// IsAsyncRepair reports whether Redis failures are tolerated after the
// durable write.
func (p RedisAckPolicy) IsAsyncRepair() bool { return p == RedisAckAsyncRepair }

type RedisConfig struct {
	Address  string
	Username string
	Password string
	DB       int

	TLS          bool
	TLSSkipVerif bool
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	AckPolicy    RedisAckPolicy
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

// Load reads configuration from the environment (and an optional .env file).
// Malformed values are startup errors: silently falling back to a default
// would hide a misconfiguration (11-production-readiness.md).
func Load() (*Config, error) {
	_ = godotenv.Load()

	r := &reader{}

	config := Config{
		Service: ServiceConfig{
			Name:            env("SERVICE_NAME", "iot-ingestion-dispatch"),
			Environment:     env("SERVICE_ENV", "development"),
			LogLevel:        env("LOG_LEVEL", "info"),
			ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers:           splitList(env("KAFKA_BROKER", "localhost:9092")),
			ConsumerGroup:     env("KAFKA_CONSUMER_GROUP", "iot-ingestion"),
			MeterTopic:        env("KAFKA_METER_TOPIC", "iot.meter-readings"),
			HeartbeatTopic:    env("KAFKA_HEARTBEAT_TOPIC", "iot.heartbeats"),
			TLS:               r.boolean("KAFKA_TLS", false),
			SASLUser:          os.Getenv("KAFKA_SASL_USER"),
			SASLPass:          os.Getenv("KAFKA_SASL_PASSWORD"),
			SASLMech:          strings.ToUpper(os.Getenv("KAFKA_SASL_MECHANISM")),
			SessionTime:       r.duration("KAFKA_SESSION_TIMEOUT", 45*time.Second),
			Heartbeat:         r.duration("KAFKA_HEARTBEAT_INTERVAL", 3*time.Second),
			FetchMaxBytes:     int32(r.integer("KAFKA_FETCH_MAX_BYTES", 0)),
			MaxPartitionBytes: int32(r.integer("KAFKA_MAX_PARTITION_FETCH_BYTES", 0)),
		},
		Postgres: PostgresConfig{
			URL:               os.Getenv("POSTGRES_URL"),
			MaxConns:          int32(r.integer("POSTGRES_MAX_CONNS", 10)),
			MinConns:          int32(r.integer("POSTGRES_MIN_CONNS", 1)),
			ConnectTimeout:    r.duration("POSTGRES_CONNECT_TIMEOUT", 10*time.Second),
			StatementTimeout:  r.duration("POSTGRES_STATEMENT_TIMEOUT", 15*time.Second),
			MaxConnLifetime:   r.duration("POSTGRES_MAX_CONN_LIFETIME", time.Hour),
			ExpectedMigration: int64(r.integer("POSTGRES_EXPECTED_MIGRATION", 0)),
		},

		Redis: RedisConfig{
			Address:      env("REDIS_ADDRESS", "localhost:6379"),
			Username:     os.Getenv("REDIS_USERNAME"),
			Password:     os.Getenv("REDIS_PASSWORD"),
			DB:           r.integer("REDIS_DB", 0),
			TLS:          r.boolean("REDIS_TLS", false),
			TLSSkipVerif: r.boolean("REDIS_TLS_INSECURE_SKIP_VERIFY", false),
			DialTimeout:  r.duration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  r.duration("REDIS_READ_TIMEOUT", 2*time.Second),
			WriteTimeout: r.duration("REDIS_WRITE_TIMEOUT", 2*time.Second),
			PoolSize:     r.integer("REDIS_POOL_SIZE", 20),
			MinIdleConns: r.integer("REDIS_MIN_IDLE_CONNS", 2),
			MaxRetries:   r.integer("REDIS_MAX_RETRIES", 2),
			AckPolicy:    RedisAckPolicy(env("REDIS_ACK_POLICY", string(RedisAckRequired))),
		},

		GRPC: GRPCConfig{
			Address: env("GRPC_ADDRESS", ":50051"),
		},

		Health: HealthConfig{
			Address: env("HEALTH_ADDRESS", ":8080"),
		},

		Admission: AdmissionConfig{
			RefreshInterval: r.duration("ADMISSION_REFRESH_INTERVAL", 120*time.Second),
		},

		Retry: RetryConfig{
			MaxTries:        r.integer("INGESTION_RETRY_MAX_TRIES", 3),
			InitialInterval: r.duration("INGESTION_RETRY_INITIAL_INTERVAL", 100*time.Millisecond),
			MaxInterval:     r.duration("INGESTION_RETRY_MAX_INTERVAL", 2*time.Second),
		},

		Input: InputConfig{
			MaxRecordBytes:        r.integer("INGESTION_MAX_RECORD_BYTES", 64*1024),
			EnforceHouseKey:       r.boolean("INGESTION_ENFORCE_HOUSE_KEY", false),
			MaxConcurrentHandlers: r.integer("INGESTION_MAX_CONCURRENT_HANDLERS", 16),
		},

		Metrics: MetricsConfig{
			Address: env("METRICS_ADDRESS", ":9090"),
		},

		Tracing: TracingConfig{
			Endpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			Insecure: r.boolean("OTEL_EXPORTER_OTLP_INSECURE", true),
		},
	}

	if err := r.err(); err != nil {
		return nil, err
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

func splitList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// reader parses typed environment values and remembers every malformed one,
// so Load can report them all at once instead of using a default.
type reader struct {
	errs []error
}

func (r *reader) err() error { return errors.Join(r.errs...) }

func (r *reader) integer(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not an integer", key, value))
		return fallback
	}
	return parsed
}

// duration uses Go duration syntax (e.g. "120s", "2m").
func (r *reader) duration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a duration (e.g. \"2s\")", key, value))
		return fallback
	}
	return parsed
}

func (r *reader) boolean(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a boolean", key, value))
		return fallback
	}
	return parsed
}
