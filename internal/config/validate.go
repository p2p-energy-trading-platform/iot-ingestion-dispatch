package config

import (
	"errors"
	"fmt"
	"strings"
)

// Validate rejects missing, out-of-range and contradictory settings so a bad
// deployment fails at startup instead of misbehaving under load.
func (config Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(config.Kafka.Brokers) == 0 {
		add("at least one Kafka broker is required")
	}
	if config.Postgres.URL == "" {
		add("POSTGRES_URL is required")
	}

	if config.Retry.MaxTries < 1 || config.Retry.MaxTries > 10 {
		add("INGESTION_RETRY_MAX_TRIES must be between 1 and 10")
	}
	if config.Retry.InitialInterval <= 0 {
		add("INGESTION_RETRY_INITIAL_INTERVAL must be greater than 0")
	}
	if config.Retry.MaxInterval < config.Retry.InitialInterval {
		add("INGESTION_RETRY_MAX_INTERVAL must be at least INGESTION_RETRY_INITIAL_INTERVAL")
	}

	// Zero values mean "Config built by hand" (tests); Load always sets them.
	if config.Input.MaxRecordBytes < 0 {
		add("INGESTION_MAX_RECORD_BYTES must not be negative")
	}
	if config.Input.MaxConcurrentHandlers < 0 {
		add("INGESTION_MAX_CONCURRENT_HANDLERS must not be negative")
	}

	switch config.Redis.AckPolicy {
	case "", RedisAckRequired, RedisAckAsyncRepair:
	default:
		add("REDIS_ACK_POLICY must be %q or %q, got %q", RedisAckRequired, RedisAckAsyncRepair, config.Redis.AckPolicy)
	}
	if config.Redis.PoolSize < 0 || config.Redis.MinIdleConns < 0 || config.Redis.MaxRetries < 0 {
		add("REDIS_POOL_SIZE, REDIS_MIN_IDLE_CONNS and REDIS_MAX_RETRIES must not be negative")
	}
	if config.Redis.PoolSize > 0 && config.Redis.MinIdleConns > config.Redis.PoolSize {
		add("REDIS_MIN_IDLE_CONNS must not exceed REDIS_POOL_SIZE")
	}

	if config.Postgres.MaxConns < 0 || config.Postgres.MinConns < 0 {
		add("POSTGRES_MAX_CONNS and POSTGRES_MIN_CONNS must not be negative")
	}
	if config.Postgres.MaxConns > 0 && config.Postgres.MinConns > config.Postgres.MaxConns {
		add("POSTGRES_MIN_CONNS must not exceed POSTGRES_MAX_CONNS")
	}

	switch config.Kafka.SASLMech {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		add("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, got %q", config.Kafka.SASLMech)
	}
	if (config.Kafka.SASLMech == "") != (config.Kafka.SASLUser == "") {
		add("KAFKA_SASL_MECHANISM and KAFKA_SASL_USER must be set together")
	}
	if config.Kafka.SessionTime > 0 && config.Kafka.Heartbeat > 0 && config.Kafka.Heartbeat*3 > config.Kafka.SessionTime {
		add("KAFKA_HEARTBEAT_INTERVAL must be at most a third of KAFKA_SESSION_TIMEOUT")
	}

	if config.Service.ShutdownTimeout < 0 {
		add("SHUTDOWN_TIMEOUT must not be negative")
	}

	if config.Service.IsProduction() {
		if config.Redis.AckPolicy == "" {
			add("REDIS_ACK_POLICY must be set explicitly in production")
		}
		if !config.Kafka.TLS && config.Kafka.SASLMech == "" {
			add("production requires KAFKA_TLS=true or SASL authentication")
		}
		if !config.Redis.TLS {
			add("production requires REDIS_TLS=true")
		}
		if config.Redis.TLSSkipVerif {
			add("REDIS_TLS_INSECURE_SKIP_VERIFY is development-only")
		}
		if strings.Contains(strings.ToLower(config.Postgres.URL), "sslmode=disable") {
			add("production requires TLS to PostgreSQL (sslmode must not be disable)")
		}
	}

	return errors.Join(errs...)
}
