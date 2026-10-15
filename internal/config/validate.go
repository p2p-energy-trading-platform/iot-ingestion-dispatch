package config

import "errors"

func (config Config) Validate() error {
	if len(config.Kafka.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}

	if config.Postgres.URL == "" {
		return errors.New("POSTGRES_URL is required")
	}

	if config.Retry.MaxTries < 1 || config.Retry.MaxTries > 10 {
		return errors.New("INGESTION_RETRY_MAX_TRIES must be between 1 and 10")
	}
	if config.Retry.InitialInterval <= 0 {
		return errors.New("INGESTION_RETRY_INITIAL_INTERVAL must be greater than 0")
	}
	if config.Retry.MaxInterval < config.Retry.InitialInterval {
		return errors.New("INGESTION_RETRY_MAX_INTERVAL must be at least INGESTION_RETRY_INITIAL_INTERVAL")
	}

	return nil
}
