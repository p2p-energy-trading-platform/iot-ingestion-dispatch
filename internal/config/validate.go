package config

import "errors"

func (config Config) Validate() error {
	if len(config.Kafka.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}

	if config.Postgres.URL == "" {
		return errors.New("POSTGRES_URL is required")
	}

	return nil
}
