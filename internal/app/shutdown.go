package app

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (app *App) shutdown() error {
	app.logger.Info("Shutting down application")

	shutdownTimeout := app.config.Service.ShutdownTimeout

	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)

	defer cancel()

	var errs []error

	// NOTE: Call Kafka stop logic
	// app.logger.Info("Stopping Kafka consumer")

	app.logger.Info("Stopping grpc server")
	app.grpc.Stop()

	app.logger.Info("Stopping health server")
	if err := app.health.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("close health server: %w", err))
	}

	app.logger.Info("Closing redis")
	if err := app.redis.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close redis: %w", err))
	}

	app.logger.Info("Closing postgres")
	app.postgres.Close()

	app.logger.Info("Application shutdown complete")

	return errors.Join(errs...)
}
