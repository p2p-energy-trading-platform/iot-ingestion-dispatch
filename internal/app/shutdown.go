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

	app.health.SetReady(false)

	// The run context is already cancelled by the time we get here, so Run
	// is returning; wait for it, then close the client. Closing stops
	// workers gracefully (committing finished work) and leaves the group.
	app.logger.Info("Stopping Kafka consumer")

	if app.consumerDone != nil {
		select {
		case <-app.consumerDone:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("wait for kafka consumer: %w", ctx.Err()))
		}
	}

	closed := make(chan struct{})
	go func() {
		app.consumer.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("close kafka consumer: %w", ctx.Err()))
	}

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
