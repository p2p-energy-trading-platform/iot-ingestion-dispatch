package app

import (
	"context"
	"errors"
	"fmt"
)

func (app *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// One slot per component goroutine: health, grpc, kafka consumer.
	errorChannel := make(chan error, 3)

	// Step 2: ensure migrations are at the expected version before doing
	// anything else. This service never applies migrations itself
	// (cmd/migrate is the single owner of that) - it only verifies the
	// database it's about to depend on already matches what this binary
	// was built against. A mismatch fails closed.
	if err := app.ensureMigrationsCurrent(ctx); err != nil {
		return fmt.Errorf("migration version check: %w", err)
	}

	app.logger.Info("starting health server",
		"address", app.config.Health.Address,
	)

	go func() {
		if err := app.health.Run(); err != nil {
			errorChannel <- fmt.Errorf("health error: %w", err)
		}
	}()

	app.logger.Info("starting gRPC server", "address", app.config.GRPC.Address)

	go func() {
		if err := app.grpc.Run(); err != nil {
			errorChannel <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	// Steps 3-6: load provisioned grids, validate, publish the immutable
	// snapshot atomically, and start the periodic refresher - per
	// 05-startup-registry.md. Must succeed before ingestion is allowed to
	// start (step 7 below).
	app.logger.Info("bootstrapping grid registry")

	if err := app.admissionRefresher.Bootstrap(ctx); err != nil {
		return fmt.Errorf("grid registry bootstrap: %w", err)
	}

	go app.admissionRefresher.Start(ctx)

	app.logger.Info("starting Kafka consumer",
		"group", app.config.Kafka.ConsumerGroup,
		"topics", []string{
			app.config.Kafka.MeterTopic,
			app.config.Kafka.HeartbeatTopic,
		},
	)

	app.consumerDone = make(chan struct{})
	go func() {
		defer close(app.consumerDone)
		if err := app.consumer.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			errorChannel <- fmt.Errorf("kafka consumer: %w", err)
		}
	}()

	// Step 7: only after grid registry bootstrap has succeeded and the
	// Kafka consumer is running is ingestion allowed to be reported ready.
	app.health.SetReady(true)
	app.logger.Info("ingestion readiness enabled")

	var runErr error

	select {
	case <-ctx.Done():
		app.logger.Info("shutdown signal received")
	case err := <-errorChannel:
		runErr = err
		app.logger.Error("application component failed", "error", err)

		cancel()
	}

	shutdownErr := app.shutdown()

	return errors.Join(runErr, shutdownErr)
}
