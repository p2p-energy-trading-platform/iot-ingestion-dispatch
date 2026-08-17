package app

import (
	"context"
	"errors"
	"fmt"
)

func (app *App) Run(ctx context.Context) error {

	// NOTE: runContext is blanked here, will be later
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	errorChannel := make(chan error, 2)

	app.logger.Info("starting health server",
		"address", app.config.Health.Address,
	)

	go func() {
		if err := app.health.Run(); err != nil {
			errorChannel <- fmt.Errorf(
				"health error: %w",
				err,
			)
		}
	}()

	app.logger.Info("starting gRPC server", "address", app.config.GRPC.Address)

	go func() {
		if err := app.grpc.Run(); err != nil {
			errorChannel <- fmt.Errorf(
				"grpc server: %w",
				err,
			)
		}
	}()

	// app.logger.Info("starting Kafka consumer",
	// 	"group", app.config.Kafka.ConsumerGroup,
	// 	"topics", []string{
	// 		app.config.Kafka.MeterTopic,
	// 		app.config.Kafka.HeartbeatTopic,
	// 	},
	// )

	// TODO: Start kafka consumer here
	// go func() {

	// }

	var runErr error

	select {
	case <-ctx.Done():
		app.logger.Info("shutdown signal recieved")
	case err := <-errorChannel:
		runErr = err
		app.logger.Error(
			"application component failed",
			"error", err,
		)

		cancel()
	}

	shutdownErr := app.shutdown()

	return errors.Join(
		runErr,
		shutdownErr,
	)
}
