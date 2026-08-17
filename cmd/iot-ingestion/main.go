package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/app"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/observability"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	config, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger := observability.SetupLogger(
		config.Service.Name,
		config.Service.Environment,
		config.Service.LogLevel,
	)

	logger.Info(
		"starting service",
		"service", config.Service.Name,
		"environment", config.Service.Environment,
	)

	application, err := app.New(ctx, config, logger)
	if err != nil {
		logger.Error("Failed to initialize application", "error", err)
		os.Exit(1)
	}

	if err := application.Run(ctx); err != nil {
		logger.Error(
			"application stopped with error",
			"error", err,
		)
		os.Exit(1)
	}

	logger.Info("service stopped")
}
