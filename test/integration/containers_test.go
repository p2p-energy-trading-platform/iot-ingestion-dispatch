//go:build integration

// Package integration runs the service against real, pinned dependencies
// started with testcontainers-go. Run with:
//
//	go test -tags integration ./test/integration/...
//
// Tests skip (not fail) when Docker is unavailable.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/migrations"
)

// Images are pinned; override only to test a candidate upgrade.
var (
	timescaleImage = envOr("TEST_TIMESCALE_IMAGE", "timescale/timescaledb:2.30.2-pg16")
	redisImage     = envOr("TEST_REDIS_IMAGE", "redis:7-alpine")
	kafkaImage     = envOr("TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.7.0")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func start(t *testing.T, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start %s: %v", req.Image, err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	return c
}

// newTimescale starts TimescaleDB with the empty iot_data schema that
// gridx-infra's bootstrap script provides in a real deployment.
func newTimescale(t *testing.T) string {
	t.Helper()
	c := start(t, testcontainers.ContainerRequest{
		Image:        timescaleImage,
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_PASSWORD": "pw", "POSTGRES_DB": "gridx"},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort("5432/tcp"),
		),
	})
	ctx := context.Background()
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("postgres://postgres:pw@%s:%s/gridx?sslmode=disable", host, port.Port())

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS iot_data"); err != nil {
		t.Fatal(err)
	}
	return dsn
}

func newRedis(t *testing.T) string {
	t.Helper()
	c := start(t, testcontainers.ContainerRequest{
		Image:        redisImage,
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute),
	})
	ctx := context.Background()
	host, _ := c.Host(ctx)
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort(host, port.Port())
}

// newKafka starts a single-node KRaft broker. The advertised listener must be
// reachable from the host, so the container publishes the same port number on
// the host that it advertises.
func newKafka(t *testing.T) string {
	t.Helper()
	port := freePort(t)
	p := fmt.Sprint(port)
	c := start(t, testcontainers.ContainerRequest{
		Image:        kafkaImage,
		ExposedPorts: []string{p + "/tcp"},
		Env: map[string]string{
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:" + p + ",CONTROLLER://0.0.0.0:9094",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:" + p,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{nat.Port(p + "/tcp"): {{HostIP: "0.0.0.0", HostPort: p}}}
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
	})
	_ = c
	return "localhost:" + p
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// gooseProvider opens the embedded migrations the way cmd/migrate does.
func gooseProvider(t *testing.T, dsn string) *goose.Provider {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
