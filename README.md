# GridX IoT Ingestion & Dispatch

The **IoT Ingestion & Dispatch** service is a Go service in the GridX platform responsible for consuming IoT data from Kafka, validating and processing incoming records, persisting durable telemetry and registry information to TimescaleDB, maintaining latest-state projections in Redis, and exposing internal query functionality through gRPC.

---

# Repository Structure

```text
iot-ingestion-dispatch/
├── cmd/
│   ├── iot-ingestion/        # the service: parse config, build app, handle signals
│   ├── migrate/              # single-owner migration command/job
│   ├── redis-rebuild/        # rebuild the Redis projection from TimescaleDB
│   ├── failure-replay/       # operator tool: list / replay / discard failed records
│   └── register-houses/      # provision houses (until the METER_VERIFIED consumer exists)
├── internal/
│   ├── app/                  # wiring, lifecycle, shutdown, dependency monitor
│   ├── config/               # typed env config; malformed values fail startup
│   ├── domain/               # internal domain types and errors
│   ├── ingestion/            # consumer, partition workers, router, decoder, handlers, retry
│   ├── admission/            # in-memory grid registry snapshot + refresher
│   ├── rebuild/              # bounded Redis rebuild logic
│   ├── store/
│   │   ├── postgres/         # telemetry, heartbeat, grids, failures, rebuild reads
│   │   └── redis/            # latest/heartbeat adapters + Lua scripts
│   ├── transport/
│   │   ├── grpc/             # server shell, health, interceptors (query API: see below)
│   │   └── httphealth/       # /healthz, /readyz and the separate /metrics listener
│   └── observability/        # slog setup, Prometheus metrics, OpenTelemetry tracing
├── migrations/               # goose SQL migrations (embedded)
├── test/integration/         # testcontainers: real TimescaleDB, Redis and Kafka
├── docs/runbooks/            # consumer-lag, dependency-outage, failure-replay, redis-rebuild
├── .github/workflows/ci.yml
├── .env.example
├── Dockerfile                # pinned multi-stage, non-root runtime
└── docker-compose.yml        # local development only
```

**Not in this repository yet:** the gRPC *query* API (`internal/query`, the generated-protocol handlers and `query_repository.go`) described in `docs/plans/iot-ingestion-dispatch/08-grpc.md` is owned separately and is not implemented here. Today the gRPC listener serves only the standard health service. Contract and load tests that depend on it (tiers 3-5 of the testing strategy) are pending that work. Dispatch and cold storage are deliberately out of scope.

---

# Clone the Project

When working inside the GridX workspace:

```bash
cd gridx-workspace
```

The repository should exist at:

```text
gridx-workspace/iot-ingestion-dispatch
```

---

# Install Go Dependencies

From the `iot-ingestion-dispatch` directory:

```bash
go mod download
```

Then verify and normalize the module dependencies:

```bash
go mod tidy
```

---

# Local Infrastructure

The IoT service depends on infrastructure provided by `gridx-infra`.

From the parent workspace:

```bash
cd gridx-workspace
```

Start the GridX environment:

```bash
go-task up
```

Or

```bash
task up
```

---

# Configuration and Operations

All settings are environment variables (a local `.env` is loaded for development; see `.env.example`). **A malformed value, an unknown enum, or a contradictory combination fails startup** instead of silently using a default. With `SERVICE_ENV=production` the service additionally refuses plaintext Kafka/Redis/PostgreSQL, `REDIS_TLS_INSECURE_SKIP_VERIFY`, an implicit `REDIS_ACK_POLICY`, and a Redis eviction policy that could drop liveness or latest-state keys.

| Area | Settings |
|---|---|
| Kafka | `KAFKA_BROKER` (comma-separated), `KAFKA_CONSUMER_GROUP`, `KAFKA_METER_TOPIC`, `KAFKA_HEARTBEAT_TOPIC`, `KAFKA_TLS`, `KAFKA_SASL_MECHANISM/USER/PASSWORD`, `KAFKA_SESSION_TIMEOUT`, `KAFKA_HEARTBEAT_INTERVAL`, `KAFKA_FETCH_MAX_BYTES`, `KAFKA_MAX_PARTITION_FETCH_BYTES` |
| Input limits | `INGESTION_MAX_RECORD_BYTES` (default 64 KiB), `INGESTION_ENFORCE_HOUSE_KEY`, `INGESTION_MAX_CONCURRENT_HANDLERS` |
| Retry | `INGESTION_RETRY_MAX_TRIES`, `INGESTION_RETRY_INITIAL_INTERVAL`, `INGESTION_RETRY_MAX_INTERVAL` |
| PostgreSQL | `POSTGRES_URL`, `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS`, `POSTGRES_CONNECT_TIMEOUT`, `POSTGRES_STATEMENT_TIMEOUT`, `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_EXPECTED_MIGRATION` |
| Redis | `REDIS_ADDRESS`, `REDIS_USERNAME/PASSWORD/DB`, `REDIS_TLS`, `REDIS_DIAL/READ/WRITE_TIMEOUT`, `REDIS_POOL_SIZE`, `REDIS_MIN_IDLE_CONNS`, `REDIS_MAX_RETRIES`, `REDIS_ACK_POLICY` |
| Registry / lifecycle | `ADMISSION_REFRESH_INTERVAL` (default 120s), `SHUTDOWN_TIMEOUT` |
| Observability | `LOG_LEVEL`, `METRICS_ADDRESS` (Prometheus `/metrics`, default `:9090`), `OTEL_EXPORTER_OTLP_ENDPOINT` (tracing is off when empty) |

Secrets are read at process start; rotating one needs a rolling restart (see `docs/runbooks/dependency-outage.md`).

## Redis acknowledgement policy

This resolves the plan's first release-blocking decision as a **per-deployment setting**, defaulting to the safe choice:

- `required` (default): a record whose TimescaleDB write succeeded but whose Redis update failed stays uncommitted and is retried. Redis is always current when a record is acknowledged.
- `async-repair`: the record is acknowledged after the durable write; `iot_ingestion_cache_degraded_total` counts the gap, and `redis-rebuild` (or the next reading for that house) repairs the projection. Production must set the policy explicitly.

The owners still need to pick the production value; both are implemented and tested.

## Operator commands

```bash
go run ./cmd/redis-rebuild -dry-run          # rebuild Redis from TimescaleDB (docs/runbooks/redis-rebuild.md)
go run ./cmd/failure-replay list             # unresolved ingestion failures
go run ./cmd/failure-replay replay -actor you -id 12
go run ./cmd/failure-replay discard -actor you -id 13
```

The container image ships `/migrate`, `/redis-rebuild`, `/failure-replay` and `/register-houses` next to the service binary.

## Houses must be registered first

A heartbeat never creates a house: the house (with its `grid_id` and `meter_id`) must already exist in `iot_data.houses`. An unregistered house's heartbeat is recorded in `ingestion_failures` with stage `grid_validation` and can be replayed after the house is registered. Unknown assets reported by a heartbeat go to `iot_data.discovered_assets`; only the registration flow writes `flexible_assets`.

The planned registration flow (a `METER_VERIFIED` event consumed from Kafka) is not specified far enough to build yet. Until it is, houses are provisioned with `register-houses`, like grids are provisioned by migration:

```bash
go run ./cmd/register-houses -file houses.json -dry-run     # validate only
go run ./cmd/register-houses -file houses.json
```

The file is a JSON array, or one JSON object per line, with `house_id`, `grid_id`, `meter_id`, `device_class` and optional `rated_solar_kw`. Simulator heartbeats have exactly these fields, so for local development a capture works as input:

```bash
mosquitto_sub -h localhost -t 'gridx/+/+/heartbeat' -C 120 > heartbeats.jsonl
go run ./cmd/register-houses -file heartbeats.jsonl
go run ./cmd/failure-replay list            # then replay any quarantined heartbeats
```

It is idempotent and never changes an existing house: a different grid, meter or device class for a registered house, or a meter already bound to another house, is refused.

## Metrics

Prometheus metrics use a separate listener so `/metrics` is never exposed through the health port. Labels are limited to topic, result class and failure stage; never house, grid, meter, asset or Kafka-key values. Signals include records processed/failed/retried, processing and commit latency, rebalances, degraded-cache events, grid-registry size/refresh/last-success, unresolved failure count and age, dependency up/down, and PostgreSQL pool usage, alongside Go runtime and process metrics.

---

# Database Migrations

Database migrations are managed using **Goose**.

---

## Migration Files

Migration SQL files are stored in:

```text
migrations/
```

Example:

```text
migrations/
├── embed.go
├── 00001_create_migration_smoke_test.sql
├── 00002_example.sql
└── ...
```

The files are embedded into the migration executable using Go's `embed` package.

This means the migration executable and its SQL files are built together.

---

## Apply Migrations Locally

Ensure TimescaleDB is running first.

Then configure `POSTGRES_URL` and `DOCKER_POSTGRES_URL`

The `POSTGRES_URL` is for local development without docker and `DOCKER_POSTGRES_URL` is needed for docker deployment.

Apply all pending migrations:

```bash
go run ./cmd/migrate up
```

---

## Check Migration Status

```bash
go run ./cmd/migrate status
```

---

## Check Current Migration Version

```bash
go run ./cmd/migrate version
```

---

## Roll Back the Latest Migration

```bash
go run ./cmd/migrate down
```

Then reapply it with:

```bash
go run ./cmd/migrate up
```

---

# Run the Application Locally

There are two main ways to run the service locally.

---

## Option 1: Run Infrastructure in Docker and Go App on Host

This is normally the easiest setup while developing Go code.

### 1. Start GridX infrastructure

From `gridx-workspace`:

```bash
go-task infra-up
```

Or

```bash
task infra-up
```

### 2. Move to the service repository

```bash
cd iot-ingestion-dispatch
```

### 3. Configure local environment variables

For host execution, use:

```text
Kafka       → localhost:9092
Redis       → localhost:6379
TimescaleDB → localhost:5433
```

### 4. Apply migrations

```bash
go run ./cmd/migrate up
```

### 5. Start the service

```bash
go run ./cmd/iot-ingestion
```

---

## Option 2: Run as full grid-workspace container

**NOTE**: Refer `gridx-workspace` docs.

Start the full service containers (or you can start them after building the go containers):

```bash
go-task up
```

Or

```bash
task up
```

Build and start the service image from `grid-workspace`:

```bash
go-task build -- iot-ingestion-dispatch
```

Or

```bash
task build -- iot-ingestion-dispatch
```

In the normal GridX development environment, prefer using the parent workspace Compose configuration so the service is connected to the same Docker network as Kafka, Redis, and TimescaleDB.

---

# Running Tests

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test -v ./...
```

Run tests with the Go race detector:

```bash
go test -race ./...
```

Store tests that need real PostgreSQL/Redis are skipped unless pointed at them (migrations applied first):

```bash
TEST_DATABASE_URL=postgres://... TEST_REDIS_ADDRESS=localhost:6379 go test ./internal/store/...
```

Integration tests start pinned TimescaleDB, Redis and Kafka containers with testcontainers-go (Docker required). They cover clean-install and previous-version migration upgrades, retention and compression policies, the heartbeat and meter workflows, replay safety, Redis rebuild, failure replay, and a real Kafka consumer including a restart in the same group:

```bash
go test -tags integration ./test/integration/...
```

The decoders are fuzzed:

```bash
go test -run '^$' -fuzz FuzzDecodeMeterReading -fuzztime 30s ./internal/ingestion
```

---

## Run a Specific Package

For example:

```bash
go test -v ./internal/config
```

---

## Run a Specific Test

For example:

```bash
go test -v ./internal/config -run TestConfigValidate
```

---

# Formatting and Static Checks

Format the project:

```bash
gofmt -w .
```

Check compilation and common issues:

```bash
go vet ./...
```

Run tests:

```bash
go test ./...
```

Run with race detection:

```bash
go test -race ./...
```

If installed, run Staticcheck:

```bash
staticcheck ./...
```

If installed, run Go vulnerability scanning:

```bash
govulncheck ./...
```

A useful local verification sequence is:

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
```

---

# Build the Go Binaries

Build the main application:

```bash
go build -o bin/iot-ingestion ./cmd/iot-ingestion
```

Build the migration executable:

```bash
go build -o bin/migrate ./cmd/migrate
```

The resulting layout is:

```text
bin/
├── iot-ingestion
└── migrate
```

Run the application:

```bash
./bin/iot-ingestion
```

Run migrations:

```bash
./bin/migrate up
```