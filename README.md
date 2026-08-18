# GridX IoT Ingestion & Dispatch

The **IoT Ingestion & Dispatch** service is a Go service in the GridX platform responsible for consuming IoT data from Kafka, validating and processing incoming records, persisting durable telemetry and registry information to TimescaleDB, maintaining latest-state projections in Redis, and exposing internal query functionality through gRPC.

---

# Repository Structure

```text
iot-ingestion/
├── cmd/
│   ├── iot-ingestion/
│   │   └── main.go                   # parse config, build app, handle signals
│   └── migrate/
│       └── main.go                   # single-owner migration command/job
├── internal/
│   ├── app/
│   │   ├── app.go                    # dependency wiring and lifecycle
│   │   └── shutdown.go               # readiness off, drain, bounded stop
│   ├── config/
│   │   ├── config.go                 # typed runtime configuration
│   │   └── validate.go               # fail-fast cross-field validation
│   ├── domain/
│   │   ├── telemetry.go              # internal domain types and invariants
│   │   ├── heartbeat.go
│   │   └── errors.go                 # typed domain/application errors
│   ├── ingestion/
│   │   ├── consumer.go               # poll loop, partition workers, commits
│   │   ├── router.go                 # strict topic-to-handler routing
│   │   ├── decoder.go                # private JSON wire structs to domain
│   │   ├── meter_handler.go          # ordered meter workflow
│   │   ├── heartbeat_handler.go      # ordered heartbeat workflow
│   │   ├── retry.go                  # transient/permanent policy
│   │   └── failure_recorder.go       # failure application port
│   ├── admission/
│   │   ├── registry.go               # immutable in-memory snapshot
│   │   └── refresher.go              # bootstrap and atomic refresh
│   ├── query/
│   │   ├── service.go                # storage-neutral query use cases
│   │   └── pagination.go             # opaque keyset-token logic
│   ├── store/
│   │   ├── postgres/
│   │   │   ├── pool.go
│   │   │   ├── telemetry_writer.go
│   │   │   ├── heartbeat_writer.go
│   │   │   ├── query_repository.go
│   │   │   ├── grid_loader.go
│   │   │   └── failures.go
│   │   └── redis/
│   │       ├── client.go
│   │       ├── latest.go
│   │       ├── heartbeat.go
│   │       ├── keys.go
│   │       └── scripts/
│   │           ├── set_latest_if_newer.lua
│   │           └── update_heartbeat.lua
│   ├── transport/
│   │   ├── grpc/
│   │   │   ├── server.go             # options, registration, lifecycle
│   │   │   ├── telemetry_handler.go  # generated API to query service
│   │   │   ├── mapper.go
│   │   │   ├── errors.go             # typed errors to gRPC status
│   │   │   └── interceptors.go
│   │   └── httphealth/
│   │       └── server.go             # /healthz, /readyz only
│   └── observability/
│       ├── logging.go
│       ├── metrics.go
│       └── tracing.go
├── migrations/
│   ├── embed.go                      # embed.FS exposed to migrate command
│   └── 00001_initial_schema.sql
├── test/
│   ├── integration/                  # real Kafka/Redis/TimescaleDB
│   ├── contract/                     # generated-client interoperability
│   ├── endtoend/                     # simulator -> query API smoke path
│   ├── load/                         # reproducible ingest/query scenarios
│   ├── fixtures/
│   └── helpers/
├── docs/
│   └── runbooks/
│       ├── consumer-lag.md
│       ├── dependency-outage.md
│       ├── failure-replay.md
│       └── redis-rebuild.md
├── .github/workflows/ci.yml
├── .dockerignore
├── .env.example                      # names and safe examples, no secrets
├── .gitignore                        # must exclude .env and local credentials
├── Dockerfile                        # pinned multi-stage, non-root runtime
├── docker-compose.yaml               # local development only
├── go.mod
├── go.sum
└── README.md
```

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