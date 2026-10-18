# Must satisfy the "go" directive in go.mod (currently 1.27).
ARG GO_VERSION=1.27.2

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -ldflags="-s -w \
      -X main.version=${VERSION} \
      -X main.commit=${COMMIT}" \
    -o /out/iot-ingestion \
    ./cmd/iot-ingestion

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -o /out/migrate \
    ./cmd/migrate

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -o /out/redis-rebuild \
    ./cmd/redis-rebuild

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -o /out/failure-replay \
    ./cmd/failure-replay

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -o /out/register-houses \
    ./cmd/register-houses

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=builder /out/iot-ingestion /iot-ingestion
COPY --from=builder /out/migrate /migrate
COPY --from=builder /out/redis-rebuild /redis-rebuild
COPY --from=builder /out/failure-replay /failure-replay
COPY --from=builder /out/register-houses /register-houses

EXPOSE 50051
EXPOSE 8080
EXPOSE 9090

USER nonroot:nonroot

ENTRYPOINT ["/iot-ingestion"]