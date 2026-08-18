ARG GO_VERSION=1.26.6

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

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=builder /out/iot-ingestion /iot-ingestion
COPY --from=builder /out/migrate /migrate

EXPOSE 50051
EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/iot-ingestion"]