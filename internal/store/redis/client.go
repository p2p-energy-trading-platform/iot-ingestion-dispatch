package redis

import (
	"context"
	_ "embed"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed scripts/set_latest_if_newer.lua
var setLatestIfNewerSource string

//go:embed scripts/update_heartbeat.lua
var updateHeartbeatSource string

// Config is the connection configuration needed to build this package's
// own *goredis.Client. Production controls (TLS, timeouts, pool sizing,
// retries, eviction policy) are deliberately out of scope here — see
// 02-redis-hot-storage.md "Production controls".
type Config struct {
	Address  string
	Password string
	DB       int
}

// Store wraps a go-redis client plus the Lua scripts this package's
// adapters depend on. Scripts are embedded at compile time.
type Store struct {
	rdb *goredis.Client

	setLatestIfNewer *goredis.Script
	updateHeartbeat  *goredis.Script
}

// New connects to Redis using cfg and pings immediately, so a bad
// address/credential fails fast at startup rather than on the first
// ingestion write.
func New(context context.Context, cfg Config) (*Store, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(context).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	return &Store{
		rdb:              rdb,
		setLatestIfNewer: goredis.NewScript(setLatestIfNewerSource),
		updateHeartbeat:  goredis.NewScript(updateHeartbeatSource),
	}, nil
}

// Close releases the underlying connection pool.
func (store *Store) Close() error {
	return store.rdb.Close()
}
