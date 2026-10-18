package redis

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed scripts/set_latest_if_newer.lua
var setLatestIfNewerSource string

//go:embed scripts/update_heartbeat.lua
var updateHeartbeatSource string

// Config is the connection configuration needed to build this package's
// own *goredis.Client (02-redis-hot-storage.md "Production controls"). Zero
// values keep go-redis defaults, except retries which are always explicit.
//
// Memory limit and eviction policy are server-side settings; Store.Verify
// checks the eviction policy at startup.
type Config struct {
	Address  string
	Username string
	Password string
	DB       int

	TLS                bool
	TLSInsecureSkipVer bool

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
}

func (cfg Config) options() *goredis.Options {
	opts := &goredis.Options{
		Addr:         cfg.Address,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		MaxRetries:   cfg.MaxRetries,
	}
	if cfg.MaxRetries == 0 {
		// go-redis treats 0 as "default (3)"; -1 disables retries.
		opts.MaxRetries = -1
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.TLSInsecureSkipVer, //nolint:gosec // development-only, rejected in production by config validation
		}
	}
	return opts
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
	rdb := goredis.NewClient(cfg.options())

	if err := rdb.Ping(context).Err(); err != nil {
		_ = rdb.Close()
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

// Ping checks that Redis answers; used for dependency health reporting.
func (store *Store) Ping(ctx context.Context) error {
	return store.rdb.Ping(ctx).Err()
}

// EvictionPolicy returns the server's maxmemory-policy.
func (store *Store) EvictionPolicy(ctx context.Context) (string, error) {
	res, err := store.rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return "", fmt.Errorf("redis: config get maxmemory-policy: %w", err)
	}
	return res["maxmemory-policy"], nil
}

// LivenessAndLatestSafe reports whether the eviction policy is one that never
// silently drops liveness or latest-state keys. "allkeys-*" policies evict
// any key, which would turn memory pressure into false "offline" houses and
// missing latest readings (02-redis-hot-storage.md).
func LivenessAndLatestSafe(policy string) bool {
	switch policy {
	case "noeviction", "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl":
		// volatile-* only evicts keys with a TTL: the status keys. noeviction
		// is the safest. Latest-state keys carry no TTL and are never evicted
		// by volatile-* policies.
		return true
	}
	return false
}

// FlushForTest empties the selected database. It exists so integration tests
// can simulate Redis loss; production code never calls it.
func (store *Store) FlushForTest(ctx context.Context) error {
	return store.rdb.FlushDB(ctx).Err()
}
