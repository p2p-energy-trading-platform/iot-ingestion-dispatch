package redis

import (
	_ "embed"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed scripts/set_latest_if_newer.lua
var setLatestIfNewerSource string

//go:embed scripts/update_heartbeat.lua
var updateHeartbeatSource string

// Client wraps a go-redis client plus the Lua scripts this package's
// adapters depend on. Scripts are embedded at compile time, so there's no
// runtime file dependency in the deployed binary/image.
//
// goredis.Script.Run already does EVALSHA-with-fallback-and-cache — it
// tries EVALSHA first, and only sends the full script source (via EVAL,
// which also caches it server-side) on a cache miss. This satisfies
// 02-redis-hot-storage.md's "load the script once and invoke it by SHA
// rather than sending its source on every heartbeat" without any extra
// code here.
//
// Production controls this package deliberately does NOT own — TLS,
// auth, connection/command timeouts, pool sizing, retries, and the
// Redis-side maxmemory/eviction policy — belong to whatever constructs
// the *goredis.Client passed into NewClient (internal/config +
// internal/app), not to this adapter layer.
type Client struct {
	rdb *goredis.Client

	setLatestIfNewer *goredis.Script
	updateHeartbeat  *goredis.Script
}

// NewClient wraps an already-configured *goredis.Client. Configuration
// (address, TLS, timeouts, pool size, etc.) is the caller's
// responsibility.
func NewClient(rdb *goredis.Client) *Client {
	return &Client{
		rdb:              rdb,
		setLatestIfNewer: goredis.NewScript(setLatestIfNewerSource),
		updateHeartbeat:  goredis.NewScript(updateHeartbeatSource),
	}
}
