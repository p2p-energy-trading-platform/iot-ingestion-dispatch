-- update_heartbeat.lua
--
-- KEYS[1]: house:{house_id}:status   (HASH)
-- KEYS[2]: grid:{grid_id}:houses     (SET)
-- ARGV[1]: TTL in seconds for KEYS[1] (the documented 10-minute heartbeat TTL)
-- ARGV[2]: status value, e.g. "online"
-- ARGV[3]: last_heartbeat_at value (caller-chosen string format, stored as-is)
-- ARGV[4]: house_id — the member added to KEYS[2]
--
-- Per 02-redis-hot-storage.md "Atomic heartbeat update": HSET, EXPIRE, and
-- SADD run together in one script so a crash between steps can never
-- leave a status hash without its required TTL, and readers never
-- observe a partially-applied heartbeat. SADD is naturally idempotent, so
-- it's safe to call on every heartbeat whether the house is new or
-- already known.
--
-- Kept to these three constant-time commands deliberately — a running
-- Lua script blocks other Redis work until it completes.

local status_key = KEYS[1]
local grid_houses_key = KEYS[2]
local ttl_seconds = tonumber(ARGV[1])
local status_value = ARGV[2]
local last_heartbeat_at = ARGV[3]
local house_id = ARGV[4]

redis.call('HSET', status_key, 'status', status_value, 'last_heartbeat_at', last_heartbeat_at)
redis.call('EXPIRE', status_key, ttl_seconds)
redis.call('SADD', grid_houses_key, house_id)

return 1
