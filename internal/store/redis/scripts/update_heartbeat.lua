local status_key = KEYS[1]
local grid_houses_key = KEYS[2]
local ttl_seconds = tonumber(ARGV[1])
local status_value = ARGV[2]
local last_heartbeat_at = ARGV[3]
local house_id = ARGV[4]

-- 1 when the house had no status, that is it was offline and is now online.
local came_online = 1 - redis.call('EXISTS', status_key)

redis.call('HSET', status_key, 'status', status_value, 'last_heartbeat_at', last_heartbeat_at)
redis.call('EXPIRE', status_key, ttl_seconds)
redis.call('SADD', grid_houses_key, house_id)

return came_online
