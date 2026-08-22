-- set_latest_if_newer.lua
--
-- KEYS[1]: meter:{grid_id}:{house_id}:latest  (STRING)
-- ARGV[1]: incoming reading's event_time, unix milliseconds (integer)
-- ARGV[2]: incoming reading's seq (integer)
-- ARGV[3]: incoming reading's full JSON projection to store verbatim if it
--          wins. Must itself contain "event_time_unix_ms" and "seq" fields
--          matching ARGV[1]/ARGV[2], so a reader never needs a second
--          round trip to learn them.
--
-- Per 02-redis-hot-storage.md "Out-of-order protection": at-least-once
-- delivery and retries mean an older reading can arrive after a newer
-- one. This script compares the incoming (event_time, seq) against
-- whatever is currently stored and writes only if the incoming reading is
-- strictly newer. Duplicate/older input is a no-op — this is what keeps a
-- replayed Kafka record from regressing the cached "latest" state that
-- the gRPC GetLatestReading call reads from.
--
-- Returns 1 if the write was applied, 0 if skipped as stale/duplicate.

local key = KEYS[1]
local new_time = tonumber(ARGV[1])
local new_seq = tonumber(ARGV[2])
local new_value = ARGV[3]

local existing = redis.call('GET', key)

if existing then
    local ok, decoded = pcall(cjson.decode, existing)
    if ok and decoded.event_time_unix_ms ~= nil and decoded.seq ~= nil then
        local existing_time = tonumber(decoded.event_time_unix_ms)
        local existing_seq = tonumber(decoded.seq)

        if new_time < existing_time then
            return 0
        end
        if new_time == existing_time and new_seq <= existing_seq then
            return 0
        end
    end
    -- If the existing value fails to decode or is missing these fields,
    -- treat it as not authoritative and fall through to overwrite rather
    -- than getting stuck unable to ever advance past a corrupt value.
end

redis.call('SET', key, new_value)
return 1
