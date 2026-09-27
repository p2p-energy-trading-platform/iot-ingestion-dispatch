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
end

redis.call('SET', key, new_value)
return 1
