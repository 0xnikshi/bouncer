package redisstore

// The Lua scripts below run atomically on the Redis server. Each one reads the
// bucket's state, advances it for elapsed time, makes the allow/deny decision,
// writes the new state, and sets a TTL so idle keys self-expire — all as a
// single indivisible operation. That atomicity is what prevents concurrent
// callers across the fleet from over-admitting.
//
// Conventions shared by both scripts:
//   KEYS[1] = the (prefixed) bucket key
//   ARGV[1] = rate      (events per second)
//   ARGV[2] = capacity  (burst)
//   ARGV[3] = requested (n)
// Time is taken from redis.call('TIME') so every instance agrees regardless of
// local clock skew. Returns 1 if allowed, 0 if denied.

// tokenBucketScript implements the token bucket: tokens accrue at `rate` up to
// `capacity`; a request is allowed when enough tokens are present and consumes
// them. A fresh key starts full, so an initial burst up to capacity is allowed.
const tokenBucketScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local capacity  = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts     = now
end

-- Refill for elapsed time, capped at capacity.
local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
  ts     = now
end

local allowed = 0
if tokens >= requested then
  tokens  = tokens - requested
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', ts)
-- Expire idle keys once they would have fully refilled (+1s slack).
local ttl = math.ceil(capacity / rate) + 1
redis.call('EXPIRE', key, ttl)

return allowed
`

// leakyBucketScript implements the leaky bucket (as a meter): the level drains
// at `rate`; a request is allowed when it fits below `capacity` and raises the
// level. A fresh key starts empty, so an initial burst up to capacity is
// allowed, after which admissions are paced by the drain rate.
const leakyBucketScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local capacity  = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state = redis.call('HMGET', key, 'level', 'ts')
local level = tonumber(state[1])
local ts    = tonumber(state[2])
if level == nil then
  level = 0
  ts    = now
end

-- Drain for elapsed time, clamped at empty.
local elapsed = now - ts
if elapsed > 0 then
  level = math.max(0, level - elapsed * rate)
  ts    = now
end

local allowed = 0
if level + requested <= capacity then
  level   = level + requested
  allowed = 1
end

redis.call('HSET', key, 'level', level, 'ts', ts)
-- Expire idle keys once they would have fully drained (+1s slack).
local ttl = math.ceil(capacity / rate) + 1
redis.call('EXPIRE', key, ttl)

return allowed
`
