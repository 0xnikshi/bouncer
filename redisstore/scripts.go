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

// fixedWindowScript implements the fixed window counter: up to `capacity` events
// are allowed per window of length capacity/rate seconds, after which the count
// resets. State is a single hash at KEYS[1] with fields {count, window} where
// window is the current window index — kept on one key so it is cluster-safe.
const fixedWindowScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local capacity  = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local window    = capacity / rate
local window_id = math.floor(now / window)

local state = redis.call('HMGET', key, 'count', 'window')
local count = tonumber(state[1])
local wid   = tonumber(state[2])
if wid == nil or wid ~= window_id then
  count = 0
end

local allowed = 0
if count + requested <= capacity then
  count   = count + requested
  allowed = 1
  redis.call('HSET', key, 'count', count, 'window', window_id)
  -- Expire after the window ends (+1s slack) so idle keys self-clean.
  redis.call('EXPIRE', key, math.ceil(window) + 1)
end

return allowed
`

// slidingWindowScript implements the exact sliding window (log). Admitted events
// are stored in a sorted set scored by timestamp; entries older than the trailing
// window are dropped, and a request is allowed if the remaining count plus the
// request fits within `capacity`. ARGV[4] is a per-call unique member prefix so
// each event is a distinct set member (Redis sets dedupe by member).
const slidingWindowScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local capacity  = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local member    = ARGV[4]

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])   -- microseconds
local window_us = (capacity / rate) * 1000000

-- Drop events that fell out of the trailing window.
redis.call('ZREMRANGEBYSCORE', key, 0, now - window_us)

local count = redis.call('ZCARD', key)

local allowed = 0
if count + requested <= capacity then
  for i = 1, requested do
    redis.call('ZADD', key, now, member .. '-' .. i)
  end
  allowed = 1
end

-- Expire once the newest event would age out (+1s slack).
redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)

return allowed
`

// slidingWindowCounterScript implements the approximate sliding window counter.
// It keeps the current and previous window counts in a hash and estimates the
// trailing count as prev*(1-frac) + cur, where frac is how far the current
// window has advanced. Uses O(1) state and smooths the fixed-window boundary.
const slidingWindowCounterScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local capacity  = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local window    = capacity / rate
local pos       = now / window
local window_id = math.floor(pos)
local frac      = pos - window_id

local state = redis.call('HMGET', key, 'cur', 'prev', 'window')
local cur  = tonumber(state[1])
local prev = tonumber(state[2])
local wid  = tonumber(state[3])

if wid == nil then
  cur, prev = 0, 0
elseif window_id == wid + 1 then
  prev, cur = cur, 0
elseif window_id > wid then
  prev, cur = 0, 0
end
-- else: same window, keep cur and prev

local estimated = prev * (1 - frac) + cur

local allowed = 0
if estimated + requested <= capacity then
  cur     = cur + requested
  allowed = 1
end

-- Persist unconditionally so a window rollover is saved even on denial.
redis.call('HSET', key, 'cur', cur, 'prev', prev, 'window', window_id)
-- Keep for two windows (prev is still referenced), +1s slack.
redis.call('EXPIRE', key, math.ceil(window) * 2 + 1)

return allowed
`
