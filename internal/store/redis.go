package store

import (
	"github.com/redis/go-redis/v9"
)

// NewRedis returns a go-redis client.
func NewRedis(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// MultiBucketScript checks and consumes tokens from several buckets atomically.
//
// KEYS:   one bucket per dimension (upstream, api key, client ip, ...)
// ARGV:   [1]=now ms  [2]=requested  then (capacity, refill-per-sec) per key
// Return: {allowed 0|1, index-of-first-exhausted-bucket (0 when allowed)}
//
// Why one script instead of calling TokenBucketScript per dimension: rate
// limiting used to be per-API-key only, which meant N active keys could each
// draw the route's upstream_rps and together push N x that at the provider -
// the number in the config protected nothing. Adding a global upstream bucket
// fixes it, but only if both buckets are taken in one atomic step; otherwise
// two concurrent requests can each pass their own check and overshoot together.
//
// On rejection the refilled amounts are still persisted, so a throttled bucket
// keeps filling instead of freezing at the moment it first ran dry.
var MultiBucketScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local req = tonumber(ARGV[2])
local n = #KEYS
local toks = {}
local fail = 0
for i = 1, n do
  local cap = tonumber(ARGV[2 + i * 2 - 1])
  local rate = tonumber(ARGV[2 + i * 2])
  local data = redis.call('HMGET', KEYS[i], 'tokens', 'ts')
  local t = tonumber(data[1])
  local ts = tonumber(data[2])
  if t == nil then
    t = cap
    ts = now
  end
  local delta = math.min(cap - t, (now - ts) / 1000 * rate)
  toks[i] = t + delta
  if toks[i] < req and fail == 0 then
    fail = i
  end
end
for i = 1, n do
  local v = toks[i]
  if fail == 0 then
    v = toks[i] - req
  end
  redis.call('HMSET', KEYS[i], 'tokens', v, 'ts', now)
  redis.call('EXPIRE', KEYS[i], 3600)
end
if fail ~= 0 then
  return {0, fail}
end
return {1, 0}
`)
