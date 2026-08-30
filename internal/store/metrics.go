package store

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// StatsWindow is how many seconds of per-second history we keep for charts.
const StatsWindow = 60

// SecPoint is a single (timestamp, count) sample used for time-series charts.
type SecPoint struct {
	Ts    int64 `json:"ts"`
	Count int64 `json:"count"`
}

// RecordRequest writes all per-request counters in a single Redis pipeline so
// the hot path only pays for one network round-trip instead of several INCRs.
//   - cumulative counters: stats:total / stats:cached / stats:rejected / stats:errors
//   - per-second series:    stats:sec:{sec} / stats:cached:sec:{sec} / ...
//
// No-ops gracefully when rdb is nil (e.g. metrics disabled) so callers never panic.
func RecordRequest(ctx context.Context, rdb *redis.Client, cached, rejected, errored bool) {
	if rdb == nil {
		return
	}
	now := time.Now().Unix()
	secKey := "stats:sec:" + strconv.FormatInt(now, 10)
	ttl := time.Duration(StatsWindow+10) * time.Second

	pipe := rdb.Pipeline()
	pipe.Incr(ctx, "stats:total")
	pipe.Incr(ctx, secKey)
	pipe.Expire(ctx, secKey, ttl)
	if cached {
		pipe.Incr(ctx, "stats:cached")
		k := "stats:cached:sec:" + strconv.FormatInt(now, 10)
		pipe.Incr(ctx, k)
		pipe.Expire(ctx, k, ttl)
	}
	if rejected {
		pipe.Incr(ctx, "stats:rejected")
		k := "stats:rej:sec:" + strconv.FormatInt(now, 10)
		pipe.Incr(ctx, k)
		pipe.Expire(ctx, k, ttl)
	}
	if errored {
		pipe.Incr(ctx, "stats:errors")
		k := "stats:err:sec:" + strconv.FormatInt(now, 10)
		pipe.Incr(ctx, k)
		pipe.Expire(ctx, k, ttl)
	}
	pipe.Exec(ctx)
}

// GetCounter returns a cumulative counter (0 if missing).
func GetCounter(ctx context.Context, rdb *redis.Client, key string) int64 {
	v, err := rdb.Get(ctx, key).Int64()
	if err != nil {
		return 0
	}
	return v
}

// GetSeries returns the last `window` per-second samples for `prefix`
// (e.g. "stats:sec:" or "stats:cached:sec:"). Samples are ordered oldest→newest
// and missing seconds are returned as 0 so charts stay continuous.
func GetSeries(ctx context.Context, rdb *redis.Client, prefix string, window int) []SecPoint {
	now := time.Now().Unix()
	pipe := rdb.Pipeline()
	cmds := make([]*redis.StringCmd, 0, window)
	for i := window - 1; i >= 0; i-- {
		sec := now - int64(i)
		cmds = append(cmds, pipe.Get(ctx, prefix+strconv.FormatInt(sec, 10)))
	}
	pipe.Exec(ctx)

	pts := make([]SecPoint, 0, window)
	for i, c := range cmds {
		v, _ := c.Int64()
		pts = append(pts, SecPoint{Ts: now - int64(window-1-i), Count: v})
	}
	return pts
}
