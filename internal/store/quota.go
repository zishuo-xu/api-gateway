package store

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Quota accounting runs on Redis and is flushed to Postgres in the background.
//
// Why not UPDATE api_keys on every request: that puts a write transaction on
// the hot path and serialises concurrent requests for the same key. Redis
// INCRBY is atomic and cheap; Postgres only needs the running total for
// reporting and for surviving a restart.
//
// The trade-off is that quota_used in Postgres lags by up to one flush
// interval. At startup SyncAllKeys warms each counter from that value, so a
// restart resumes from where it left off instead of granting a fresh allowance.

// quotaKey / dirtySet are the two Redis keys used by the quota subsystem.
func quotaKey(id int64) string { return "quota:" + strconv.FormatInt(id, 10) }

const dirtySet = "quota:dirty"

// DefaultQuotaFlushInterval is how often Redis counters land in Postgres.
const DefaultQuotaFlushInterval = 10 * time.Second

// WarmQuota seeds the Redis counter from Postgres if it is not there yet.
// SetNX (not Set) is what makes this safe to call concurrently: whichever
// request wins the race installs the persisted total, the rest keep counting on
// top of it instead of resetting it.
func WarmQuota(ctx context.Context, db *sql.DB, rdb *redis.Client, id int64) {
	if rdb == nil {
		return
	}
	var used int64
	if err := db.QueryRowContext(ctx, `SELECT quota_used FROM api_keys WHERE id=$1`, id).Scan(&used); err != nil {
		return
	}
	if err := rdb.SetNX(ctx, quotaKey(id), used, 0).Err(); err != nil {
		log.Printf("quota warm key=%d err=%v", id, err)
	}
}

// QuotaUsed returns how much of the allowance a key has consumed.
// A counter missing from Redis is rehydrated from Postgres first.
func QuotaUsed(ctx context.Context, rdb *redis.Client, db *sql.DB, id int64) int64 {
	if rdb == nil {
		var used int64
		_ = db.QueryRowContext(ctx, `SELECT quota_used FROM api_keys WHERE id=$1`, id).Scan(&used)
		return used
	}
	v, err := rdb.Get(ctx, quotaKey(id)).Int64()
	if err == nil {
		return v
	}
	if db != nil {
		WarmQuota(ctx, db, rdb, id)
		if v2, err2 := rdb.Get(ctx, quotaKey(id)).Int64(); err2 == nil {
			return v2
		}
	}
	return 0
}

// QuotaAdd charges `delta` units to a key and marks it for the next flush.
// A no-op when delta <= 0, so callers can pass a raw token count without
// special-casing responses that carry no usage information.
func QuotaAdd(ctx context.Context, rdb *redis.Client, id int64, delta int64) error {
	if rdb == nil || delta <= 0 {
		return nil
	}
	pipe := rdb.Pipeline()
	pipe.IncrBy(ctx, quotaKey(id), delta)
	pipe.SAdd(ctx, dirtySet, id)
	_, err := pipe.Exec(ctx)
	return err
}

// FlushQuota writes every dirty counter back to Postgres and drops it from the
// dirty set only once the write succeeded, so a transient DB error retries on
// the next tick instead of silently losing usage.
func FlushQuota(ctx context.Context, db *sql.DB, rdb *redis.Client) error {
	if rdb == nil || db == nil {
		return nil
	}
	raw, err := rdb.SMembers(ctx, dirtySet).Result()
	if err != nil || len(raw) == 0 {
		return err
	}
	ids := make([]int64, 0, len(raw))
	for _, s := range raw {
		if id, e := strconv.ParseInt(s, 10, 64); e == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}

	pipe := rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, quotaKey(id))
	}
	// Exec reports redis.Nil when any member vanished; the per-command errors
	// below are the authoritative per-key result, so only bail on real failures.
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}

	flushed := make([]interface{}, 0, len(ids))
	for i, c := range cmds {
		v, err := c.Int64()
		if err != nil {
			continue // counter expired/never existed: nothing durable to write
		}
		if _, err := db.ExecContext(ctx, `UPDATE api_keys SET quota_used=$2 WHERE id=$1`, ids[i], v); err != nil {
			log.Printf("quota flush key=%d err=%v", ids[i], err)
			continue
		}
		flushed = append(flushed, ids[i])
	}
	if len(flushed) == 0 {
		return nil
	}
	return rdb.SRem(ctx, dirtySet, flushed...).Err()
}

// StartQuotaFlusher runs FlushQuota on a timer. The returned stop function
// performs one last flush so a clean shutdown does not drop the tail.
func StartQuotaFlusher(db *sql.DB, rdb *redis.Client, interval time.Duration) func() {
	if rdb == nil || db == nil {
		return func() {}
	}
	if interval <= 0 {
		interval = DefaultQuotaFlushInterval
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := FlushQuota(context.Background(), db, rdb); err != nil {
					log.Printf("quota flush err: %v", err)
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		if err := FlushQuota(context.Background(), db, rdb); err != nil {
			log.Printf("quota final flush err: %v", err)
		}
	}
}
