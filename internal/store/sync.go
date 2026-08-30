package store

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// Route versioning lets several gateway replicas share one route table.
//
// Routes and channels are held in process memory for fast matching, so an
// admin change applied on one replica would otherwise never reach the others.
// A single Redis counter acts as a revision marker: the replica that performs
// the change bumps it, and every replica's watcher reloads when it notices the
// number moved.
//
// Redis is only the doorbell — Postgres stays the source of truth. If the
// counter is lost (Redis restart, FLUSHDB) replicas simply fall back to the
// periodic full reload.

const routeVersionKey = "routes:version"

// BumpRouteVersion notifies every other replica that the route table changed.
// A missing Redis client is not an error: a single-instance deployment has
// nobody to notify.
func BumpRouteVersion(ctx context.Context, rdb *redis.Client) {
	if rdb == nil {
		return
	}
	if err := rdb.Incr(ctx, routeVersionKey).Err(); err != nil {
		log.Printf("route version bump err: %v", err)
	}
}

// RouteVersion returns the current revision, or -1 when it cannot be read.
// -1 (rather than 0) so a fresh counter still counts as "newer than nothing".
func RouteVersion(ctx context.Context, rdb *redis.Client) int64 {
	if rdb == nil {
		return -1
	}
	v, err := rdb.Get(ctx, routeVersionKey).Int64()
	if err != nil {
		return -1
	}
	return v
}
