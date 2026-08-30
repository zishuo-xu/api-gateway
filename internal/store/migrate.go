package store

import (
	"context"
	"database/sql"
	"log"

	_ "embed"
)

//go:embed migrate.sql
var migrateSQL string

// AutoMigrate applies migrate.sql at startup.
//
// init.sql only runs when a Postgres data directory is first created, so an
// existing deployment would otherwise never pick up new columns or tables.
// Every statement in migrate.sql is idempotent, which makes this safe to run on
// every boot - including on several replicas racing at the same time.
func AutoMigrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, migrateSQL); err != nil {
		return err
	}
	log.Printf("schema migration applied")
	return nil
}

// BackfillChannels creates a default channel for every route that has none, so
// the channel-based selector never has to special-case legacy routes.
func BackfillChannels(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, `
		INSERT INTO channels (route_id, name, base_url, downstream_auth_key, api_format, weight, priority, enabled)
		SELECT r.id, r.name, r.base_url, COALESCE(r.downstream_auth_key,''), COALESCE(r.api_format,'generic'), 1, 0, true
		FROM routes r
		WHERE r.status = 1
		  AND NOT EXISTS (SELECT 1 FROM channels c WHERE c.route_id = r.id)
	`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("backfilled %d default channels", n)
	}
	return n, nil
}
