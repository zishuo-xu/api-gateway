package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/gateway"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	rdb := store.NewRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	db, err := store.NewDB(cfg.PGDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	// Bring an existing database up to the current schema before anything reads
	// from it, otherwise the new columns simply do not exist yet.
	if cfg.AutoMigrate {
		if err := store.AutoMigrate(ctx, db); err != nil {
			log.Fatalf("migrate: %v", err)
		}
	}
	if _, err := store.BackfillChannels(ctx, db); err != nil {
		log.Printf("backfill channels warn: %v", err)
	}

	if cfg.DemoAPIKey != "" {
		if err := store.SeedDemoKey(ctx, db, rdb, cfg.DemoAPIKey, "demo"); err != nil {
			log.Printf("seed key warn: %v", err)
		}
	}

	// Refresh the key cache from Postgres on boot: it is the source of truth for
	// expiry / IP / model policy, and Redis entries written by an older build
	// hold only a bare id.
	if n, err := store.SyncAllKeys(ctx, db, rdb); err != nil {
		log.Printf("sync keys warn: %v", err)
	} else {
		log.Printf("synced %d api keys into cache", n)
	}

	routes, err := gateway.LoadRoutes(db)
	if err != nil {
		log.Fatalf("load routes: %v", err)
	}
	chans := 0
	for _, r := range routes {
		chans += len(r.Channels)
	}
	log.Printf("loaded %d routes (%d channels)", len(routes), chans)

	// The admin token is the one secret that unlocks key issuance and the
	// stored upstream credentials, so a weak one is worth shouting about at
	// startup — while there is still time to fix it, rather than after.
	switch {
	case cfg.AdminToken == "":
		log.Printf("WARNING: ADMIN_TOKEN is empty; the admin console is disabled")
	case cfg.AdminToken == "admin-secret-change-me":
		log.Printf("WARNING: ADMIN_TOKEN is the documented example value; change it. " +
			"Generate one with: openssl rand -hex 24")
	case len(cfg.AdminToken) < 20:
		log.Printf("WARNING: ADMIN_TOKEN is shorter than 20 chars. On a public host " +
			"generate one with: openssl rand -hex 24")
	}

	auditor, stopAudit := store.StartAuditor(db, 1024)
	defer stopAudit()

	stopQuota := store.StartQuotaFlusher(db, rdb, time.Duration(cfg.QuotaFlushSec)*time.Second)
	defer stopQuota()

	srv := &gateway.Server{
		Cfg:     cfg,
		RDB:     rdb,
		DB:      db,
		Routes:  routes,
		Auditor: auditor,
		Start:   time.Now(),
	}
	// Without this a route/channel change made on one replica never reaches the
	// others: the table lives in process memory, and only the replica that
	// served the admin request would reload it.
	stopReloader := srv.StartRouteReloader(time.Duration(cfg.RouteReloadSec)*time.Second, 0)
	defer stopReloader()

	s := &http.Server{
		Addr:    cfg.GatewayAddr,
		Handler: srv.Handler(),
	}

	go func() {
		log.Printf("gateway listening on %s", cfg.GatewayAddr)
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Shutdown(sctx)
}
