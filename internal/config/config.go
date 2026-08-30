package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds runtime configuration.
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	PGDSN         string
	GatewayAddr   string
	DemoAPIKey    string
	AdminToken    string
	// UpstreamTimeoutSec bounds a single upstream request, including reading a
	// streamed body to completion. LLM providers routinely need tens of seconds
	// (reasoning models far more), so this must be generous by default — a short
	// value silently truncates long answers with a 502.
	UpstreamTimeoutSec int
	// MaxAttempts is how many channels a request may try before failing. 1
	// disables failover entirely.
	MaxAttempts int
	// TrustProxy decides whether X-Forwarded-For / X-Real-IP are used for the
	// per-key IP allowlist. Off by default: trusting them blindly lets any
	// client forge its source address and walk through the allowlist.
	TrustProxy bool
	// InjectStreamUsage adds stream_options.include_usage to streaming chat
	// requests. Without it providers omit the final usage event and every
	// streamed call would bill as a flat single unit.
	InjectStreamUsage bool
	// QuotaFlushSec is how often Redis quota counters are written back to
	// Postgres.
	QuotaFlushSec int
	// AutoMigrate applies migrate.sql at startup. Idempotent, and the only way
	// an existing deployment picks up new columns without manual SQL.
	AutoMigrate bool
	// UnifiedPrefix is the OpenAI-compatible entry point: a client sets its
	// base_url to this prefix and the gateway picks the upstream from the
	// "model" field in the request body. Must start with "/" to take effect —
	// set it to "off" to expose only the per-route prefixes.
	UnifiedPrefix string
	// RouteReloadSec is how often a replica checks whether another replica
	// changed the route table. 0 disables the watcher (single instance).
	RouteReloadSec int
	// IPRPS caps requests per client IP. 0 disables that dimension — useful when
	// everything arrives through one NAT or egress gateway.
	IPRPS int
}

// Load reads config from environment with sensible defaults.
func Load() *Config {
	return &Config{
		RedisAddr:          getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getenv("REDIS_PASSWORD", ""),
		RedisDB:            0,
		PGDSN:              getenv("PG_DSN", "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"),
		GatewayAddr:        getenv("GATEWAY_ADDR", ":8080"),
		DemoAPIKey:         getenv("DEMO_API_KEY", ""),
		AdminToken:         getenv("ADMIN_TOKEN", ""),
		UpstreamTimeoutSec: getenvInt("UPSTREAM_TIMEOUT_SEC", 180),
		MaxAttempts:        getenvInt("MAX_ATTEMPTS", 3),
		TrustProxy:         getenvBool("TRUST_PROXY", false),
		InjectStreamUsage:  getenvBool("INJECT_STREAM_USAGE", true),
		QuotaFlushSec:      getenvInt("QUOTA_FLUSH_SEC", 10),
		AutoMigrate:        getenvBool("AUTO_MIGRATE", true),
		UnifiedPrefix:      getenv("UNIFIED_PREFIX", "/v1"),
		RouteReloadSec:     getenvInt("ROUTE_RELOAD_SEC", 10),
		IPRPS:              getenvInt("IP_RPS", 0),
	}
}

func getenvBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
