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
	// RequestBudgetSec caps the whole failover chain, across every attempt.
	// Without it the worst case is MaxAttempts × UpstreamTimeoutSec — 3 × 180 s
	// = nine minutes, most of it spent after the caller stopped listening.
	//
	// Set this just under the client's own timeout, otherwise a retry can only
	// run once nobody is listening any more and all it does is spend upstream
	// quota. opencode defaults to 60 s, so 55 is the number to use with it.
	//
	// The budget bounds the wait for a response header only. Once an upstream
	// starts answering, streaming onwards to the caller is no longer charged
	// against it, so this never truncates a long answer.
	RequestBudgetSec int
	// TrustProxy decides whether X-Forwarded-For / X-Real-IP are used for the
	// per-key IP allowlist. Off by default: trusting them blindly lets any
	// client forge its source address and walk through the allowlist.
	TrustProxy bool
	// InjectStreamUsage adds stream_options.include_usage to streaming chat
	// requests. Without it providers omit the final usage event and every
	// streamed call would bill as a flat single unit.
	InjectStreamUsage bool
	// NormalizeParams rewrites request parameters that upstreams reject on
	// shape alone, currently reasoning_effort: casing is folded to the level
	// the upstream implements and unknown levels are dropped instead of
	// turning the whole call into a 400.
	NormalizeParams bool
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
	// MihomoAPI is the base URL of a mihomo external controller, e.g.
	// http://mihomo:9090. Empty disables the console's egress page entirely —
	// this gateway has no mihomo dependency, the page is only wired up when
	// someone actually runs one alongside it.
	// Keep it on a private network: the controller is unauthenticated and
	// exposes every upstream credential the proxy holds.
	MihomoAPI string
	// MihomoGroup is the proxy group the console switches. It must be a
	// `select` group: mihomo's url-test groups re-pick the fastest node every
	// interval and would silently overwrite a manual choice within minutes.
	MihomoGroup string
	// MihomoAutoGroup is the url-test group offered as the "give control back
	// to mihomo" option. Empty hides that option.
	MihomoAutoGroup string

	// Circuit-breaker tuning. A circuit is per upstream (not per route), so
	// two routes pointing at the same provider share failure history — which
	// is what you want while that provider is down.
	//
	// CBFailThreshold is how many failures open a circuit. Successes do not
	// clear the counter outright, they decrement it, so an upstream that fails
	// half the time still trips eventually instead of never accumulating
	// enough failures in a row.
	CBFailThreshold int
	// CBOpenSec is how long an open circuit refuses requests outright before
	// letting a single probe through.
	CBOpenSec int
	// CBProbeSec is how long that one probe is given to come back before the
	// circuit opens again. Only one probe is in flight at a time, so a
	// recovering upstream is never hit by the full traffic spike at once.
	CBProbeSec int
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
		RequestBudgetSec:   getenvInt("REQUEST_BUDGET_SEC", 300),
		TrustProxy:         getenvBool("TRUST_PROXY", false),
		InjectStreamUsage:  getenvBool("INJECT_STREAM_USAGE", true),
		NormalizeParams:    getenvBool("NORMALIZE_PARAMS", true),
		QuotaFlushSec:      getenvInt("QUOTA_FLUSH_SEC", 10),
		AutoMigrate:        getenvBool("AUTO_MIGRATE", true),
		UnifiedPrefix:      getenv("UNIFIED_PREFIX", "/v1"),
		RouteReloadSec:     getenvInt("ROUTE_RELOAD_SEC", 10),
		IPRPS:              getenvInt("IP_RPS", 0),
		MihomoAPI:          getenv("MIHOMO_API", ""),
		MihomoGroup:        getenv("MIHOMO_GROUP", "PROXY"),
		MihomoAutoGroup:    getenv("MIHOMO_AUTO_GROUP", "AUTO"),

		CBFailThreshold: getenvInt("CB_FAIL_THRESHOLD", 5),
		CBOpenSec:       getenvInt("CB_OPEN_SEC", 10),
		CBProbeSec:      getenvInt("CB_PROBE_SEC", 5),
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
