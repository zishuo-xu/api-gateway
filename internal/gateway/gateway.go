package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// Route is an upstream route loaded from DB. It owns the public path prefix and
// the shared policy (rate limit, cache, model allowlist); the actual upstreams
// live in Channels so one prefix can fan out to several providers.
type Route struct {
	ID                int64
	Name              string
	BaseURL           string
	MatchPrefix       string // e.g. /v1/weather
	Upstream          string // upstream key for CB/limit
	UpstreamRPS       int
	CacheTTL          int
	CBEnabled         bool
	APIFormat         string   // openai-chat / openai-responses / anthropic-messages / generic
	DownstreamAuthKey string   // provider API key (gateway injects on forward)
	Models            []string // allowed model IDs (parsed from JSON)
	// CacheScope decides whether a cached GET response is shared by every API
	// key ("global", the default) or kept per key ("key"). See mwCache — getting
	// this wrong is a cross-user data leak, not just a stale answer.
	CacheScope string
	Channels   []Channel
}

// Channel is one concrete upstream behind a route.
//
//   - Priority splits channels into tiers: every channel in the lowest-numbered
//     healthy tier is preferred, and the next tier is only a fallback.
//   - Weight splits traffic inside a tier, proportionally.
type Channel struct {
	ID                int64  `json:"id"`
	RouteID           int64  `json:"route_id"`
	Name              string `json:"name"`
	BaseURL           string `json:"base_url"`
	APIFormat         string `json:"api_format"`
	DownstreamAuthKey string `json:"-"` // never serialised to the console
	Weight            int    `json:"weight"`
	Priority          int    `json:"priority"`
	Enabled           bool   `json:"enabled"`
}

// upstreamKey names the circuit-breaker bucket for a channel. Circuit state is
// per upstream, not per route, so two routes pointing at the same provider
// share failure history - which is what you want when that provider is down.
func (c Channel) upstreamKey(routeName string) string {
	if routeName == "" {
		return c.Name
	}
	return routeName + "#" + c.Name
}

// Label is a stable human name for logs and UI.
func (c Channel) Label() string {
	if c.Name != "" {
		return c.Name
	}
	return fmt.Sprintf("ch-%d", c.ID)
}

// Server holds dependencies and routes.
type Server struct {
	Cfg     *config.Config
	RDB     *redis.Client
	DB      *sql.DB
	Routes  []Route
	Auditor chan store.LogEntry
	Start   time.Time
	mu      sync.RWMutex // guards Routes against concurrent admin reloads

	clientOnce sync.Once
	client     *http.Client

	// logins rate-limits admin login failures per client IP. Lazy-init:
	// tests build Server structs by hand without it.
	logins *loginGate

	statMu   sync.Mutex // guards the in-memory latency histogram
	latency  *histogram
	upstream *histogram
	tokens   [2]int64 // [prompt, completion]

	// proxyDelays remembers the last full egress delay sweep this process ran.
	// It is the only source of a live answer to "is this node up right now";
	// mihomo itself will happily re-probe a whole group but keeps no readable
	// record of the outcome.
	// Lazy-init so tests can build a Server by hand.
	proxyOnce   sync.Once
	proxyDelays *proxyDelayCache

	// proxyMeta remembers per-node protocol and mihomo's own last probe result,
	// which come from GET /providers/proxies — a ~110 KB payload, far too
	// heavy to refetch on every two-second dashboard refresh.
	metaOnce  sync.Once
	proxyMeta *proxyMetaCache
}

type ctxKey int

const ctxKeyMeta ctxKey = iota

// meta is a per-request bag mutated by inner middlewares and read by logging/audit.
type meta struct {
	APIKeyID         int64
	RouteID          int64
	Upstream         string
	Cached           bool
	KeyInfo          store.KeyInfo
	Model            string
	PromptTokens     int64
	CompletionTokens int64
	IsStream         bool
	ChannelID        int64
	// CacheHitTokens is the part of PromptTokens the provider served from its
	// own prefix cache and therefore billed at the discounted rate. Zero means
	// "the provider reported nothing", which is not the same as a real 0%.
	CacheHitTokens int64
	// CacheWriteTokens is the part of PromptTokens the provider charged a
	// premium to store. Only Anthropic reports it.
	CacheWriteTokens int64
	// Billed is set once a request actually reached an upstream. Gateway-side
	// rejections (401/404/429/403) leave it false, so a client that never
	// consumed upstream capacity is not charged for it.
	Billed bool
	ErrMsg string
	// RejectReason names the gate that turned this request away, in a short
	// stable token (quota, rate_ip, rate_route, no_route, ...). It is empty
	// for anything that reached an upstream.
	//
	// Without it a 429 in the log is ambiguous between "you spent your
	// allowance" and "you are hammering us", and a 403 between an expired
	// key and a model the key may not call. Those have completely different
	// fixes, and the status code alone cannot tell them apart.
	RejectReason string
	// StartedAt is the wall-clock time when mwLogging began handling this
	// request. Used to derive TTFT (firstWriteAt - StartedAt) in the
	// streaming proxy path.
	StartedAt time.Time
	// TTFTMs is Time To First Token in milliseconds. Only meaningful for
	// streaming: wall-clock from request start to the first SSE data chunk.
	// Zero for non-streaming, cache hits, and requests rejected before proxy.
	TTFTMs int64
}

func (m *meta) totalTokens() int64 {
	return m.PromptTokens + m.CompletionTokens
}

func metaFrom(r *http.Request) *meta {
	m, _ := r.Context().Value(ctxKeyMeta).(*meta)
	return m
}

// setReject records why a request was turned away. It tolerates a missing
// meta because the credential failures fire before mwAuth has resolved one —
// and those are precisely the rejections an operator most wants labelled.
func setReject(r *http.Request, reason string) {
	if m := metaFrom(r); m != nil {
		m.RejectReason = reason
	}
}

// withMeta attaches a request bag to a context. Used by the public middleware
// chain and by entry points (playground, tests) that drive the proxy directly.
func withMeta(ctx context.Context, m *meta) context.Context {
	return context.WithValue(ctx, ctxKeyMeta, m)
}

// Handler builds the routing trees:
//   - public : /*        -> logging -> auth -> quota -> ratelimit -> cache -> proxy (needs X-API-Key)
//   - adminUI: /admin/   -> serves the dashboard HTML (token baked in for JS fetches)
//   - adminAPI: /admin/{metrics,keys,routes,channels,logs,usage,playground} + /metrics -> adminAuth
//   - favicon : /favicon.ico -> served directly, outside every middleware chain
//
// The dashboard page is served WITHOUT a header check so it can be opened in a
// plain browser; the page embeds ADMIN_TOKEN which its JS uses to authenticate
// the JSON fetches. The JSON endpoints themselves stay protected by mwAdminAuth.
// ServeMux prefers the exact patterns over the /admin/ prefix, so the page and
// the APIs never clash.
func (s *Server) Handler() http.Handler {
	pub := http.NewServeMux()
	pub.HandleFunc("/", s.proxy)
	var p http.Handler = pub
	p = s.mwCache(p)
	p = s.mwRateLimit(p)
	p = s.mwQuota(p)
	p = s.mwAuth(p)
	p = s.mwLogging(p)

	api := http.NewServeMux()
	api.HandleFunc("/admin/metrics", s.adminMetrics)
	api.HandleFunc("/admin/keys", s.adminKeys)
	api.HandleFunc("/admin/routes", s.adminRoutes)
	api.HandleFunc("/admin/channels", s.adminChannels)
	api.HandleFunc("/admin/logs", s.adminLogs)
	api.HandleFunc("/admin/usage", s.adminUsage)
	api.HandleFunc("/admin/playground", s.adminPlayground)
	api.HandleFunc("/admin/proxy", s.adminProxy)
	api.HandleFunc("/admin/proxy/retest", s.adminProxyRetest)
	api.HandleFunc("/metrics", s.adminPrometheus)
	var a http.Handler = api
	a = s.mwAdminAuth(a)

	ui := http.NewServeMux()
	ui.HandleFunc("/admin/", s.adminUI)

	root := http.NewServeMux()
	root.Handle("/admin/metrics", a)
	root.Handle("/admin/keys", a)
	root.Handle("/admin/routes", a)
	root.Handle("/admin/channels", a)
	root.Handle("/admin/logs", a)
	root.Handle("/admin/usage", a)
	root.Handle("/admin/playground", a)
	root.Handle("/admin/proxy", a)
	root.Handle("/admin/proxy/retest", a)
	root.Handle("/metrics", a)
	// Login/logout sit outside mwAdminAuth: login is how you earn the session,
	// and logout only ever revokes a session the caller already holds.
	root.HandleFunc("/admin/login", s.adminLogin)
	root.HandleFunc("/admin/logout", s.adminLogout)
	root.Handle("/admin/", ui)
	// Registered before the catch-all so the browser's automatic icon probe
	// never reaches the public chain. See favicon() for why that matters.
	root.HandleFunc("/favicon.ico", s.favicon)
	root.Handle("/", p)
	return root
}

// faviconSVG is the console icon, inlined so the binary needs no external
// static files. Colours follow the dashboard palette.
var faviconSVG = []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">` +
	`<rect width="32" height="32" rx="7" fill="#0b0e14"/>` +
	`<path d="M8 16h7M17 9l7 7-7 7" fill="none" stroke="#58a6ff" ` +
	`stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"/>` +
	`</svg>`)

// favicon answers the browser's automatic /favicon.ico probe.
//
// This must not go through the public chain. The probe never carries an API
// key, so mwAuth would reject it with 401 and mwLogging would then record a
// failed request — one per page load, per tab, forever. The audit log, the
// error-rate metric and the operator's patience all get spent on something
// that is not a failure at all.
//
// Handling it on the root mux means it never reaches mwLogging, so it costs
// nothing: no log line, no audit row, no counter.
func (s *Server) favicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	// Without this the browser re-probes on every navigation and the request
	// count stays high even though it is no longer being logged.
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(faviconSVG)
}

// ----- middleware -----

func (s *Server) mwLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m := &meta{}
		m.StartedAt = start
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), ctxKeyMeta, m)))
		latency := time.Since(start)

		// Smoke-test keys are flagged no_log so the failures they provoke on
		// purpose (quota 429, model 403, failover probes) stop burying real
		// traffic in the request log. Only the record-keeping is skipped; the
		// quota charge below still applies.
		//
		// A request rejected before authentication never populates KeyInfo, so
		// credential failures stay logged regardless. That is the intent: this
		// flag exists for self-inflicted test traffic, not as a way to hide.
		silent := m.KeyInfo.NoLog

		if !silent {
			log.Printf("%s %s -> %d %s cached=%v model=%q tokens=%d ch=%d",
				r.Method, r.URL.Path, sw.status, latency, m.Cached, m.Model, m.totalTokens(), m.ChannelID)

			s.observe(latency, m)

			// live metrics: pipelined counters, excluded from audit (meta is empty for these)
			store.RecordRequest(r.Context(), s.RDB, m.Cached,
				sw.status == http.StatusTooManyRequests, sw.status >= 500)
		}

		// Charge the key only when it actually consumed upstream capacity.
		// Every billed call costs at least 1 so responses with no usage block
		// (generic APIs, streams without include_usage) still count.
		//
		// Prompt tokens the provider served from its own prefix cache are
		// billed by the provider at a steep discount (DeepSeek ~1/10), so
		// charging them at full quota price would exhaust a key's allowance
		// up to 10x earlier than the real bill does. CacheHitTokens is
		// subtracted from the charge; it is already recorded separately in
		// the request log for the cost dashboard.
		//
		// Deliberately outside the silent branch: the e2e suite asserts that a
		// key runs out of quota. Exempting test keys from billing would make
		// the very assertion that motivates this flag stop meaning anything.
		if m.APIKeyID > 0 && m.Billed {
			delta := m.totalTokens() - m.CacheHitTokens
			if delta <= 0 {
				delta = 1
			}
			if err := store.QuotaAdd(r.Context(), s.RDB, m.APIKeyID, delta); err != nil {
				log.Printf("quota add key=%d err=%v", m.APIKeyID, err)
			}
		}

		if s.Auditor != nil && !silent {
			// Compute throughput. For streaming, the generation window is
			// (latency - ttft): ttft is the "thinking" phase, tokens arrive
			// only after. For buffered responses the whole latency is generation.
			var genMs int64 = latency.Milliseconds()
			if m.TTFTMs > 0 && genMs > m.TTFTMs {
				genMs -= m.TTFTMs
			}
			var tps float64
			if genMs > 0 {
				if m.IsStream && m.CompletionTokens > 0 {
					tps = float64(m.CompletionTokens) / float64(genMs) * 1000
				} else if total := m.totalTokens(); total > 0 {
					tps = float64(total) / float64(genMs) * 1000
				}
			}

			s.Auditor <- store.LogEntry{
				APIKeyID:         m.APIKeyID,
				RouteID:          m.RouteID,
				Method:           r.Method,
				Path:             r.URL.Path,
				Upstream:         m.Upstream,
				StatusCode:       sw.status,
				LatencyMs:        int(latency.Milliseconds()),
				Cached:           m.Cached,
				Model:            m.Model,
				PromptTokens:     m.PromptTokens,
				CompletionTokens: m.CompletionTokens,
				TotalTokens:      m.totalTokens(),
				IsStream:         m.IsStream,
				ChannelID:        m.ChannelID,
				ErrMsg:           m.ErrMsg,
				TTFTMs:           m.TTFTMs,
				TokensPerSec:     tps,
				CacheHitTokens:   m.CacheHitTokens,
				CacheWriteTokens: m.CacheWriteTokens,
				RejectReason:     m.RejectReason,
			}
		}
	})
}

// authHeader names which request header carried the gateway credential. Both
// spellings are removed once authenticated: forwarding either one would hand
// the caller's gateway key to the provider — a credential leak, and also a
// routing bug, because injectCredentials would see a non-empty Authorization
// and skip injecting the provider's own key.
//
// It still records which header won, because that is what tells an operator a
// client is using the X-API-Key convention rather than the SDK's Bearer.
type authHeader int

const (
	authNone authHeader = iota
	authXAPIKey
	authBearer
)

// extractGatewayKey pulls the caller's credential out of the request.
//
// Two spellings are accepted because "drop-in OpenAI compatible" has to mean
// the official SDK works unmodified, and that SDK only ever sends
// "Authorization: Bearer <key>". X-API-Key is tried first so existing clients
// and docs keep working unchanged.
func extractGatewayKey(r *http.Request) (key string, which authHeader) {
	if v := strings.TrimSpace(r.Header.Get("X-API-Key")); v != "" {
		return v, authXAPIKey
	}
	if ah := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(ah, "Bearer ") {
		if v := strings.TrimSpace(strings.TrimPrefix(ah, "Bearer ")); v != "" {
			return v, authBearer
		}
	}
	return "", authNone
}

func (s *Server) mwAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, which := extractGatewayKey(r)
		if which == authNone {
			setReject(r, "no_key")
			http.Error(w, "missing api key", http.StatusUnauthorized)
			return
		}
		hash := store.HashKey(raw)
		v, err := s.RDB.Get(r.Context(), "key:"+hash).Result()
		ki, ok := store.DecodeKeyInfo(v)
		if err != nil || !ok {
			setReject(r, "bad_key")
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
		// Populate the identity BEFORE any policy check.
		//
		// mwLogging reads KeyInfo.NoLog to decide whether to record the
		// request, so a key has to be identifiable even while being turned
		// away. Checking first meant a smoke-test key blocked by its own
		// expiry or IP allowlist - precisely the failures such a key exists to
		// provoke - landed in the log anyway.
		//
		// Rejected requests never reach the proxy, so m.Billed stays false and
		// none of this charges the key.
		//
		// A missing or forged credential still returns above without ever
		// resolving to a key, so those stay logged: there is no owner to
		// attribute them to, and that is exactly what an operator needs to see.
		m := metaFrom(r)
		m.APIKeyID = ki.ID
		m.KeyInfo = ki

		// Expiry and source-IP are enforced here rather than in the console:
		// a key that has lapsed or is being used from outside its allowlist must
		// never reach an upstream, even if the Redis entry is stale.
		if ki.Expired(time.Now()) {
			m.RejectReason = "expired"
			http.Error(w, "api key expired", http.StatusForbidden)
			return
		}
		if !ki.IPAllowed(clientIP(r, s.Cfg != nil && s.Cfg.TrustProxy)) {
			m.RejectReason = "ip_denied"
			http.Error(w, "source ip not allowed", http.StatusForbidden)
			return
		}
		// Consume the credential: no spelling of it may travel upstream.
		// Without this, an OpenAI SDK client's
		// "Authorization: Bearer gw-..." would be relayed verbatim to the
		// provider, and injectCredentials would then skip injecting the real
		// provider key because the slot is already occupied.
		//
		// Both spellings go, not just the one that authenticated. Several
		// clients set X-API-Key and Authorization together; deleting only the
		// winner left the other one in place, so the provider received the
		// caller's gateway key and answered 401 Invalid API key — while the
		// gateway itself had happily authenticated the very same call. A
		// caller who genuinely wants to supply an upstream credential uses
		// X-Provider-Key, which is passed through untouched.
		r.Header.Del("X-API-Key")
		r.Header.Del("Authorization")
		next.ServeHTTP(w, r)
	})
}

// mwQuota rejects a key once it has burnt through its allowance.
func (s *Server) mwQuota(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := metaFrom(r)
		if m == nil || m.KeyInfo.QuotaLimit <= 0 {
			next.ServeHTTP(w, r)
			return
		}
		used := store.QuotaUsed(r.Context(), s.RDB, s.DB, m.KeyInfo.ID)
		if used >= m.KeyInfo.QuotaLimit {
			m.RejectReason = "quota"
			w.Header().Set("X-Quota-Limit", strconv.FormatInt(m.KeyInfo.QuotaLimit, 10))
			w.Header().Set("X-Quota-Used", strconv.FormatInt(used, 10))
			http.Error(w, fmt.Sprintf("quota exceeded: %d/%d", used, m.KeyInfo.QuotaLimit),
				http.StatusTooManyRequests)
			return
		}
		w.Header().Set("X-Quota-Limit", strconv.FormatInt(m.KeyInfo.QuotaLimit, 10))
		w.Header().Set("X-Quota-Used", strconv.FormatInt(used, 10))
		next.ServeHTTP(w, r)
	})
}

// mwRateLimit throttles on three dimensions at once, all taken atomically:
//
//  1. the upstream as a whole  - the cap that actually protects the provider
//  2. the API key              - stops one key from crowding out the others
//  3. the client IP (optional) - IP_RPS, off by default
//
// Dimension 1 is the one that used to be missing. With a per-key bucket alone,
// N active keys could each spend the route's upstream_rps and together push
// N x that at the provider: the configured number protected nothing, and the
// provider saw the sum.
func (s *Server) mwRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// On the unified entry the upstream is only known after the body is
		// parsed (in proxy), so fall back to a bucket named after the prefix.
		// Coarser, but proxy reports the proper 404/400 for an unroutable path;
		// this middleware must not pre-empt it.
		rps := 50
		dim := s.unifiedPrefix()
		if route := s.matchRoute(r.URL.Path); route != nil {
			rps = route.UpstreamRPS
			dim = route.Upstream
		}
		if rps <= 0 {
			rps = 50
		}
		if dim == "" {
			dim = "default"
		}
		m := metaFrom(r)

		keys := []string{
			"bucket:up:" + dim,
			"bucket:key:" + strconv.FormatInt(m.APIKeyID, 10) + ":" + dim,
		}
		args := []interface{}{
			time.Now().UnixMilli(), 1,
			rps * 2, rps, // global upstream bucket
			rps * 2, rps, // per-key bucket
		}
		if ipRPS := s.ipRPS(); ipRPS > 0 {
			keys = append(keys, "bucket:ip:"+clientIP(r, s.Cfg != nil && s.Cfg.TrustProxy))
			args = append(args, ipRPS*2, ipRPS)
		}

		res, err := store.MultiBucketScript.Run(r.Context(), s.RDB, keys, args...).Result()
		if err != nil {
			http.Error(w, "rate limit error", http.StatusInternalServerError)
			return
		}
		arr, ok := res.([]interface{})
		if !ok || len(arr) < 2 {
			http.Error(w, "rate limit error", http.StatusInternalServerError)
			return
		}
		if arr[0].(int64) != 1 {
			// arr[1] names the bucket that refused: 1 = the provider-wide cap,
			// 2 = this key's own, 3 = the client IP. The three have different
			// remedies (add capacity, quiet this key, find the noisy address),
			// so they are recorded separately rather than as one "rate limited".
			switch arr[1].(int64) {
			case 3:
				setReject(r, "rate_ip")
				http.Error(w, "rate limited (source ip)", http.StatusTooManyRequests)
			case 2:
				setReject(r, "rate_key")
				http.Error(w, "rate limited", http.StatusTooManyRequests)
			default:
				setReject(r, "rate_upstream")
				http.Error(w, "rate limited", http.StatusTooManyRequests)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ipRPS returns the per-IP ceiling; 0 disables that dimension.
func (s *Server) ipRPS() int {
	if s.Cfg == nil {
		return 0
	}
	return s.Cfg.IPRPS
}

func (s *Server) mwCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only GET is cacheable. Requests that explicitly ask for an event stream
		// must bypass the buffering wrapper entirely, otherwise their chunks get
		// swallowed by captureWriter and never reach the client until the end.
		if r.Method != http.MethodGet ||
			strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream") {
			next.ServeHTTP(w, r)
			return
		}
		// The model listing depends on which key is asking (a restricted key must
		// not be shown models it cannot use), and it is cheap to compute, so it is
		// never cached - correctness beats saving one aggregation.
		if s.isModelListPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		m := metaFrom(r)
		route := s.matchRoute(r.URL.Path)
		ttl := 30
		scope := ""
		if route != nil {
			ttl = route.CacheTTL
			scope = route.CacheScope
		}
		// "key" scope keeps a private copy per API key. Without it the first
		// caller's response is served to whoever asks next - fine for weather,
		// a cross-user leak for anything user-specific.
		prefix := ""
		if scope == "key" {
			prefix = "key" + strconv.FormatInt(m.APIKeyID, 10) + ":"
		}
		key := "cache:" + prefix + hashOf(r.Method+r.URL.Path+"?"+r.URL.RawQuery)
		if data, err := s.RDB.Get(r.Context(), key).Bytes(); err == nil {
			m.Cached = true
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		w.Header().Set("X-Cache", "MISS")
		cw := &captureWriter{ResponseWriter: w, buf: &bytes.Buffer{}, status: http.StatusOK}
		next.ServeHTTP(cw, r)
		// Never persist a stream: it has no meaningful end and replaying it would
		// feed clients a stale, possibly truncated event sequence.
		if cw.status == http.StatusOK && !isStreamed(w.Header().Get("Content-Type")) {
			s.RDB.Set(r.Context(), key, cw.buf.Bytes(), time.Duration(ttl)*time.Second)
		}
		w.WriteHeader(cw.status)
		w.Write(cw.buf.Bytes())
	})
}

// isModelListPath reports whether this is the gateway's own model discovery
// endpoint on the unified entry.
func (s *Server) isModelListPath(path string) bool {
	p := s.unifiedPrefix()
	if p == "" {
		return false
	}
	return path == p || path == p+"/" || path == p+"/models"
}

// ----- proxy -----

// maxInspectBody bounds how much of a request body the gateway will read into
// memory. Bodies beyond this skip model inspection and become non-replayable
// (no failover retry), which keeps memory flat under large uploads.
const maxInspectBody = 1 << 20 // 1 MiB

// httpError carries the status code that should go back to the client. Resolving
// a request can fail in several distinct ways (no route, unknown model, missing
// model) and each needs its own code, not a blanket 404.
type httpError struct {
	status int
	msg    string
	// reason is the stable token recorded in the request log. Several of
	// these failures share a status code but need different fixes — a 404
	// for "no route" and one for "unknown model" are not the same problem —
	// so the code alone is not enough to act on later.
	reason string
}

func (e *httpError) Error() string { return e.msg }

// target is a resolved request: which route serves it, and which part of the
// path should be appended to the upstream base URL.
type target struct {
	route  *Route
	suffix string
	// listModels marks a discovery call (GET /v1/models) that the gateway
	// answers itself instead of forwarding.
	listModels bool
}

// unifiedPrefix returns the OpenAI-compatible entry prefix, or "" when the
// feature is off. Requiring a leading "/" means UNIFIED_PREFIX=off disables it
// without needing a second boolean flag.
func (s *Server) unifiedPrefix() string {
	if s.Cfg == nil {
		return ""
	}
	p := s.Cfg.UnifiedPrefix
	if !strings.HasPrefix(p, "/") {
		return ""
	}
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return ""
	}
	return p
}

// resolveTarget decides where a request goes.
//
// Two addressing schemes coexist:
//
//  1. Unified prefix ("/v1/..." by default): the prefix is a *virtual* version
//     number and is stripped before forwarding; the route is chosen by the
//     "model" in the request body. This is what makes the gateway drop-in
//     compatible with OpenAI clients — one base_url, switch provider by
//     changing the model name.
//  2. Per-route prefix ("/opencodeai/..."): one prefix bound to one route,
//     kept as an escape hatch for pinning a request to a specific provider.
func (s *Server) resolveTarget(path, model string) (*target, *httpError) {
	if prefix := s.unifiedPrefix(); prefix != "" {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			suffix := strings.TrimPrefix(path, prefix)
			if suffix == "" || suffix == "/" || suffix == "/models" {
				return &target{listModels: true}, nil
			}
			if model == "" {
				return nil, &httpError{status: http.StatusBadRequest, reason: "no_model",
					msg: `unified entry needs a "model" in the request body`}
			}
			rt := s.routeByModel(model)
			if rt == nil {
				avail := s.availableModels()
				if len(avail) == 0 {
					return nil, &httpError{status: http.StatusNotFound, reason: "no_model_allowlist",
						msg: "no route declares a model allowlist, so the unified entry has nothing to route to"}
				}
				return nil, &httpError{status: http.StatusNotFound, reason: "unknown_model",
					msg: fmt.Sprintf("unknown model %q; available: %s", model, strings.Join(avail, ", "))}
			}
			return &target{route: rt, suffix: suffix}, nil
		}
	}
	rt := s.matchRoute(path)
	if rt == nil {
		return nil, &httpError{status: http.StatusNotFound, reason: "no_route", msg: "no route"}
	}
	return &target{route: rt, suffix: strings.TrimPrefix(path, rt.MatchPrefix)}, nil
}

// routeByModel finds the route that declared this model in its allowlist.
func (s *Server) routeByModel(model string) *Route {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.Routes {
		if store.ModelInList(s.Routes[i].Models, model) {
			return &s.Routes[i]
		}
	}
	return nil
}

// availableModels collects every model any route has declared, sorted.
// Routes with an empty allowlist never appear here: letting them into the
// unified entry would make model-based routing ambiguous.
func (s *Server) availableModels() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for _, rt := range s.Routes {
		for _, m := range rt.Models {
			if m != "" && !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	sort.Strings(out)
	return out
}

// extractModel pulls the "model" field out of a JSON request body.
//
// The second result says whether a model could be *looked for*: true when the
// body was a JSON object the scanner understood, false when it was binary,
// compressed-looking garbage or a prefix cut before the top level ended. That
// distinction is what lets model allowlists be enforced without rejecting
// bodies that were simply not inspectable.
//
// Tolerance to truncation is the whole point. peekBody caps how much of a body
// it reads, so a request larger than the cap arrives here as JSON cut off
// mid-document — and json.Unmarshal rejects any truncated document outright.
// Every such request therefore used to look like it carried no model, and came
// back as `unified entry needs a "model" in the request body` for a body that
// plainly had one. The whole-body unmarshal stays as the fast path; when it
// fails, the prefix is walked token by token instead.
func extractModel(body []byte) (model string, seen bool) {
	if len(body) == 0 {
		// No body at all: a definitive "there is no model", not an unknown.
		return "", true
	}
	var probe struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &probe); err == nil {
		return probe.Model, true
	}
	return scanModelPrefix(body)
}

// scanModelPrefix walks the top level of a possibly truncated JSON object and
// returns its "model" value.
//
// complete is true only when the scanner reached the end of the top-level
// object, so a model that is merely absent can be told apart from one that sits
// past the cut. Nested objects and arrays are skipped wholesale: a "model" key
// inside a message or a tool definition must never be mistaken for the
// top-level one.
func scanModelPrefix(body []byte) (model string, complete bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return "", false
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return "", false
	}
	for {
		keyTok, err := dec.Token()
		if err != nil {
			return "", false
		}
		if d, ok := keyTok.(json.Delim); ok && d == '}' {
			return "", true // whole object read, no model in it
		}
		key, _ := keyTok.(string)
		valTok, err := dec.Token()
		if err != nil {
			return "", false
		}
		if d, ok := valTok.(json.Delim); ok && (d == '{' || d == '[') {
			if !skipJSONValue(dec) {
				return "", false
			}
			continue
		}
		if key == "model" {
			if s, ok := valTok.(string); ok {
				return s, true
			}
			return "", false
		}
	}
}

// skipJSONValue consumes the rest of an object or array whose opening delimiter
// has already been read. It returns false if the body ends first.
func skipJSONValue(dec *json.Decoder) bool {
	for depth := 1; depth > 0; {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return true
}

// inspectableBody returns the bytes model extraction should look at.
//
// A client that compresses its request body would otherwise be inspected as
// binary noise and rejected for "missing" a model it did send. Only a bounded
// prefix is decompressed — enough to find the model, not enough to blow up
// memory on a large upload. The compressed bytes are still what gets forwarded:
// the upstream accepted them, the gateway is only trying to read along.
func inspectableBody(r *http.Request, body []byte) []byte {
	if len(body) == 0 || !bytes.HasPrefix(body, gzipMagic) {
		return body
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return body
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, maxInspectBody))
	if err != nil {
		return body
	}
	return out
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	m := metaFrom(r)

	// Peek the body first: on the unified entry the model inside it is what
	// selects the route, so this has to happen before we know the route.
	// The peeked bytes are always handed back to r.Body, so the streaming path
	// still sees a complete request.
	body, oversized, peekErr := peekBody(r, maxInspectBody)
	if peekErr != nil {
		setReject(r, "bad_body")
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	model, modelSeen := extractModel(inspectableBody(r, body))
	tgt, herr := s.resolveTarget(r.URL.Path, model)
	if herr != nil {
		if m != nil {
			m.ErrMsg = herr.msg
			m.RejectReason = herr.reason
		}
		http.Error(w, herr.msg, herr.status)
		return
	}
	// Discovery: answer /v1/models ourselves so clients that probe the model
	// list on connect do not fail against a provider-specific endpoint.
	if tgt.listModels {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			setReject(r, "method")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.writeModelList(w, r)
		return
	}

	route := tgt.route
	if m != nil {
		// mwRateLimit runs before the body is parsed, so on the unified entry it
		// cannot know which route will serve the request. Record the real route
		// here, once it is known.
		m.RouteID = route.ID
		m.Upstream = route.Name
	}

	if err := s.authorizeModel(r, route, model, modelSeen, oversized); err != nil {
		if m != nil {
			m.ErrMsg = err.Error()
			m.RejectReason = "model_denied"
		}
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	// A stream that never reports usage leaves billing blind, so ask for it
	// unless the caller already specified stream_options.
	if !oversized && s.Cfg != nil && s.Cfg.InjectStreamUsage {
		body = injectStreamUsage(r, route, body)
	}
	// Upstreams reject reasoning_effort on casing or unknown levels alone, and
	// a rejected parameter takes the whole request down with it.
	if !oversized && s.Cfg != nil && s.Cfg.NormalizeParams {
		body = normalizeReasoningEffort(r, route, body)
	}

	order := s.orderedChannels(route)
	if len(order) == 0 {
		setReject(r, "no_channel")
		http.Error(w, "no healthy channel", http.StatusServiceUnavailable)
		return
	}
	attempts := s.maxAttempts()
	if attempts > len(order) {
		attempts = len(order)
	}

	replayable := !oversized && body != nil
	var lastErr string

	for i := 0; i < attempts; i++ {
		ch := order[i]
		if route.CBEnabled && s.isCircuitOpen(r.Context(), ch.upstreamKey(route.Name)) {
			lastErr = "circuit open"
			continue
		}
		if i > 0 {
			// Linear backoff: enough for a transient blip to clear without
			// stalling the caller on a genuinely dead channel.
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
			log.Printf("retry route=%s path=%s attempt=%d channel=%s", route.Name, r.URL.Path, i+1, ch.Label())
		}

		var reader io.Reader
		if replayable {
			reader = bytes.NewReader(body)
		}

		resp, err := s.forward(r.Context(), r, route, ch, tgt.suffix, reader)
		if err != nil {
			s.recordFailure(r.Context(), ch.upstreamKey(route.Name))
			lastErr = err.Error()
			continue
		}
		// Fail over on upstream-side failures only. A 4xx is the caller's
		// problem and replaying it against another channel just burns quota.
		if retryableStatus(resp.StatusCode) && i < attempts-1 {
			resp.Body.Close()
			s.recordFailure(r.Context(), ch.upstreamKey(route.Name))
			lastErr = fmt.Sprintf("upstream %d", resp.StatusCode)
			continue
		}
		s.recordSuccess(r.Context(), ch.upstreamKey(route.Name))
		s.writeUpstream(w, r, resp, route, ch)
		return
	}

	if m != nil {
		m.ErrMsg = lastErr
		if m.RejectReason == "" {
			m.RejectReason = "upstream"
		}
	}
	http.Error(w, "upstream error: "+lastErr, http.StatusBadGateway)
}

// retryableStatus marks upstream responses worth replaying on another channel.
// 429 is included because a rate-limited channel is exactly the case where a
// second channel would succeed.
func retryableStatus(code int) bool {
	return code >= 500 || code == http.StatusTooManyRequests || code == http.StatusRequestTimeout
}

// forward issues one upstream call for a channel.
//
// The caller passes an explicit body so a failed attempt can be replayed: on a
// retry, r.Body has already been drained, and re-sending an empty body would
// turn "channel A is down" into a confusing upstream 400.
//
// The caller also passes the path suffix. It cannot be derived here because the
// two addressing schemes strip different prefixes: the unified entry strips its
// virtual version prefix, a per-route entry strips that route's match prefix.
func (s *Server) forward(ctx context.Context, r *http.Request, route *Route, ch Channel, suffix string, body io.Reader) (*http.Response, error) {
	if body == nil && r.Body != nil {
		body = r.Body
	}
	// An empty suffix is meaningful and must stay empty: calling the route
	// prefix exactly ("POST /myroute") means "hit base_url as-is". Forcing a
	// trailing slash turns https://host/status/500 into /status/500/, which the
	// upstream answers 404 — and a 404 is not retryable, so failover would
	// silently never kick in.
	u := ch.BaseURL + suffix
	if ch.BaseURL == "" {
		u = route.BaseURL + suffix
	}
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u, body)
	if err != nil {
		return nil, err
	}
	for k, vv := range r.Header {
		if strings.EqualFold(k, "X-API-Key") {
			continue // do not leak our key upstream
		}
		// Relaying the client's Accept-Encoding verbatim switches off the
		// transport's transparent gzip handling: Go then assumes the caller
		// asked for compression and will decompress it downstream — and
		// nothing here does, so the usage parsers saw binary noise.
		if strings.EqualFold(k, "Accept-Encoding") {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	s.injectCredentials(req, ch, route)

	start := time.Now()
	resp, err := s.httpClient().Do(req)
	if d := time.Since(start); d > 0 {
		s.observeUpstream(d)
	}
	return resp, err
}

// injectCredentials attaches the provider's key using whichever header the
// channel's api_format expects. The downstream key never reaches the client.
func (s *Server) injectCredentials(req *http.Request, ch Channel, route *Route) {
	key := ch.DownstreamAuthKey
	if key == "" {
		key = route.DownstreamAuthKey
	}
	if key == "" {
		return
	}
	format := ch.APIFormat
	if format == "" {
		format = route.APIFormat
	}
	switch format {
	case "anthropic-messages":
		if req.Header.Get("x-api-key") == "" {
			req.Header.Set("x-api-key", key)
		}
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	case "openai-chat", "openai-responses", "openai-embeddings", "openai-completions", "embeddings":
		if req.Header.Get("Authorization") == "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
	default:
		// Generic routes get a namespaced header, leaving Authorization free for
		// the provider key below. A caller wanting to supply their own upstream
		// credential must use X-Provider-Key: Authorization is consumed by
		// mwAuth as the gateway credential and never reaches this point.
		if req.Header.Get("Authorization") == "" && req.Header.Get("X-Provider-Key") == "" {
			req.Header.Set("X-Provider-Key", key)
		}
	}
}

// writeUpstream copies the upstream answer to the client, extracting token
// usage along the way. Streaming responses are flushed chunk by chunk.
func (s *Server) writeUpstream(w http.ResponseWriter, r *http.Request, resp *http.Response, route *Route, ch Channel) {
	m := metaFrom(r)
	// Peek-based gzip detection is only safe on a body read whole; on a
	// stream it would split the first chunk into a two-byte write.
	unwrapGzip(resp, !isStreamed(resp.Header.Get("Content-Type")))
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	// Strip downstream auth headers from response so the user can't see them.
	hdr := w.Header()
	hdr.Del("WWW-Authenticate")
	hdr.Set("X-Upstream-Channel", ch.Label())
	if m != nil {
		m.ChannelID = ch.ID
		m.Upstream = route.Name
	}

	// A streamed (SSE / NDJSON) answer has to be flushed chunk by chunk: reading
	// it to completion first means the user stares at a spinner for the whole
	// generation time and, once the timeout hits, gets a truncated 502 instead of
	// the tokens already produced.
	if isStreamed(resp.Header.Get("Content-Type")) {
		hdr.Del("Content-Length") // unknown up front; let the server use chunked
		w.WriteHeader(resp.StatusCode)
		if f, ok := w.(http.Flusher); ok {
			if m != nil {
				m.IsStream = true
				m.Billed = true
			}
			f.Flush()
			sniff := &usageSniffer{w: &flushWriter{w: w, f: f}}
			if _, cerr := io.Copy(sniff, resp.Body); cerr != nil {
				s.recordFailure(r.Context(), ch.upstreamKey(route.Name))
				log.Printf("stream aborted upstream=%s err=%v", ch.upstreamKey(route.Name), cerr)
			}
			if m != nil {
				m.Model = sniff.model
				m.PromptTokens = sniff.prompt
				m.CompletionTokens = sniff.completion
				m.CacheHitTokens = sniff.cacheHit
				m.CacheWriteTokens = sniff.cacheWrite
				// TTFT: wall-clock from request start to the first generated
				// token. Falls back to the first data frame for streams whose
				// delta shape we do not recognise, and stays zero when the
				// upstream sent nothing at all.
				at := sniff.firstTokenAt
				if at.IsZero() {
					at = sniff.firstFrameAt
				}
				if !at.IsZero() && !m.StartedAt.IsZero() {
					m.TTFTMs = int64(at.Sub(m.StartedAt).Milliseconds())
				}
			}
			return
		}
		// No flusher available (e.g. a buffering middleware wrapper): fall through
		// to the buffered path rather than hanging the request.
	}

	body, rerr := io.ReadAll(resp.Body)
	if m != nil {
		m.Billed = true
		f := usageFromJSON(body)
		// Some providers stream SSE while labelling the response
		// application/json. isStreamed() then classifies the body as
		// buffered, and a whole SSE transcript is not valid JSON — so the
		// model and every token count came out zero and the request fell back
		// to a flat 1-unit charge. Replaying the body through the stream
		// sniffer recovers the real usage.
		if f.model == "" && f.prompt == 0 && f.completion == 0 && looksLikeSSE(body) {
			var sn usageSniffer
			sn.feed(body)
			f.model, f.prompt, f.completion = sn.model, sn.prompt, sn.completion
			f.cacheHit, f.cacheWrite = sn.cacheHit, sn.cacheWrite
			m.IsStream = true
		}
		if f.model != "" {
			m.Model = f.model
		}
		m.PromptTokens = f.prompt
		m.CompletionTokens = f.completion
		m.CacheHitTokens = f.cacheHit
		m.CacheWriteTokens = f.cacheWrite
		// Still nothing: worth a line in the log. It is either a provider
		// answering in a shape this parser does not recognise, or a body the
		// client hung up on mid-transfer — and the request row alone cannot
		// tell those apart, because both leave the model and every token
		// count sitting at zero.
		if f.model == "" && f.prompt == 0 && f.completion == 0 {
			log.Printf("unparsed buffered body upstream=%s status=%d ct=%q bytes=%d readErr=%v head=%.160q",
				route.Name, resp.StatusCode, resp.Header.Get("Content-Type"), len(body), rerr, body)
		}
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// writeModelList answers GET <unified>/models in the OpenAI shape.
//
// Clients probe this endpoint on connect; forwarding it would send them to one
// provider's catalogue and hide every other provider behind the gateway.
func (s *Server) writeModelList(w http.ResponseWriter, r *http.Request) {
	type modelObj struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	out := struct {
		Object string     `json:"object"`
		Data   []modelObj `json:"data"`
	}{Object: "list", Data: []modelObj{}}

	// A key restricted to certain models must not be shown the whole catalogue:
	// clients that pick a model off this list would otherwise pick one they
	// cannot use and get a 403 at call time.
	allowed := s.availableModels()
	if m := metaFrom(r); m != nil && len(m.KeyInfo.AllowedModels) > 0 {
		filtered := allowed[:0:0]
		for _, name := range allowed {
			if store.ModelInList(m.KeyInfo.AllowedModels, name) {
				filtered = append(filtered, name)
			}
		}
		allowed = filtered
	}
	for _, m := range allowed {
		out.Data = append(out.Data, modelObj{ID: m, Object: "model", OwnedBy: "gateway"})
	}
	writeJSON(w, out)
}

// authorizeModel enforces the route's model allowlist plus the key's own model
// permission. Both were previously stored but never compared against anything.
//
// model comes from the caller, which already had to extract it to pick a route;
// modelSeen says whether the body was inspectable at all, so an unreadable body
// is left for the upstream instead of being turned into a 403 here.
func (s *Server) authorizeModel(r *http.Request, route *Route, model string, modelSeen, oversized bool) error {
	ki := store.KeyInfo{}
	if m := metaFrom(r); m != nil {
		ki = m.KeyInfo
	}
	// Only body-bearing methods are inspected. A GET to /v1/models carries no
	// model, and blocking it would break the discovery call every client makes.
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return nil
	}
	// Only LLM-shaped formats carry a model in the body. Peeking a generic
	// route could be a file upload, so leave those completely untouched.
	if !formatCarriesModel(route.APIFormat) && len(ki.AllowedModels) == 0 {
		return nil
	}
	// Nothing usable was recovered from the body. A route or key that declared
	// an allowlist cannot be satisfied by an unknown model, so refuse; without
	// an allowlist there is nothing to check and the upstream can judge the
	// body itself.
	//
	// The prefix scan means this now only triggers for bodies that genuinely
	// could not be read — an oversized request still has its model checked,
	// where before it was turned away on size alone.
	if model == "" && (oversized || !modelSeen) {
		if len(route.Models) > 0 || len(ki.AllowedModels) > 0 {
			return fmt.Errorf("no model found in the inspectable part of the request body (limit %d bytes)", maxInspectBody)
		}
		return nil
	}
	if len(ki.AllowedModels) > 0 && !store.ModelInList(ki.AllowedModels, model) {
		return fmt.Errorf("model %q not allowed for this api key", model)
	}
	if len(route.Models) > 0 && !store.ModelInList(route.Models, model) {
		return fmt.Errorf("model %q not allowed on this route", model)
	}
	return nil
}

// formatCarriesModel reports whether requests on this format put a model id in
// the JSON body, which is what makes allowlist checking meaningful.
func formatCarriesModel(format string) bool {
	switch format {
	case "openai-chat", "openai-completions", "openai-responses",
		"openai-embeddings", "embeddings", "anthropic-messages":
		return true
	}
	return false
}

// injectStreamUsage adds stream_options.include_usage to streaming chat
// requests. Without it OpenAI-compatible providers omit the final usage event
// and every streamed call would bill as a flat 1 unit.
func injectStreamUsage(r *http.Request, route *Route, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	switch route.APIFormat {
	case "openai-chat", "openai-completions", "openai-responses":
	default:
		return body
	}
	var payload map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // keep integer literals exact on the way back out
	if err := dec.Decode(&payload); err != nil {
		return body
	}
	stream, _ := payload["stream"].(bool)
	if !stream {
		return body
	}
	if _, exists := payload["stream_options"]; exists {
		return body // caller already decided
	}
	payload["stream_options"] = map[string]interface{}{"include_usage": true}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	return out
}

// reasoningEffortLevels are the effort levels providers actually implement.
// Clients disagree on casing and invent levels: some send "HIGH", some send
// "auto", and both come back as a 400 that never reaches the model — Grok
// answers "Invalid reasoning effort.", DeepSeek wraps a provider error. The
// caller meant to hint at a thinking budget, not to make the call fail, so map
// what we can and drop what we cannot.
var reasoningEffortLevels = map[string]string{
	"low": "low", "medium": "medium", "high": "high",
	"minimal": "minimal", "xhigh": "xhigh",
}

// normalizeReasoningEffort rewrites reasoning_effort into a value the upstream
// accepts. Returns the body untouched unless something actually needed fixing,
// so a well-formed request costs nothing but a JSON round-trip.
//
// Callers must only pass a complete body: an oversized one is a truncated
// prefix and re-marshalling it would silently drop the rest.
func normalizeReasoningEffort(r *http.Request, route *Route, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	switch route.APIFormat {
	case "openai-chat", "openai-completions", "openai-responses":
	default:
		return body
	}
	var payload map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // keep integer literals exact on the way back out
	if err := dec.Decode(&payload); err != nil {
		return body
	}
	raw, ok := payload["reasoning_effort"]
	if !ok {
		return body
	}
	s, isStr := raw.(string)
	norm, known := "", false
	if isStr {
		norm, known = reasoningEffortLevels[strings.ToLower(strings.TrimSpace(s))]
	}
	switch {
	case known && norm == s:
		return body // already canonical, leave the bytes alone
	case known:
		payload["reasoning_effort"] = norm
	default:
		// Unknown level, or not a string at all: forwarding it is a guaranteed
		// 400, so drop the field and let the model use its default budget.
		delete(payload, "reasoning_effort")
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	return out
}

// peekBody reads a request body into memory and hands back a fresh reader so
// the rest of the pipeline still sees the original bytes.
//
// Bodies larger than `limit` are only buffered up to the cap: the already-read
// prefix is stitched back onto the still-unread stream with a MultiReader, so
// the proxy streams the whole thing through while inspection sees just the
// head. That head still has to be handed back — a large request must not be
// rejected for "missing" a model that sits in the first kilobyte of its body.
func peekBody(r *http.Request, limit int64) (raw []byte, oversized bool, err error) {
	if r.Body == nil {
		return nil, false, nil
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, false, err
	}
	raw = buf.Bytes()
	if n > limit {
		// The byte past the cap only exists to prove the body is oversized;
		// it belongs on the wire, not in the inspection buffer.
		head := raw[:limit]
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(raw), r.Body))
		return head, true, nil
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, false, nil
}

// orderedChannels returns the channels to try, best first:
// healthy channels, lowest priority tier, then weighted-random within the tier.
//
// A route with no channel rows (only reachable for hand-built routes, since
// LoadRoutes always backfills one) falls back to a single implicit channel built
// from the route itself, so it behaves exactly as it did before channels existed.
func (s *Server) orderedChannels(route *Route) []Channel {
	if len(route.Channels) == 0 {
		return []Channel{{
			RouteID:           route.ID,
			Name:              route.Name,
			BaseURL:           route.BaseURL,
			APIFormat:         route.APIFormat,
			DownstreamAuthKey: route.DownstreamAuthKey,
			Weight:            1,
			Enabled:           true,
		}}
	}
	var healthy, tripped []Channel
	for _, c := range route.Channels {
		if !c.Enabled {
			continue
		}
		if s.isCircuitOpen(context.Background(), c.upstreamKey(route.Name)) {
			tripped = append(tripped, c)
			continue
		}
		healthy = append(healthy, c)
	}
	// If every channel is tripped, fall back to the full list: better to try
	// and fail with a real upstream error than to short-circuit to a 503.
	cands := healthy
	if len(cands) == 0 {
		cands = tripped
	}
	if len(cands) == 0 {
		return nil
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].Priority < cands[j].Priority })

	// Shuffle within each priority tier by weight.
	out := make([]Channel, 0, len(cands))
	for i := 0; i < len(cands); {
		j := i
		for j < len(cands) && cands[j].Priority == cands[i].Priority {
			j++
		}
		tier := cands[i:j]
		weightedShuffle(tier)
		out = append(out, tier...)
		i = j
	}
	// Append any tripped channels of a healthier tier at the end: if the
	// healthy subset all fails, the retry loop can still reach them.
	for _, c := range tripped {
		out = append(out, c)
	}
	return out
}

// weightedShuffle orders candidates so that P(front) is proportional to weight.
// Uses the Efraimidis-Spirakis key k = u^(1/w): sorting by descending key
// yields a weighted permutation in O(n log n) without replacement logic.
func weightedShuffle(cs []Channel) {
	type item struct {
		c Channel
		k float64
	}
	items := make([]item, len(cs))
	for i, c := range cs {
		w := c.Weight
		if w <= 0 {
			w = 1
		}
		u := rand.Float64()
		if u <= 0 {
			u = math.SmallestNonzeroFloat64
		}
		items[i] = item{c: c, k: math.Pow(u, 1.0/float64(w))}
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].k > items[j].k })
	for i := range cs {
		cs[i] = items[i].c
	}
}

// ----- usage metering -----

// usageSniffer tees a streamed response to the client while scanning SSE frames
// for usage objects. The final usage event is the only place a stream reports
// token counts, so without this every stream would bill as a flat unit.
type usageSniffer struct {
	w           io.Writer
	rem         []byte
	model       string
	prompt      int64
	completion  int64
	cacheHit    int64
	cacheWrite  int64
	sniffedSize int
	// firstTokenAt is stamped on the first frame that carries generated text.
	// That is what "time to first token" is supposed to mean, and it is NOT
	// the same as the first byte off the wire: providers routinely open a
	// stream with a ": ping" keepalive or an empty role delta long before the
	// model has produced anything, and stamping on those reports the
	// provider's liveness check as if it were an answer.
	firstTokenAt time.Time
	// firstFrameAt is stamped on the first non-keepalive data frame. It is the
	// fallback for a provider whose delta shape we do not recognise: without
	// it an unknown format would report TTFT = 0, which reads like "instant"
	// and is far more misleading than a slightly early number.
	firstFrameAt time.Time
}

const maxSniffRemainder = 256 << 10

func (u *usageSniffer) Write(p []byte) (int, error) {
	n, err := u.w.Write(p)
	if n > 0 {
		u.feed(p[:n])
	}
	return n, err
}

func (u *usageSniffer) feed(p []byte) {
	u.rem = append(u.rem, p...)
	if len(u.rem) > maxSniffRemainder {
		// Usage frames are small and arrive at the end; keep only the tail so
		// a long stream cannot grow this buffer without bound.
		u.rem = u.rem[len(u.rem)-maxSniffRemainder:]
	}
	for {
		i := bytes.IndexByte(u.rem, '\n')
		if i < 0 {
			return
		}
		line := u.rem[:i]
		u.rem = u.rem[i+1:]
		u.consume(line)
	}
}

func (u *usageSniffer) consume(line []byte) {
	line = bytes.TrimSpace(line)
	// isStreamed() lets two wire formats through, so both have to be readable:
	//
	//	SSE     data: {"choices":[{"delta":{"content":"Hel"}}]}
	//	NDJSON        {"choices":[{"delta":{"content":"Hel"}}]}
	//
	// Recognising a bare JSON line is safe because SSE framing always prefixes
	// a field name — no SSE line ever begins with '{' or '[' — so the two
	// shapes cannot be confused. Before this, NDJSON frames were dropped
	// outright, which silently billed streamed requests as zero tokens.
	var payload []byte
	switch {
	case bytes.HasPrefix(line, []byte("data:")):
		payload = bytes.TrimSpace(line[len("data:"):])
	case len(line) > 0 && (line[0] == '{' || line[0] == '['):
		payload = line
	default:
		// SSE comments (": keepalive"), "event:" lines and blank separators.
		return
	}
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		// Both are stream bookkeeping, never content. A bare "data:" line is
		// how some providers send a keepalive.
		return
	}
	if u.firstFrameAt.IsZero() {
		u.firstFrameAt = time.Now()
	}
	if u.firstTokenAt.IsZero() && carriesGeneratedText(payload) {
		u.firstTokenAt = time.Now()
	}
	// Cheap gate: only frames carrying usage are worth parsing. OpenAI's final
	// usage frame also carries "model", so one parse picks up both.
	if !bytes.Contains(payload, []byte("usage")) {
		return
	}
	f := usageFromJSON(payload)
	if f.model != "" {
		u.model = f.model
	}
	if f.prompt > u.prompt {
		u.prompt = f.prompt
	}
	if f.completion > u.completion {
		u.completion = f.completion
	}
	if f.cacheHit > u.cacheHit {
		u.cacheHit = f.cacheHit
	}
	if f.cacheWrite > u.cacheWrite {
		u.cacheWrite = f.cacheWrite
	}
	u.sniffedSize++
}

// carriesGeneratedText reports whether a stream frame is carrying output the
// user is waiting for, as opposed to the provider announcing that a stream is
// open.
//
// The shapes it has to separate look like this:
//
//	OpenAI  {"choices":[{"delta":{"role":"assistant","content":""}}]}   <- not yet
//	OpenAI  {"choices":[{"delta":{"content":"Hel"}}]}                    <- token
//	Anthropic {"type":"message_start","message":{"usage":{...}}}          <- not yet
//	Anthropic {"type":"content_block_delta","delta":{"text":"Hel"}}       <- token
//	Responses {"type":"response.output_text.delta","delta":"Hel"}         <- token
//
// Rather than enumerate event names per provider — which silently rots every
// time one adds an event — it looks for a non-empty string in the slots where
// providers put generated text. A frame without one is treated as bookkeeping,
// and the caller falls back to the first data frame if a stream never
// produces any.
func carriesGeneratedText(payload []byte) bool {
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		// Not JSON at all (plain-text chunked streams exist). Anything that
		// is not empty is content.
		return true
	}
	return hasTextDelta(v)
}

var textDeltaKeys = map[string]bool{"content": true, "text": true, "delta": true}

func hasTextDelta(v interface{}) bool {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			if textDeltaKeys[k] {
				// "delta" is a string in the Responses API but an object in
				// chat completions; only a non-empty string is a token.
				if s, ok := val.(string); ok && s != "" {
					return true
				}
			}
		}
		for _, val := range t {
			if hasTextDelta(val) {
				return true
			}
		}
	case []interface{}:
		for _, val := range t {
			if hasTextDelta(val) {
				return true
			}
		}
	}
	return false
}

// usageFigures is what the gateway lifts out of a provider's response body.
//
// The two cache columns are the reason this struct exists at all. Every major
// provider bills input tokens that repeat a prefix it has already seen at a
// fraction of the normal rate, and for a chat workload those tokens are the
// large majority of the bill. Recording only the prompt total makes a cached
// 40k-token request and an uncached one look identical.
type usageFigures struct {
	model      string
	prompt     int64
	completion int64
	// cacheHit is input tokens the provider served from its own prefix cache
	// instead of recomputing them.
	cacheHit int64
	// cacheWrite is input tokens the provider charged a premium to store.
	// Anthropic is the only one that reports it and it bills writes at ~1.25x,
	// so it is not free and belongs in the ledger next to the hits.
	cacheWrite int64
	// anthroInput is the largest total Anthropic input seen in one usage
	// object: input_tokens + cache_read + cache_creation. Kept in its own field
	// rather than folded into prompt during the walk because a stream carries
	// several usage blocks with the same figures, and adding on each visit
	// would count them once per block.
	anthroInput int64
}

// resolve folds Anthropic's three-way split into the prompt total.
//
// Anthropic counts only the tokens after the last cache breakpoint in
// input_tokens and reports the cached bulk separately, so its three figures
// have to be added to know how much input was actually processed. Reading
// input_tokens alone under-reports a cached request by the entire cached
// prefix, which is both a wrong denominator for the hit rate and an
// undercharge on the quota ledger.
//
// DeepSeek and OpenAI fold everything into prompt_tokens, which leaves
// anthroInput at zero and makes this a no-op for them.
func (f *usageFigures) resolve() {
	if f.anthroInput > f.prompt {
		f.prompt = f.anthroInput
	}
}

// usageFromJSON pulls (model, prompt_tokens, completion_tokens, cache tokens)
// out of an arbitrary provider payload. It walks the object instead of reading
// fixed paths because the shapes differ:
//
//	OpenAI    {"model":"gpt-4o","usage":{"prompt_tokens":1,"completion_tokens":2}}
//	Anthropic {"usage":{"input_tokens":1,"output_tokens":2}}
//	Anthropic SSE {"type":"message_start","message":{"model":"...","usage":{"input_tokens":1}}}
func usageFromJSON(raw []byte) usageFigures {
	var f usageFigures
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return f
	}
	walkUsage(v, &f)
	f.resolve()
	return f
}

func walkUsage(v interface{}, f *usageFigures) {
	switch t := v.(type) {
	case map[string]interface{}:
		if u, ok := t["usage"].(map[string]interface{}); ok {
			if p := numOf(u["prompt_tokens"]); p > f.prompt {
				f.prompt = p
			}
			if p := numOf(u["input_tokens"]); p > f.prompt {
				f.prompt = p
			}
			if c := numOf(u["completion_tokens"]); c > f.completion {
				f.completion = c
			}
			if c := numOf(u["output_tokens"]); c > f.completion {
				f.completion = c
			}
			readCache(u, f)
		}
		// OpenAI nests its cache figure one level below usage, and it is the
		// only one that does, so it is picked up here instead of in readCache.
		if d, ok := t["prompt_tokens_details"].(map[string]interface{}); ok {
			if c := numOf(d["cached_tokens"]); c > f.cacheHit {
				f.cacheHit = c
			}
		}
		if f.model == "" {
			if s, ok := t["model"].(string); ok && s != "" {
				f.model = s
			}
		}
		for _, val := range t {
			walkUsage(val, f)
		}
	case []interface{}:
		for _, val := range t {
			walkUsage(val, f)
		}
	}
}

// readCache lifts the cache columns out of a usage object.
//
// Every provider that discounts repeated prefixes reports it, and no two of
// them agree on the name:
//
//	DeepSeek  "prompt_cache_hit_tokens":38000,"prompt_cache_miss_tokens":3427
//	Anthropic "cache_read_input_tokens":38000,"cache_creation_input_tokens":900
//	OpenAI    "prompt_tokens_details":{"cached_tokens":38000}
//
// Figures are read as a maximum rather than assigned because a stream emits
// the usage block more than once and the largest value is the settled one.
//
// DeepSeek's miss counter is deliberately dropped: hit+miss is the prompt
// total, so the miss is derivable, and storing both would invite whoever
// reads the column next to add them together and double-count.
func readCache(u map[string]interface{}, f *usageFigures) {
	read := numOf(u["cache_read_input_tokens"])
	write := numOf(u["cache_creation_input_tokens"])

	if n := numOf(u["prompt_cache_hit_tokens"]); n > f.cacheHit {
		f.cacheHit = n
	}
	if read > f.cacheHit {
		f.cacheHit = read
	}
	if write > f.cacheWrite {
		f.cacheWrite = write
	}

	// Anthropic splits the input across three fields instead of folding the
	// cached part into input_tokens, so the real input size only exists as
	// their sum. The sum is built from this object's own input_tokens rather
	// than the running f.prompt, which keeps it stable when the same figures
	// are walked twice inside one stream.
	if read+write > 0 {
		if total := numOf(u["input_tokens"]) + read + write; total > f.anthroInput {
			f.anthroInput = total
		}
	}
}

func numOf(v interface{}) int64 {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		f, _ := n.Float64()
		return int64(f)
	case float64:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

// ----- helpers -----

// clientIP resolves the caller address. Forwarded headers are only trusted when
// TRUST_PROXY is on: otherwise a client could spoof X-Forwarded-For and walk
// straight through an IP allowlist.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i > 0 {
				return strings.TrimSpace(xff[:i])
			}
			return strings.TrimSpace(xff)
		}
		if xr := r.Header.Get("X-Real-IP"); xr != "" {
			return strings.TrimSpace(xr)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// httpClient returns a pooled client. Reusing one client (instead of building a
// fresh one per request) is what makes keep-alive work; without it every call
// pays a new TCP+TLS handshake against the provider.
func (s *Server) httpClient() *http.Client {
	s.clientOnce.Do(func() {
		s.client = &http.Client{
			Timeout: s.upstreamTimeout(),
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   32,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: time.Second,
				ResponseHeaderTimeout: s.upstreamTimeout(),
			},
		}
	})
	return s.client
}

// upstreamTimeout bounds one upstream call, including draining a streamed body.
func (s *Server) upstreamTimeout() time.Duration {
	if s.Cfg == nil || s.Cfg.UpstreamTimeoutSec <= 0 {
		return 180 * time.Second
	}
	return time.Duration(s.Cfg.UpstreamTimeoutSec) * time.Second
}

func (s *Server) maxAttempts() int {
	if s.Cfg == nil || s.Cfg.MaxAttempts <= 0 {
		return 3
	}
	return s.Cfg.MaxAttempts
}

// isStreamed reports whether a content type delivers incremental chunks the
// client is meant to consume as they arrive (SSE, NDJSON).
func isStreamed(contentType string) bool {
	ct := strings.ToLower(contentType)
	return strings.Contains(ct, "text/event-stream") || strings.Contains(ct, "application/x-ndjson")
}

// gzipMagic is how every gzip stream starts.
var gzipMagic = []byte{0x1f, 0x8b}

// unwrapGzip transparently decompresses a gzipped upstream body.
//
// Belt and braces alongside dropping the relayed Accept-Encoding: an upstream
// that compresses without advertising it, or one compressed before the header
// drop was deployed, still lands here as binary. Reading gzip bytes as JSON
// yields nothing — empty model, zero tokens, a quota charge silently falling
// back to one unit.
//
// allowPeek must be false for a response that will be streamed through: the
// magic-byte probe reads two bytes, and on a stream that would split the
// first chunk into a two-byte write. A declared Content-Encoding is still
// honoured either way, because it needs no probing.
func unwrapGzip(resp *http.Response, allowPeek bool) {
	if resp == nil || resp.Body == nil {
		return
	}
	orig := resp.Body
	replace := func(r io.Reader) {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return
		}
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.Body = struct {
			io.Reader
			io.Closer
		}{zr, orig}
	}
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		replace(orig)
		return
	}
	if !allowPeek {
		return
	}
	// Not advertised: peek at the magic bytes. Some upstreams compress anyway.
	// Only the bytes actually read go back in front of the stream — ReadFull
	// leaves the tail of the buffer at its zero value, and writing those
	// would append a NUL to every short response.
	head := make([]byte, 2)
	n, err := io.ReadFull(orig, head)
	if n == 0 && err != nil {
		return // nothing was read: the body is empty, leave it alone
	}
	if !bytes.Equal(head[:n], gzipMagic) {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(head[:n]), orig), orig}
		return
	}
	// gzip.NewReader validates the header itself, so the peeked bytes go back
	// in front of the rest of the stream.
	replace(io.MultiReader(bytes.NewReader(head[:n]), orig))
}

// looksLikeSSE reports whether a buffered body is really an SSE transcript.
//
// That is what a provider produces when it streams but sets a Content-Type
// isStreamed() does not recognise. The body is then read whole and handed to
// the JSON parser, which finds a long non-JSON blob and returns nothing.
func looksLikeSSE(body []byte) bool {
	return bytes.HasPrefix(bytes.TrimSpace(body), []byte("data:")) ||
		bytes.Contains(body, []byte("\ndata: ")) ||
		bytes.Contains(body, []byte("\r\ndata: "))
}

// flushWriter flushes after each write so chunks reach the client immediately
// instead of sitting in the server's buffer until the response ends.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}

func (s *Server) matchRoute(path string) *Route {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Longest prefix wins. Taking the first match instead means a shorter prefix
	// loaded earlier silently swallows a longer one: with both "/v1" and
	// "/v1/messages" configured, requests for the latter would always land on
	// the former, and the more specific route becomes unreachable.
	var best *Route
	bestLen := -1
	for i := range s.Routes {
		p := s.Routes[i].MatchPrefix
		if p == "" || !strings.HasPrefix(path, p) {
			continue
		}
		if len(p) > bestLen {
			best = &s.Routes[i]
			bestLen = len(p)
		}
	}
	return best
}

// routeByID looks a route up by primary key (used by the playground, which
// addresses routes directly instead of by path).
func (s *Server) routeByID(id int64) *Route {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.Routes {
		if s.Routes[i].ID == id {
			return &s.Routes[i]
		}
	}
	return nil
}

// Circuit state lives in Redis; with no client attached the breaker is simply
// disabled rather than panicking partway through proxying a request.
func (s *Server) isCircuitOpen(ctx context.Context, upstream string) bool {
	if s.RDB == nil {
		return false
	}
	v, err := s.RDB.Get(ctx, "cb:"+upstream).Result()
	if err != nil {
		return false
	}
	return v == "open"
}

func (s *Server) recordFailure(ctx context.Context, upstream string) {
	if s.RDB == nil {
		return
	}
	key := "cb:" + upstream + ":fail"
	n, _ := s.RDB.Incr(ctx, key).Result()
	s.RDB.Expire(ctx, key, 30*time.Second)
	if n >= 5 {
		s.RDB.Set(ctx, "cb:"+upstream, "open", 10*time.Second)
	}
}

func (s *Server) recordSuccess(ctx context.Context, upstream string) {
	if s.RDB == nil {
		return
	}
	s.RDB.Del(ctx, "cb:"+upstream+":fail")
	s.RDB.Del(ctx, "cb:"+upstream)
}

// circuitKeys enumerates every circuit-breaker bucket currently in use, one per
// channel of every route.
func (s *Server) circuitKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.Routes))
	for _, rt := range s.Routes {
		if len(rt.Channels) == 0 {
			out = append(out, rt.Name)
			continue
		}
		for _, c := range rt.Channels {
			out = append(out, c.upstreamKey(rt.Name))
		}
	}
	return out
}

// adoptChannelFormat fills a route's empty api_format from its first channel.
//
// A route inserted straight into SQL — or created before api_format was written
// — can leave the route's own format empty while its channels name the real
// one. Forwarding already prefers the channel's, so the two disagreed:
// injectStreamUsage and parameter normalisation both read route.APIFormat and
// were silently skipped, which showed up as a Grok route that never got
// stream_options injected. Adopt the channel's so every reader sees the format
// the request is actually sent as.
func adoptChannelFormat(route *Route) {
	if route.APIFormat == "" && len(route.Channels) > 0 {
		route.APIFormat = route.Channels[0].APIFormat
	}
}

// LoadRoutes reads active routes (and their channels) from DB.
func LoadRoutes(db *sql.DB) ([]Route, error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT id, name, base_url, match_path, upstream_rps, cache_ttl, cb_enabled,
		       api_format, COALESCE(downstream_auth_key,''), COALESCE(models,'[]'),
		       COALESCE(cache_scope,'global')
		FROM routes WHERE status = 1 ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var routes []Route
	for rows.Next() {
		var r Route
		var match string
		var modelsJSON string
		if err := rows.Scan(&r.ID, &r.Name, &r.BaseURL, &match, &r.UpstreamRPS, &r.CacheTTL, &r.CBEnabled,
			&r.APIFormat, &r.DownstreamAuthKey, &modelsJSON, &r.CacheScope); err != nil {
			return nil, err
		}
		r.MatchPrefix = strings.TrimSuffix(match, "/*")
		r.Upstream = r.Name
		_ = json.Unmarshal([]byte(modelsJSON), &r.Models)
		routes = append(routes, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	chans, err := LoadChannels(db)
	if err != nil {
		// Channels are an enhancement, not a requirement: if the table has not
		// been migrated yet, fall back to one implicit channel per route built
		// from the route's own base_url/key/format. Behaviour is unchanged.
		log.Printf("load channels err (falling back to route default): %v", err)
		for i := range routes {
			routes[i].Channels = []Channel{{
				RouteID:           routes[i].ID,
				Name:              routes[i].Name,
				BaseURL:           routes[i].BaseURL,
				APIFormat:         routes[i].APIFormat,
				DownstreamAuthKey: routes[i].DownstreamAuthKey,
				Weight:            1,
				Enabled:           true,
			}}
		}
		return routes, nil
	}
	byRoute := map[int64][]Channel{}
	for _, c := range chans {
		byRoute[c.RouteID] = append(byRoute[c.RouteID], c)
	}
	for i := range routes {
		if cs, ok := byRoute[routes[i].ID]; ok && len(cs) > 0 {
			routes[i].Channels = cs
		} else {
			// Route predates the channels table: synthesise its implicit
			// channel so every code path can assume len(Channels) >= 1.
			routes[i].Channels = []Channel{{
				RouteID:           routes[i].ID,
				Name:              routes[i].Name,
				BaseURL:           routes[i].BaseURL,
				APIFormat:         routes[i].APIFormat,
				DownstreamAuthKey: routes[i].DownstreamAuthKey,
				Weight:            1,
				Enabled:           true,
			}}
		}
		adoptChannelFormat(&routes[i])
	}
	return routes, nil
}

// LoadChannels reads every channel, ordered for deterministic selection ties.
func LoadChannels(db *sql.DB) ([]Channel, error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT id, route_id, COALESCE(name,''), base_url, api_format,
		       COALESCE(downstream_auth_key,''), weight, priority, enabled
		FROM channels ORDER BY route_id, priority, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.RouteID, &c.Name, &c.BaseURL, &c.APIFormat,
			&c.DownstreamAuthKey, &c.Weight, &c.Priority, &c.Enabled); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func hashOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// statusWriter captures the response status and forwards once.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if s.wroteHeader {
		return
	}
	s.status = code
	s.wroteHeader = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(b)
}

// Flush passes the call down so streaming handlers can push chunks out.
// Without it the embedded http.ResponseWriter interface does not expose Flush,
// w.(http.Flusher) fails, and every streamed response silently degrades into a
// fully buffered one.
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// captureWriter buffers the body so it can be cached after the handler returns.
type captureWriter struct {
	http.ResponseWriter
	buf    *bytes.Buffer
	status int
}

func (c *captureWriter) WriteHeader(code int) { c.status = code }

func (c *captureWriter) Write(b []byte) (int, error) { return c.buf.Write(b) }

// Flush is a no-op: the whole point of this wrapper is to hold bytes back until
// the handler returns. Forwarding Flush to the real writer would leak a partial
// response before the cache entry was computed.
func (c *captureWriter) Flush() {}
