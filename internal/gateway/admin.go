package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"github.com/zishuo-xu/api-gateway/internal/store"
)

// ----- admin console -----

// sessionCookie holds the browser's admin session id. HttpOnly keeps it away
// from any injected script; SameSite=Strict means a cross-site page can never
// ride it. Secure is deliberately not set because the gateway commonly runs
// behind a TLS-terminating proxy with no TLS of its own — flip it on once the
// deployment serves HTTPS directly.
const (
	sessionCookie = "admin_session"
	sessionTTL    = 7 * 24 * time.Hour
	// loginMaxFails is failures from one IP inside a lockout cycle; exceeding
	// it buys a loginLockout timeout. Pointless against a token that should be
	// ~32 random characters anyway — this exists to slow careless guessing.
	loginMaxFails = 10
	loginLockout  = 10 * time.Minute
)

// secureEqual compares two secrets in constant time. Hashing both sides first
// means the comparison never leaks the token's length either.
func secureEqual(a, b string) bool {
	hb := sha256.Sum256([]byte(a))
	hc := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(hb[:], hc[:]) == 1
}

// loginGate rate-limits admin login failures per client IP. In memory and
// therefore reset by a restart: it exists to make brute force slow, not to be
// an audit trail (the audit log already records every 403).
type loginGate struct {
	mu    sync.Mutex
	fails map[string]*loginFails
}

type loginFails struct {
	n     int
	until time.Time
}

// key returns the per-IP record, creating it on first use. The caller must
// already hold g.mu — it is called from inside the locked methods, and
// sync.Mutex is not reentrant.
func (g *loginGate) key(ip string) *loginFails {
	if g.fails == nil {
		g.fails = map[string]*loginFails{}
	}
	e, ok := g.fails[ip]
	if !ok {
		e = &loginFails{}
		g.fails[ip] = e
	}
	return e
}

func (g *loginGate) allow(ip string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.fails[ip]
	if !ok {
		return true
	}
	if e.until.IsZero() {
		return true
	}
	return time.Now().After(e.until)
}

func (g *loginGate) fail(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.key(ip)
	// A run of failures only counts together while the lockout that ended
	// them is still recent; long after it expired, start a fresh count.
	if !e.until.IsZero() && time.Now().After(e.until.Add(loginLockout)) {
		e.n = 0
		e.until = time.Time{}
	}
	e.n++
	if e.n >= loginMaxFails {
		e.until = time.Now().Add(loginLockout)
	}
}

func (g *loginGate) reset(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.fails, ip)
}

func (s *Server) gate() *loginGate {
	if s.logins == nil {
		s.logins = &loginGate{}
	}
	return s.logins
}

// mwAdminAuth protects the /admin tree with a separate token so the dashboard
// is never reachable with an ordinary API key.
//
// Two ways in, both constant-time:
//   - the X-Admin-Token / Authorization: Bearer header, for scripts and curl;
//   - an admin_session cookie minted by /admin/login, for the browser — so the
//     token itself never has to live in page HTML. It used to be baked into
//     the dashboard markup, which handed admin to anyone who could fetch the
//     page.
//
// If ADMIN_TOKEN is unset the console is hard-disabled to avoid accidental
// exposure.
func (s *Server) mwAdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Cfg.AdminToken == "" {
			http.Error(w, "admin console disabled: set ADMIN_TOKEN", http.StatusForbidden)
			return
		}
		tok := r.Header.Get("X-Admin-Token")
		if tok == "" {
			if ah := r.Header.Get("Authorization"); strings.HasPrefix(ah, "Bearer ") {
				tok = strings.TrimPrefix(ah, "Bearer ")
			}
		}
		if tok != "" {
			if secureEqual(tok, s.Cfg.AdminToken) {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" &&
			s.sessionValid(r.Context(), c.Value) {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "forbidden", http.StatusForbidden)
	})
}

// adminLogin exchanges the ADMIN_TOKEN for an HttpOnly session cookie. The
// token itself never touches page HTML again, and the session can be revoked
// server-side (delete the Redis key) without changing the token.
func (s *Server) adminLogin(w http.ResponseWriter, r *http.Request) {
	if s.Cfg.AdminToken == "" {
		http.Error(w, "admin console disabled: set ADMIN_TOKEN", http.StatusForbidden)
		return
	}
	ip := clientIP(r, s.Cfg != nil && s.Cfg.TrustProxy)
	if !s.gate().allow(ip) {
		http.Error(w, "too many failed logins; wait ten minutes", http.StatusTooManyRequests)
		return
	}
	var in struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !secureEqual(in.Token, s.Cfg.AdminToken) {
		s.gate().fail(ip)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	s.gate().reset(ip)
	if s.RDB == nil {
		// No Redis means nowhere to keep sessions. Fail loudly rather than
		// silently downgrading to something the browser cannot use.
		http.Error(w, "session storage unavailable (no redis); use the header", http.StatusInternalServerError)
		return
	}
	sid, err := startSession(r.Context(), s.RDB)
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    sid,
		Path:     "/admin",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
	writeJSON(w, map[string]interface{}{"ok": true})
}

// adminLogout revokes the caller's session. Unauthenticated on purpose: all
// it can ever do is delete a session the caller already holds.
func (s *Server) adminLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && s.RDB != nil {
		s.RDB.Del(r.Context(), "adminsess:"+c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/admin", MaxAge: -1})
	writeJSON(w, map[string]interface{}{"ok": true})
}

func startSession(ctx context.Context, rdb *redis.Client) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	if err := rdb.Set(ctx, "adminsess:"+id, "1", sessionTTL).Err(); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Server) sessionValid(ctx context.Context, id string) bool {
	if s.RDB == nil {
		return false
	}
	n, err := s.RDB.Exists(ctx, "adminsess:"+id).Result()
	return err == nil && n == 1
}

// AdminUI is the embedded dashboard HTML (see embed.go).
var AdminUI = mustEmbedAdminUI()

// adminUI serves the monitoring dashboard HTML. The page ships markup only:
// the ADMIN_TOKEN is NOT baked into it any more. The browser logs in through
// /admin/login and holds an HttpOnly session cookie, so handing the page to a
// stranger hands them nothing. If ADMIN_TOKEN is unset the console is
// hard-disabled (consistent with the doc: no token -> 403).
func (s *Server) adminUI(w http.ResponseWriter, r *http.Request) {
	if s.Cfg.AdminToken == "" {
		http.Error(w, "admin console disabled: set ADMIN_TOKEN", http.StatusForbidden)
		return
	}
	page := bytes.ReplaceAll(AdminUI, []byte("__UNIFIED_PREFIX__"), []byte(s.unifiedPrefix()))
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	// Hardening headers for a page that will sit on a public host.
	h.Set("X-Frame-Options", "DENY")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	w.WriteHeader(http.StatusOK)
	w.Write(page)
}

// MetricsSnapshot is the payload returned by /admin/metrics.
type MetricsSnapshot struct {
	Now          int64             `json:"now"`
	UptimeSec    int64             `json:"uptime_sec"`
	Total        int64             `json:"total"`
	Cached       int64             `json:"cached"`
	Rejected     int64             `json:"rejected"`
	Errors       int64             `json:"errors"`
	CacheHitRate float64           `json:"cache_hit_rate"`
	RejectRate   float64           `json:"reject_rate"`
	ErrorRate    float64           `json:"error_rate"`
	CurrentQPS   float64           `json:"current_qps"`
	AvgQPS       float64           `json:"avg_qps"`
	Series       []store.SecPoint  `json:"series"`
	CachedSeries []store.SecPoint  `json:"cached_series"`
	RejectSeries []store.SecPoint  `json:"reject_series"`
	Circuits     map[string]string `json:"circuits"`
}

func (s *Server) adminMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	total := store.GetCounter(ctx, s.RDB, "stats:total")
	cached := store.GetCounter(ctx, s.RDB, "stats:cached")
	rejected := store.GetCounter(ctx, s.RDB, "stats:rejected")
	errors := store.GetCounter(ctx, s.RDB, "stats:errors")

	series := store.GetSeries(ctx, s.RDB, "stats:sec:", store.StatsWindow)
	cachedSeries := store.GetSeries(ctx, s.RDB, "stats:cached:sec:", store.StatsWindow)
	rejectSeries := store.GetSeries(ctx, s.RDB, "stats:rej:sec:", store.StatsWindow)

	var cur, sum int64
	if n := len(series); n > 0 {
		cur = series[n-1].Count
		for _, p := range series {
			sum += p.Count
		}
	}

	snap := MetricsSnapshot{
		Now:          time.Now().Unix(),
		UptimeSec:    int64(time.Since(s.Start).Seconds()),
		Total:        total,
		Cached:       cached,
		Rejected:     rejected,
		Errors:       errors,
		CacheHitRate: pct(cached, total),
		RejectRate:   pct(rejected, total),
		ErrorRate:    pct(errors, total),
		CurrentQPS:   float64(cur),
		AvgQPS:       float64(sum) / float64(len(series)),
		Series:       series,
		CachedSeries: cachedSeries,
		RejectSeries: rejectSeries,
		Circuits:     s.circuitStatus(ctx),
	}
	writeJSON(w, snap)
}

// adminPrometheus exposes the Prometheus text format under /metrics.
func (s *Server) adminPrometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, s.prometheusMetrics(r.Context()))
}

// circuitStatus reports open/closed state of the breaker per channel.
func (s *Server) circuitStatus(ctx context.Context) map[string]string {
	m := map[string]string{}
	for _, k := range s.circuitKeys() {
		m[k] = s.circuitState(ctx, k)
	}
	return m
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON err: %v", err)
	}
}

// ----- API keys -----

// adminKeys dispatches GET(list) / POST(issue) / PUT(update) / DELETE(revoke).
func (s *Server) adminKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listKeys(w, r)
	case http.MethodPost:
		s.createKey(w, r)
	case http.MethodPut:
		s.updateKey(w, r)
	case http.MethodDelete:
		s.deleteKey(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// createKey issues a new user-facing API key. Only its sha256 hash is stored, so
// the raw key is returned exactly once and the console must display it now.
func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Owner         string   `json:"owner"`
		Name          string   `json:"name"`           // human label, e.g. "我的笔记本"; "" = show owner only
		QuotaLimit    *int64   `json:"quota_limit"`    // nil = default 100000; <=0 = unlimited
		ExpiresAt     string   `json:"expires_at"`     // RFC3339, "" = never
		AllowedIPs    []string `json:"allowed_ips"`    // IPs or CIDRs
		AllowedModels []string `json:"allowed_models"` // empty = any
		NoLog         bool     `json:"no_log"`         // smoke-test keys: skip request log/audit/metrics
	}
	// An empty body is fine (defaults apply), but a malformed one is rejected:
	// these fields are restrictions, so ignoring a parse error would issue a
	// key that is more permissive than requested, silently.
	if err := decodeOptional(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var expires time.Time
	if in.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, in.ExpiresAt)
		if err != nil {
			http.Error(w, "invalid expires_at, want RFC3339", http.StatusBadRequest)
			return
		}
		if t.Before(time.Now()) {
			http.Error(w, "expires_at is in the past", http.StatusBadRequest)
			return
		}
		expires = t
	}
	if err := validateIPList(in.AllowedIPs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rawKey, id, err := store.CreateAPIKey(r.Context(), s.DB, s.RDB, store.KeySpec{
		Owner:         in.Owner,
		Name:          in.Name,
		QuotaLimit:    in.QuotaLimit,
		ExpiresAt:     expires,
		AllowedIPs:    in.AllowedIPs,
		AllowedModels: in.AllowedModels,
		NoLog:         in.NoLog,
	})
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// The issued ceiling goes in the log line because "unlimited" is a
	// deliberate choice someone made, and it should be auditable rather than
	// something you have to infer later from a row that says 0.
	quotaDesc := "default"
	if in.QuotaLimit != nil {
		if *in.QuotaLimit <= 0 {
			quotaDesc = "unlimited"
		} else {
			quotaDesc = strconv.FormatInt(*in.QuotaLimit, 10)
		}
	}
	log.Printf("issued api key id=%d owner=%s name=%q quota=%s expires=%v ips=%v models=%v nolog=%v",
		id, in.Owner, in.Name, quotaDesc, expires, in.AllowedIPs, in.AllowedModels, in.NoLog)
	writeJSON(w, map[string]interface{}{"id": id, "ok": true, "api_key": rawKey})
}

// updateKey applies a partial patch. Status is a pointer so an empty body does
// not silently disable the key (the old code treated "missing" as 0 = disabled).
func (s *Server) updateKey(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var in struct {
		Status        *int     `json:"status"`
		QuotaLimit    *int64   `json:"quota_limit"`
		ExpiresAt     *string  `json:"expires_at"` // "" clears the expiry
		AllowedIPs    []string `json:"allowed_ips"`
		AllowedModels []string `json:"allowed_models"`
		NoLog         *bool    `json:"no_log"`
	}
	if err := decodeOptional(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if in.Status != nil {
		if err := store.SetKeyStatus(r.Context(), s.DB, s.RDB, id, *in.Status == 1); err != nil {
			http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("api key %d status -> %d", id, *in.Status)
	}

	patch := store.KeyPatch{QuotaLimit: in.QuotaLimit}
	if in.ExpiresAt != nil {
		if *in.ExpiresAt == "" {
			zero := time.Time{}
			patch.ExpiresAt = &zero
		} else {
			t, perr := time.Parse(time.RFC3339, *in.ExpiresAt)
			if perr != nil {
				http.Error(w, "invalid expires_at, want RFC3339", http.StatusBadRequest)
				return
			}
			patch.ExpiresAt = &t
		}
	}
	if in.AllowedIPs != nil {
		if err := validateIPList(in.AllowedIPs); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		patch.AllowedIPs = &in.AllowedIPs
	}
	if in.AllowedModels != nil {
		patch.AllowedModels = &in.AllowedModels
	}
	// Pointer so a missing field leaves the flag alone while an explicit
	// false turns it back off - same partial-update rule as the other fields.
	patch.NoLog = in.NoLog

	if patch.QuotaLimit != nil || patch.ExpiresAt != nil || patch.AllowedIPs != nil ||
		patch.AllowedModels != nil || patch.NoLog != nil {
		if err := store.UpdateAPIKey(r.Context(), s.DB, s.RDB, id, patch); err != nil {
			http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("api key %d policy updated", id)
	}
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

// deleteKey permanently removes a key. Revoking (status 0) keeps the audit
// trail; deleting is for keys that were never meant to exist.
func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := store.DeleteAPIKey(r.Context(), s.DB, s.RDB, id); err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("api key %d deleted", id)
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

// decodeOptional reads an optional JSON body into v.
//
// An empty body is fine — defaults apply. A malformed one is rejected, because
// these payloads carry *restrictions* (allowed IPs, model allowlists, expiry):
// ignoring a parse error would apply fewer restrictions than the caller asked
// for, and they would never find out.
func decodeOptional(r *http.Request, v interface{}) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		return fmt.Errorf("unreadable request body")
	}
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("bad json: %v", err)
	}
	return nil
}

// validateIPList rejects malformed allowlist entries up front: a typo that
// silently matched nothing would lock a key out with no explanation.
func validateIPList(list []string) error {
	for _, raw := range list {
		rule := strings.TrimSpace(raw)
		if rule == "" {
			continue
		}
		if strings.Contains(rule, "/") {
			if _, _, err := net.ParseCIDR(rule); err != nil {
				return fmt.Errorf("invalid CIDR %q", raw)
			}
			continue
		}
		if net.ParseIP(rule) == nil {
			return fmt.Errorf("invalid IP %q", raw)
		}
	}
	return nil
}

// keyRow is one API key as returned to the console.
type keyRow struct {
	ID            int64    `json:"id"`
	Owner         string   `json:"owner"`
	Name          string   `json:"name"`
	Status        int      `json:"status"`
	QuotaLimit    int64    `json:"quota_limit"`
	QuotaUsed     int64    `json:"quota_used"`
	CreatedAt     string   `json:"created_at"`
	ExpiresAt     string   `json:"expires_at"`
	Expired       bool     `json:"expired"`
	AllowedIPs    []string `json:"allowed_ips"`
	AllowedModels []string `json:"allowed_models"`
	NoLog         bool     `json:"no_log"`
}

// listKeys lists API keys (hashes are not exposed; only metadata).
// quota_used is read from Redis because that is where the live counter lives;
// Postgres is only updated on the flush tick.
func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(), `
		SELECT id, owner, COALESCE(name,''), status, quota_limit, created_at, expires_at,
		       COALESCE(allowed_ips,''), COALESCE(allowed_models,'[]'), COALESCE(no_log,false)
		FROM api_keys ORDER BY id
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []keyRow{}
	for rows.Next() {
		var (
			k          keyRow
			created    time.Time
			expires    sql.NullTime
			ips        string
			modelsJSON string
		)
		if err := rows.Scan(&k.ID, &k.Owner, &k.Name, &k.Status, &k.QuotaLimit, &created, &expires,
			&ips, &modelsJSON, &k.NoLog); err != nil {
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}
		k.CreatedAt = created.Format(time.RFC3339)
		if expires.Valid {
			k.ExpiresAt = expires.Time.Format(time.RFC3339)
			k.Expired = expires.Time.Before(time.Now())
		}
		for _, p := range strings.Split(ips, ",") {
			if t := strings.TrimSpace(p); t != "" {
				k.AllowedIPs = append(k.AllowedIPs, t)
			}
		}
		_ = json.Unmarshal([]byte(modelsJSON), &k.AllowedModels)
		k.QuotaUsed = store.QuotaUsed(r.Context(), s.RDB, s.DB, k.ID)
		out = append(out, k)
	}
	writeJSON(w, out)
}

// ----- routes -----

// adminRoutes dispatches GET(list) / POST(create) / PUT(update) / DELETE(soft-delete)
// so the console can manage downstream third-party APIs at runtime.
func (s *Server) adminRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listRoutes(w, r)
	case http.MethodPost:
		s.createRoute(w, r)
	case http.MethodPut:
		s.updateRoute(w, r)
	case http.MethodDelete:
		s.deleteRoute(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type routeRow struct {
	ID               int64        `json:"id"`
	Name             string       `json:"name"`
	BaseURL          string       `json:"base_url"`
	MatchPrefix      string       `json:"match_prefix"`
	UpstreamRPS      int          `json:"upstream_rps"`
	CacheTTL         int          `json:"cache_ttl"`
	CBEnabled        bool         `json:"cb_enabled"`
	APIFormat        string       `json:"api_format"`
	HasDownstreamKey bool         `json:"has_downstream_key"` // true if key is set (never expose the actual key in list)
	Models           []string     `json:"models"`
	CacheScope       string       `json:"cache_scope"`
	Channels         []channelRow `json:"channels"`
	Healthy          int          `json:"healthy_channels"`
}

type channelRow struct {
	ID        int64  `json:"id"`
	RouteID   int64  `json:"route_id"`
	Name      string `json:"name"`
	BaseURL   string `json:"base_url"`
	APIFormat string `json:"api_format"`
	Weight    int    `json:"weight"`
	Priority  int    `json:"priority"`
	Enabled   bool   `json:"enabled"`
	HasKey    bool   `json:"has_key"` // never expose the provider key
	Open      bool   `json:"circuit_open"`
}

func (s *Server) listRoutes(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	snapshot := make([]Route, len(s.Routes))
	copy(snapshot, s.Routes)
	s.mu.RUnlock()

	out := make([]routeRow, 0, len(snapshot))
	for _, rt := range snapshot {
		row := routeRow{
			ID: rt.ID, Name: rt.Name, BaseURL: rt.BaseURL, MatchPrefix: rt.MatchPrefix,
			UpstreamRPS: rt.UpstreamRPS, CacheTTL: rt.CacheTTL, CBEnabled: rt.CBEnabled,
			APIFormat: rt.APIFormat, HasDownstreamKey: rt.DownstreamAuthKey != "",
			Models: rt.Models, CacheScope: normaliseCacheScope(rt.CacheScope),
		}
		for _, c := range rt.Channels {
			row.Channels = append(row.Channels, channelRow{
				ID: c.ID, RouteID: c.RouteID, Name: c.Name, BaseURL: c.BaseURL,
				APIFormat: c.APIFormat, Weight: c.Weight, Priority: c.Priority,
				Enabled: c.Enabled, HasKey: c.DownstreamAuthKey != "",
				Open: s.isCircuitOpen(r.Context(), c.upstreamKey(rt.Name)),
			})
			if c.Enabled && !s.isCircuitOpen(r.Context(), c.upstreamKey(rt.Name)) {
				row.Healthy++
			}
		}
		out = append(out, row)
	}
	writeJSON(w, out)
}

type routeInput struct {
	Name              string `json:"name"`
	BaseURL           string `json:"base_url"`
	MatchPath         string `json:"match_path"`
	UpstreamRPS       int    `json:"upstream_rps"`
	CacheTTL          int    `json:"cache_ttl"`
	CBEnabled         bool   `json:"cb_enabled"`
	APIFormat         string `json:"api_format"`          // openai-chat / anthropic-messages / generic
	DownstreamAuthKey string `json:"downstream_auth_key"` // provider's API key
	// Models is a pointer so a partial update can tell "field absent" (nil =
	// leave the stored allowlist alone) from "empty array" (clear it).
	//
	// It matches name/base_url/cache_scope, which already treat blank as "keep".
	// Without it, a PUT that only meant to touch cache_scope marshals a nil
	// slice to "null" and wipes the allowlist — silently turning a restricted
	// route into one that accepts every model.
	Models     *[]string `json:"models"`      // allowed model IDs
	CacheScope string    `json:"cache_scope"` // "global" = shared, "key" = per-key
}

// modelListJSON renders the model allowlist for storage.
//
// ok=false means "not supplied"; the write path turns that into "keep the
// current value" instead of overwriting it.
func (in routeInput) modelListJSON() (text string, ok bool) {
	if in.Models == nil {
		return "", false
	}
	b, err := json.Marshal(*in.Models)
	if err != nil || len(b) == 0 {
		return "[]", true
	}
	return string(b), true
}

// normaliseCacheScope accepts only the two known scopes. Anything unrecognised
// falls back to "global" (the pre-existing behaviour) so a typo can never
// silently opt a route into per-key caching.
func normaliseCacheScope(s string) string {
	if strings.EqualFold(strings.TrimSpace(s), "key") {
		return "key"
	}
	return "global"
}

// normaliseScopeOrBlank keeps "" as "" so a partial update can leave the stored
// scope untouched; a non-blank value is still forced to a known scope.
func normaliseScopeOrBlank(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	return normaliseCacheScope(s)
}

// validateRoute is a basic SSRF guard: only http/https and non-private hosts.
// (DNS-rebinding is still possible; for production add egress allow-lists + connect-time IP checks.)
func validateRoute(in routeInput) error {
	u, err := url.Parse(in.BaseURL)
	if err != nil {
		return fmt.Errorf("invalid base_url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	host := u.Hostname()
	if host == "" || host == "localhost" || host == "0.0.0.0" || host == "::1" {
		return fmt.Errorf("private host not allowed")
	}
	if host == "169.254.169.254" {
		return fmt.Errorf("cloud metadata endpoint blocked")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return fmt.Errorf("private IP not allowed")
		}
	}
	return nil
}

// applyRoutes re-reads the route table from Postgres into memory. It does NOT
// bump the shared revision — only reloadRoutes (used by the admin handlers)
// does that. Otherwise replicas would ping-pong: A reloads and bumps; B sees
// the bump, reloads, bumps; A sees that one, and so on forever.
func (s *Server) applyRoutes() {
	routes, err := LoadRoutes(s.DB)
	if err != nil {
		log.Printf("reload routes err: %v", err)
		return
	}
	s.mu.Lock()
	s.Routes = routes
	s.mu.Unlock()
	log.Printf("routes reloaded: %d active", len(routes))
}

// reloadRoutes swaps the in-memory route table after a DB change (no restart)
// and tells the other replicas to do the same.
func (s *Server) reloadRoutes() {
	s.applyRoutes()
	store.BumpRouteVersion(context.Background(), s.RDB)
}

// StartRouteReloader keeps this replica's route table in step with the others.
//
// Routes live in process memory, so an admin change applied on one replica
// would never reach the rest. Two triggers:
//
//  1. The shared Redis revision moved (a peer changed something) -> reload now.
//  2. A periodic forced reload -> covers a lost Redis counter, plus any change
//     an operator made directly in SQL.
//
// The returned function stops the goroutine.
func (s *Server) StartRouteReloader(interval, forceEvery time.Duration) func() {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if forceEvery < interval {
		forceEvery = 5 * time.Minute
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		// Seed before the first tick, not during it: a peer may bump the counter
		// between our startup DB read and our first check, and seeding late
		// would hide exactly that change.
		last := store.RouteVersion(context.Background(), s.RDB)
		lastForce := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				cur := store.RouteVersion(context.Background(), s.RDB)
				changed := false
				if cur >= 0 {
					// "Was unknown, now known" also counts as a change: the
					// counter was either just created or a peer bumped it while
					// we were starting up. Reloading is the safe answer.
					changed = last < 0 || cur != last
					last = cur
				}
				if changed {
					lastForce = time.Now()
					s.applyRoutes()
					continue
				}
				if time.Since(lastForce) >= forceEvery {
					lastForce = time.Now()
					s.applyRoutes()
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}

func (s *Server) createRoute(w http.ResponseWriter, r *http.Request) {
	var in routeInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if in.Name == "" && in.BaseURL != "" {
		// auto-generate name from base_url hostname (same as frontend does)
		u, _ := url.Parse(in.BaseURL)
		if u != nil && u.Hostname() != "" {
			in.Name = strings.Map(func(r rune) rune {
				if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
					return r
				}
				return -1
			}, strings.ToLower(u.Hostname()))
		}
	}
	if in.Name == "" {
		in.Name = "route-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	// base_url is the only hard requirement: without it the gateway has nowhere
	// to forward. match_path is OPTIONAL — when left blank the gateway assigns
	// "/<name>" automatically (collision-free), so configuring a model provider
	// never gets blocked by validation. The assigned prefix is returned in the
	// response and can be changed later via the edit flow.
	if in.BaseURL == "" {
		http.Error(w, "missing required field(s): base_url", http.StatusBadRequest)
		return
	}
	if in.MatchPath == "" {
		in.MatchPath = "/" + in.Name
		s.mu.RLock()
		taken := make(map[string]bool, len(s.Routes))
		for _, rt := range s.Routes {
			taken[rt.MatchPrefix] = true
		}
		s.mu.RUnlock()
		for i := 2; taken[in.MatchPath]; i++ {
			in.MatchPath = fmt.Sprintf("%s-%d", "/"+in.Name, i)
		}
	}
	if in.UpstreamRPS <= 0 {
		in.UpstreamRPS = 50
	}
	if in.CacheTTL <= 0 {
		in.CacheTTL = 30
	}
	if in.APIFormat == "" {
		in.APIFormat = "generic"
	}
	// A route squatting on the unified prefix would half-work: resolveTarget
	// checks the unified entry first, so the route would be silently
	// unreachable while still showing up in the table.
	if up := s.unifiedPrefix(); up != "" && strings.TrimSuffix(in.MatchPath, "/*") == up {
		http.Error(w, fmt.Sprintf("prefix %q is reserved for the unified entry (UNIFIED_PREFIX)", up),
			http.StatusBadRequest)
		return
	}
	if err := validateRoute(in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Creating has no "previous value" to keep, so an absent list means the
	// documented default: no restriction at all.
	modelsText, _ := in.modelListJSON()
	if modelsText == "" {
		modelsText = "[]"
	}
	var id int64
	err := s.DB.QueryRowContext(r.Context(),
		`INSERT INTO routes (name, base_url, match_path, auth_type, upstream_rps, cache_ttl, cb_enabled,
		                         api_format, downstream_auth_key, models, status, cache_scope)
		 VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8,$9,1,$10) RETURNING id`,
		in.Name, in.BaseURL, in.MatchPath, in.UpstreamRPS, in.CacheTTL, in.CBEnabled,
		in.APIFormat, in.DownstreamAuthKey, modelsText,
		normaliseCacheScope(in.CacheScope)).Scan(&id)
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Mirror the route into a default channel so the channel-based selector has
	// something to pick without special-casing channel-less routes.
	_, _ = s.DB.ExecContext(r.Context(),
		`INSERT INTO channels (route_id, name, base_url, downstream_auth_key, api_format, weight, priority, enabled)
		 VALUES ($1,$2,$3,$4,$5,1,0,true)`, id, in.Name, in.BaseURL, in.DownstreamAuthKey, in.APIFormat)
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true, "match_path": in.MatchPath})
}

func (s *Server) updateRoute(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var in routeInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// Empty name / match_path on update means "keep the current value" so a
	// partial form can never wipe an existing route's identity or prefix.
	if in.Name == "" || in.MatchPath == "" {
		var oldName, oldPath string
		_ = s.DB.QueryRowContext(r.Context(),
			`SELECT name, match_path FROM routes WHERE id=$1 AND status=1`, id).Scan(&oldName, &oldPath)
		if in.Name == "" {
			in.Name = oldName
		}
		if in.MatchPath == "" {
			if oldPath != "" {
				in.MatchPath = oldPath
			} else {
				in.MatchPath = "/" + in.Name
			}
		}
	}
	if in.BaseURL != "" {
		if err := validateRoute(in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	// models and cache_scope are only touched when the payload actually sends
	// them. For a partial update an absent field means "leave it alone", the
	// same contract as name/match_path — otherwise a PUT meant to change one
	// thing would quietly reset the rest.
	modelsText, _ := in.modelListJSON()
	if _, err := s.DB.ExecContext(r.Context(),
		`UPDATE routes SET name=$1, base_url=$2, match_path=$3, upstream_rps=$4, cache_ttl=$5,
		         cb_enabled=$6, api_format=$7, downstream_auth_key=$8,
		         models      = COALESCE(NULLIF($9,'') , models),
		         cache_scope = COALESCE(NULLIF($10,''), cache_scope)
		 WHERE id=$11 AND status=1`,
		in.Name, in.BaseURL, in.MatchPath, in.UpstreamRPS, in.CacheTTL, in.CBEnabled,
		in.APIFormat, in.DownstreamAuthKey,
		// Blank -> COALESCE keeps the stored value. An explicit "[]" clears the
		// allowlist, which is why this needs to be a pointer rather than a slice.
		modelsText,
		// Same contract for the scope, plus normalisation so only "key" or
		// "global" can ever reach the column.
		normaliseScopeOrBlank(in.CacheScope),
		id); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Keep the route's fallback channel in step with the route itself for
	// routes that never got an explicit channel set.
	_, _ = s.DB.ExecContext(r.Context(), `
		UPDATE channels SET name=$2, base_url=$3, api_format=$4
		 WHERE route_id=$1 AND priority=0 AND id = (SELECT min(id) FROM channels WHERE route_id=$1)`,
		id, in.Name, nullIfEmpty(in.BaseURL), in.APIFormat)
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func (s *Server) deleteRoute(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	// Deactivate the route and disable its channels together.
	//
	// The channel update is not cosmetic. A route is what pulls a channel into
	// memory, so once the route is off, its channels can never be reached.
	// Leaving them enabled accumulates rows that look live but are not, and
	// "how many upstreams are actually serving" stops being answerable from
	// the data. The rows are kept either way - request_logs may reference them.
	//
	// Both statements share a transaction: a half-applied delete is worse than
	// a failed one.
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// soft delete: keep the row for audit, just deactivate it.
	if _, err := tx.ExecContext(r.Context(),
		`UPDATE routes SET status=0 WHERE id=$1`, id); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := tx.ExecContext(r.Context(),
		`UPDATE channels SET enabled=false WHERE route_id=$1`, id); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

// ----- channels -----

// adminChannels dispatches GET(list) / POST(create) / PUT(update) / DELETE.
func (s *Server) adminChannels(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listChannels(w, r)
	case http.MethodPost:
		s.createChannel(w, r)
	case http.MethodPut:
		s.updateChannel(w, r)
	case http.MethodDelete:
		s.deleteChannel(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) listChannels(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("route_id")
	query := `SELECT id, route_id, COALESCE(name,''), base_url, api_format,
	                 COALESCE(downstream_auth_key,''), weight, priority, enabled
	          FROM channels`
	var args []interface{}
	if q != "" {
		id, err := strconv.ParseInt(q, 10, 64)
		if err != nil {
			http.Error(w, "invalid route_id", http.StatusBadRequest)
			return
		}
		query += " WHERE route_id=$1"
		args = append(args, id)
	}
	query += " ORDER BY route_id, priority, id"
	rows, err := s.DB.QueryContext(r.Context(), query, args...)
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []channelRow{}
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.RouteID, &c.Name, &c.BaseURL, &c.APIFormat,
			&c.DownstreamAuthKey, &c.Weight, &c.Priority, &c.Enabled); err != nil {
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}
		out = append(out, channelRow{
			ID: c.ID, RouteID: c.RouteID, Name: c.Name, BaseURL: c.BaseURL,
			APIFormat: c.APIFormat, Weight: c.Weight, Priority: c.Priority,
			Enabled: c.Enabled, HasKey: c.DownstreamAuthKey != "",
		})
	}
	writeJSON(w, out)
}

type channelInput struct {
	RouteID           int64   `json:"route_id"`
	Name              string  `json:"name"`
	BaseURL           string  `json:"base_url"`
	DownstreamAuthKey string  `json:"downstream_auth_key"`
	APIFormat         *string `json:"api_format"`
	Weight            *int    `json:"weight"`
	Priority          *int    `json:"priority"`
	Enabled           *bool   `json:"enabled"`
}

func (s *Server) createChannel(w http.ResponseWriter, r *http.Request) {
	var in channelInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if in.RouteID == 0 {
		http.Error(w, "missing required field(s): route_id", http.StatusBadRequest)
		return
	}
	if in.BaseURL == "" {
		http.Error(w, "missing required field(s): base_url", http.StatusBadRequest)
		return
	}
	// Reuse the route SSRF guard by borrowing its shape.
	if err := validateRoute(routeInput{BaseURL: in.BaseURL}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in.Name == "" {
		u, _ := url.Parse(in.BaseURL)
		if u != nil && u.Hostname() != "" {
			in.Name = u.Hostname()
		} else {
			in.Name = "channel-" + strconv.FormatInt(time.Now().Unix(), 10)
		}
	}
	weight := 1
	if in.Weight != nil && *in.Weight > 0 {
		weight = *in.Weight
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	var id int64
	err := s.DB.QueryRowContext(r.Context(), `
		INSERT INTO channels (route_id, name, base_url, downstream_auth_key, api_format, weight, priority, enabled)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
		in.RouteID, in.Name, in.BaseURL, in.DownstreamAuthKey, derefStr(in.APIFormat, "generic"),
		weight, derefInt(in.Priority, 0), enabled).Scan(&id)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23503" {
			http.Error(w, "route not found", http.StatusBadRequest)
			return
		}
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

func (s *Server) updateChannel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var in channelInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if in.BaseURL != "" {
		if err := validateRoute(routeInput{BaseURL: in.BaseURL}); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	res, err := s.DB.ExecContext(r.Context(), `
		UPDATE channels SET
		  name                = COALESCE(NULLIF($2,''), name),
		  base_url            = COALESCE(NULLIF($3,''), base_url),
		  downstream_auth_key = CASE WHEN $4::boolean THEN $5 ELSE downstream_auth_key END,
		  api_format          = COALESCE($6, api_format),
		  weight              = COALESCE($7, weight),
		  priority            = COALESCE($8, priority),
		  enabled             = COALESCE($9, enabled)
		WHERE id = $1`,
		id, in.Name, in.BaseURL,
		in.DownstreamAuthKey != "", in.DownstreamAuthKey,
		in.APIFormat, in.Weight, in.Priority, in.Enabled)
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		http.Error(w, "channel not found", http.StatusNotFound)
		return
	}
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

func (s *Server) deleteChannel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var routeID int64
	if err := s.DB.QueryRowContext(r.Context(), `SELECT route_id FROM channels WHERE id=$1`, id).Scan(&routeID); err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "channel not found", http.StatusNotFound)
			return
		}
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Deleting the last channel would silently change behaviour: the route would
	// fall back to its own base_url, which may point somewhere else entirely.
	var remaining int
	if err := s.DB.QueryRowContext(r.Context(),
		`SELECT count(*) FROM channels WHERE route_id=$1 AND id <> $2 AND enabled`, routeID, id).Scan(&remaining); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if remaining == 0 {
		http.Error(w, "cannot delete the last enabled channel of a route", http.StatusBadRequest)
		return
	}
	if _, err := s.DB.ExecContext(r.Context(), `DELETE FROM channels WHERE id=$1`, id); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.reloadRoutes()
	writeJSON(w, map[string]interface{}{"id": id, "ok": true})
}

func derefStr(p *string, def string) string {
	if p == nil || *p == "" {
		return def
	}
	return *p
}

func derefInt(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

// ----- request logs -----

type logRow struct {
	ID               int64  `json:"id"`
	APIKeyID         int64  `json:"api_key_id"`
	Method           string `json:"method"`
	Path             string `json:"path"`
	Upstream         string `json:"upstream"`
	StatusCode       int    `json:"status_code"`
	LatencyMs        int    `json:"latency_ms"`
	Cached           bool   `json:"cached"`
	Model            string `json:"model"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
	IsStream         bool   `json:"is_stream"`
	ChannelID        int64  `json:"channel_id"`
	ErrMsg           string `json:"err_msg"`
	// TTFTMs is Time To First Token. Only populated for streaming responses;
	// 0 for buffered responses, cache hits and pre-proxy rejections.
	TTFTMs int64 `json:"ttft_ms"`
	// TokensPerSec is the generation throughput. The column is NUMERIC(10,2),
	// cast to float8 in SQL so it scans cleanly.
	TokensPerSec float64 `json:"tokens_per_sec"`
	// CacheHitTokens is the part of PromptTokens the provider served from its
	// own prefix cache. Compare it with PromptTokens to see what share of the
	// input was billed at the discounted rate.
	CacheHitTokens int64 `json:"prompt_cache_hit_tokens"`
	// CacheWriteTokens is the part of PromptTokens the provider charged a
	// premium to store. Anthropic only.
	CacheWriteTokens int64 `json:"prompt_cache_write_tokens"`
	// RejectReason names the gate that refused the request and is empty when
	// it reached an upstream. See migrate.sql for the full vocabulary.
	RejectReason string `json:"reject_reason"`
	CreatedAt    string `json:"created_at"`
}

// adminLogs returns the most recent request log lines. The optional "limit"
// query parameter (default 50, capped at 500) lets the console pull a deeper
// window: internet scanner noise floods the tail, so a shallow slice can hold
// almost no real traffic for the console's filtered views to show.
func (s *Server) adminLogs(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	rows, err := s.DB.QueryContext(r.Context(), `
		SELECT id, COALESCE(api_key_id,0), method, path, COALESCE(upstream,''), status_code,
		       latency_ms, cached, COALESCE(model,''),
		       COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0), COALESCE(total_tokens,0),
		       COALESCE(is_stream,false), COALESCE(channel_id,0), COALESCE(err_msg,''),
		       COALESCE(ttft_ms,0), COALESCE(tokens_per_sec,0)::float8,
		       COALESCE(prompt_cache_hit_tokens,0), COALESCE(prompt_cache_write_tokens,0),
		       COALESCE(reject_reason,''),
		       created_at
		FROM request_logs ORDER BY id DESC LIMIT $1
	`, limit)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []logRow{}
	for rows.Next() {
		var l logRow
		var created time.Time
		if err := rows.Scan(&l.ID, &l.APIKeyID, &l.Method, &l.Path, &l.Upstream, &l.StatusCode,
			&l.LatencyMs, &l.Cached, &l.Model, &l.PromptTokens, &l.CompletionTokens, &l.TotalTokens,
			&l.IsStream, &l.ChannelID, &l.ErrMsg, &l.TTFTMs, &l.TokensPerSec,
			&l.CacheHitTokens, &l.CacheWriteTokens, &l.RejectReason, &created); err != nil {
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}
		l.CreatedAt = created.Format(time.RFC3339)
		out = append(out, l)
	}
	writeJSON(w, out)
}

// ----- usage dashboard -----

type usageRow struct {
	KeyID    int64  `json:"key_id"`
	Owner    string `json:"owner"`
	Requests int64  `json:"requests"`
	Prompt   int64  `json:"prompt_tokens"`
	Complete int64  `json:"completion_tokens"`
	Tokens   int64  `json:"total_tokens"`
	Errors   int64  `json:"errors"`
}

// usageLatency carries the speed metrics the console shows alongside the
// spend metrics. TTFT and throughput are averaged only over rows that actually
// have them: a buffered answer has no first-token event, and a rejected
// request has no tokens, so including those would drag every number toward
// zero and hide the thing being measured.
type usageLatency struct {
	StreamRequests  int64   `json:"stream_requests"`
	AvgTTFTMs       int64   `json:"avg_ttft_ms"`
	P95TTFTMs       int64   `json:"p95_ttft_ms"`
	AvgLatencyMs    int64   `json:"avg_latency_ms"`
	P95LatencyMs    int64   `json:"p95_latency_ms"`
	AvgTokensPerSec float64 `json:"avg_tokens_per_sec"`
	// CacheHits is counted over the whole window (not just streams) so the hit
	// rate stays comparable with the request count on the KPI row above.
	CacheHits int64 `json:"cache_hits"`
}

// usagePromptCache carries the provider-side prefix cache accounting for the
// window. This is the number that decides what the window actually cost:
// providers bill input tokens served from their own cache at a fraction of the
// normal rate, so two windows with the same prompt total can differ by an order
// of magnitude in price.
//
// It is deliberately separate from usageLatency.CacheHits, which counts the
// gateway's own GET response cache. That one is nearly always zero for an LLM
// workload because chat completions are POSTs, and reading the two from one
// number is how a dashboard ends up claiming a healthy service has a 0% hit
// rate.
type usagePromptCache struct {
	// PromptTokens is the denominator: every input token billed in the window.
	PromptTokens int64 `json:"prompt_tokens"`
	// HitTokens is input the provider served from cache at the discounted rate.
	HitTokens int64 `json:"hit_tokens"`
	// WriteTokens is input the provider charged a premium to store.
	WriteTokens int64 `json:"write_tokens"`
	// MeasuredRequests is how many rows carried a non-zero prompt total. The
	// rate is only meaningful over those: rows that never reached a model
	// (rejections, gateway cache hits, GET probes) report no tokens at all and
	// would drag the rate toward zero.
	MeasuredRequests int64 `json:"measured_requests"`
	// HitRate is HitTokens/PromptTokens as a percentage. Zero when nothing was
	// measured, which must not be rendered as a real 0%.
	HitRate float64 `json:"hit_rate"`
}

type usageModelRow struct {
	Model    string `json:"model"`
	Requests int64  `json:"requests"`
	Tokens   int64  `json:"total_tokens"`
	// Per-model speed: the ranking that actually decides which provider to
	// keep. Averages are over the rows that carry the measurement.
	AvgTTFTMs       int64   `json:"avg_ttft_ms"`
	AvgTokensPerSec float64 `json:"avg_tokens_per_sec"`
	// CacheHitRate is this model's share of input tokens the provider served
	// from its prefix cache. Two models with the same per-token price are not
	// equally cheap if only one of them warms a cache.
	CacheHitRate float64 `json:"cache_hit_rate"`
}

type usageDayRow struct {
	Day      string `json:"day"`
	Requests int64  `json:"requests"`
	Tokens   int64  `json:"total_tokens"`
}

type usageStatusRow struct {
	StatusCode int   `json:"status_code"`
	Count      int64 `json:"count"`
}

type usageSnapshot struct {
	From        string           `json:"from"`
	Hours       int              `json:"hours"`
	Totals      usageRow         `json:"totals"`
	Latency     usageLatency     `json:"latency"`
	PromptCache usagePromptCache `json:"prompt_cache"`
	ByKey       []usageRow       `json:"by_key"`
	ByModel     []usageModelRow  `json:"by_model"`
	ByDay       []usageDayRow    `json:"by_day"`
	ByStatus    []usageStatusRow `json:"by_status"`
}

// adminUsage aggregates request logs for the usage dashboard. The window is
// capped so a large table cannot turn a dashboard refresh into a full scan.
func (s *Server) adminUsage(w http.ResponseWriter, r *http.Request) {
	hours := 24
	if h := r.URL.Query().Get("hours"); h != "" {
		if n, err := strconv.Atoi(h); err == nil && n > 0 {
			hours = n
			if hours > 24*90 {
				hours = 24 * 90
			}
		}
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour).UTC()
	ctx := r.Context()

	snap := usageSnapshot{
		From:     from.Format(time.RFC3339),
		Hours:    hours,
		ByKey:    []usageRow{},
		ByModel:  []usageModelRow{},
		ByDay:    []usageDayRow{},
		ByStatus: []usageStatusRow{},
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(k.id,0), COALESCE(k.owner,'(deleted)'),
		       count(*),
		       COALESCE(sum(l.prompt_tokens),0), COALESCE(sum(l.completion_tokens),0),
		       COALESCE(sum(l.total_tokens),0),
		       COALESCE(sum(CASE WHEN l.status_code >= 400 THEN 1 ELSE 0 END),0)
		FROM request_logs l
		LEFT JOIN api_keys k ON k.id = l.api_key_id
		WHERE l.created_at >= $1
		GROUP BY k.id, k.owner
		ORDER BY 6 DESC
	`, from)
	if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for rows.Next() {
		var u usageRow
		if err := rows.Scan(&u.KeyID, &u.Owner, &u.Requests, &u.Prompt, &u.Complete, &u.Tokens, &u.Errors); err != nil {
			rows.Close()
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}
		snap.ByKey = append(snap.ByKey, u)
		snap.Totals.Requests += u.Requests
		snap.Totals.Prompt += u.Prompt
		snap.Totals.Complete += u.Complete
		snap.Totals.Tokens += u.Tokens
		snap.Totals.Errors += u.Errors
	}
	rows.Close()

	// Speed plus cost metrics, one pass over the window. percentile_cont gives
	// a true interpolated p95 rather than "the worst row in the top 5%", which
	// is what matters when a single stalled request would otherwise dominate.
	// The prompt-cache sums ride along here rather than in their own scan.
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(count(*) FILTER (WHERE l.ttft_ms > 0),0),
		       COALESCE(avg(l.ttft_ms) FILTER (WHERE l.ttft_ms > 0),0)::bigint,
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY l.ttft_ms)
		                FILTER (WHERE l.ttft_ms > 0),0)::bigint,
		       COALESCE(avg(l.latency_ms),0)::bigint,
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY l.latency_ms),0)::bigint,
		       COALESCE(avg(l.tokens_per_sec) FILTER (WHERE l.tokens_per_sec > 0),0)::float8,
		       COALESCE(count(*) FILTER (WHERE l.cached),0),
		       COALESCE(sum(l.prompt_tokens),0),
		       COALESCE(sum(l.prompt_cache_hit_tokens),0),
		       COALESCE(sum(l.prompt_cache_write_tokens),0),
		       COALESCE(count(*) FILTER (WHERE l.prompt_tokens > 0),0)
		FROM request_logs l WHERE l.created_at >= $1
	`, from).Scan(&snap.Latency.StreamRequests, &snap.Latency.AvgTTFTMs,
		&snap.Latency.P95TTFTMs, &snap.Latency.AvgLatencyMs,
		&snap.Latency.P95LatencyMs, &snap.Latency.AvgTokensPerSec,
		&snap.Latency.CacheHits,
		&snap.PromptCache.PromptTokens, &snap.PromptCache.HitTokens,
		&snap.PromptCache.WriteTokens, &snap.PromptCache.MeasuredRequests); err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if snap.PromptCache.PromptTokens > 0 {
		snap.PromptCache.HitRate =
			float64(snap.PromptCache.HitTokens) / float64(snap.PromptCache.PromptTokens) * 100
	}

	mrows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(model,''),'(none)'), count(*), COALESCE(sum(total_tokens),0),
		       COALESCE(avg(ttft_ms) FILTER (WHERE ttft_ms > 0),0)::bigint,
		       COALESCE(avg(tokens_per_sec) FILTER (WHERE tokens_per_sec > 0),0)::float8,
		       COALESCE(sum(prompt_cache_hit_tokens),0), COALESCE(sum(prompt_tokens),0)
		FROM request_logs WHERE created_at >= $1
		GROUP BY 1 ORDER BY 3 DESC LIMIT 100
	`, from)
	if err == nil {
		for mrows.Next() {
			var m usageModelRow
			var hit, prompt int64
			if err := mrows.Scan(&m.Model, &m.Requests, &m.Tokens,
				&m.AvgTTFTMs, &m.AvgTokensPerSec, &hit, &prompt); err == nil {
				if prompt > 0 {
					m.CacheHitRate = float64(hit) / float64(prompt) * 100
				}
				snap.ByModel = append(snap.ByModel, m)
			}
		}
		mrows.Close()
	}

	drows, err := s.DB.QueryContext(ctx, `
		SELECT to_char(date_trunc('day', created_at), 'YYYY-MM-DD'), count(*), COALESCE(sum(total_tokens),0)
		FROM request_logs WHERE created_at >= $1
		GROUP BY 1 ORDER BY 1
	`, from)
	if err == nil {
		for drows.Next() {
			var d usageDayRow
			if err := drows.Scan(&d.Day, &d.Requests, &d.Tokens); err == nil {
				snap.ByDay = append(snap.ByDay, d)
			}
		}
		drows.Close()
	}

	srows, err := s.DB.QueryContext(ctx, `
		SELECT status_code, count(*) FROM request_logs WHERE created_at >= $1
		GROUP BY 1 ORDER BY 2 DESC
	`, from)
	if err == nil {
		for srows.Next() {
			var st usageStatusRow
			if err := srows.Scan(&st.StatusCode, &st.Count); err == nil {
				snap.ByStatus = append(snap.ByStatus, st)
			}
		}
		srows.Close()
	}

	writeJSON(w, snap)
}

// ----- playground -----

// playgroundTimeout bounds a single debug call so the console never hangs
// forever on a slow stream.
const playgroundTimeout = 120 * time.Second

type playgroundRequest struct {
	RouteID int64  `json:"route_id"`
	Path    string `json:"path"`
	Method  string `json:"method"`
	Body    string `json:"body"`
}

type playgroundResponse struct {
	Status    int               `json:"status"`
	LatencyMs int64             `json:"latency_ms"`
	Channel   string            `json:"channel"`
	Headers   map[string]string `json:"headers"`
	Body      string            `json:"body"`
	Truncated bool              `json:"truncated"`
}

// captureResponse is a minimal ResponseWriter used to run a real proxy call and
// inspect the result, without importing net/http/httptest into production code.
type captureResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
	wrote  bool
}

func (c *captureResponse) Header() http.Header {
	if c.header == nil {
		c.header = http.Header{}
	}
	return c.header
}

func (c *captureResponse) WriteHeader(code int) {
	if !c.wrote {
		c.status = code
		c.wrote = true
	}
}

func (c *captureResponse) Write(b []byte) (int, error) {
	if !c.wrote {
		c.WriteHeader(http.StatusOK)
	}
	return c.body.Write(b)
}

func (c *captureResponse) Flush() {}

// adminPlayground runs one request through the real proxy path so an operator
// can debug a route from the console.
//
// It deliberately skips auth, quota and rate limiting: this is an
// admin-authenticated debugging tool, and charging a real key for it would
// pollute both the quota and the usage dashboard.
func (s *Server) adminPlayground(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in playgroundRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	route := s.routeByID(in.RouteID)
	if route == nil {
		http.Error(w, "route not found", http.StatusNotFound)
		return
	}
	method := in.Method
	if method == "" {
		method = http.MethodPost
	}
	path := in.Path
	if path == "" {
		path = route.MatchPrefix
	}
	order := s.orderedChannels(route)
	if len(order) == 0 {
		http.Error(w, "route has no channel", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), playgroundTimeout)
	defer cancel()

	body := []byte(in.Body)
	req, err := http.NewRequestWithContext(ctx, method, "http://gateway.local"+path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	m := &meta{RouteID: route.ID, Upstream: route.Name}
	req = req.WithContext(context.WithValue(ctx, ctxKeyMeta, m))

	// Peek the body so the model allowlist is enforced exactly as the proxy
	// enforces it. peekBody hands the bytes straight back on req.Body, so the
	// local `body` is left as the operator typed it: trimming it to the peeked
	// prefix would silently truncate any payload past the inspect cap.
	peeked, oversized, _ := peekBody(req, maxInspectBody)
	model, modelSeen := extractModel(inspectableBody(req, peeked))
	if err := s.authorizeModel(req, route, model, modelSeen, oversized); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The playground addresses a route directly, but the operator may still type
	// either the unified path or the route's own prefix - strip whichever matched.
	suffix := strings.TrimPrefix(path, route.MatchPrefix)
	if up := s.unifiedPrefix(); up != "" && (path == up || strings.HasPrefix(path, up+"/")) {
		suffix = strings.TrimPrefix(path, up)
	}

	start := time.Now()
	resp, err := s.forward(ctx, req, route, order[0], suffix, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, playgroundResponse{
			Status:    http.StatusBadGateway,
			LatencyMs: time.Since(start).Milliseconds(),
			Channel:   order[0].Label(),
			Body:      "upstream error: " + err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	rec := &captureResponse{}
	s.writeUpstream(rec, req, resp, route, order[0])

	headers := map[string]string{}
	for k, vv := range rec.Header() {
		if len(vv) > 0 {
			headers[k] = vv[0]
		}
	}
	out := playgroundResponse{
		Status:    rec.status,
		LatencyMs: time.Since(start).Milliseconds(),
		Channel:   order[0].Label(),
		Headers:   headers,
		Body:      rec.body.String(),
	}
	// Keep the console responsive: a huge payload would be unreadable anyway.
	if len(out.Body) > 200000 {
		out.Body = out.Body[:200000]
		out.Truncated = true
	}
	writeJSON(w, out)
}
