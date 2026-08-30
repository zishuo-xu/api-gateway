package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// testRedis returns a live Redis client, or skips the test when the local
// Redis is not running. Quota and key-cache behaviour lives in Redis, so those
// tests genuinely need it rather than a stub.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := store.NewRedis("localhost:6379", "", 0)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	return rdb
}

// testDB returns a live Postgres handle, or skips the test. Only the route
// reload tests need it - everything else runs against httptest doubles.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.NewDB("postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable")
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	// Tests read and write the same schema production does, so they have to be
	// on the same version of it. Without this, every new column shows up as a
	// "column does not exist" failure in whichever test happens to touch it
	// first — a confusing error that points at the test instead of the schema.
	// migrate.sql is idempotent, so running it per test is safe.
	if err := store.AutoMigrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// testChain builds the middleware stack a real request passes through, minus
// the cache (GET-only, and a shared cache would make assertions order
// dependent). Everything else mirrors Handler() so tests exercise the same
// order as production - rate limiting included, since quota and throttling
// interact (a throttled request must not be charged).
func testChain(s *Server) http.Handler {
	var h http.Handler = http.HandlerFunc(s.proxy)
	h = s.mwRateLimit(h)
	h = s.mwQuota(h)
	h = s.mwAuth(h)
	h = s.mwLogging(h)
	return h
}

// seedKey registers a raw key in Redis with the given policy.
//
// The id is derived from a checksum of the key, NOT from its length: two keys
// of equal length would otherwise collide onto one id, and any test that needs
// two distinct callers (rate-limit sharing, quota isolation) would silently
// exercise a single one.
func seedKey(t *testing.T, rdb *redis.Client, raw string, ki store.KeyInfo) int64 {
	t.Helper()
	ki.ID = 1000 + int64(crc32.ChecksumIEEE([]byte(raw))%100000)
	hash := store.HashKey(raw)
	if err := rdb.Set(context.Background(), "key:"+hash, store.EncodeKeyInfo(ki), 0).Err(); err != nil {
		t.Fatalf("seed key: %v", err)
	}
	rdb.Del(context.Background(), "quota:"+itoa(ki.ID))
	t.Cleanup(func() {
		rdb.Del(context.Background(), "key:"+hash)
		rdb.Del(context.Background(), "quota:"+itoa(ki.ID))
	})
	return ki.ID
}

func itoa(n int64) string { return fmt.Sprintf("%d", n) }

// echoUpstream replies 200 with whatever body it received.
func echoUpstream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	}))
}

// ---------------------------------------------------------------------------
// P0-1 quota
// ---------------------------------------------------------------------------

// Before this change quota_used was only ever SELECTed: the console showed a
// number that never moved, and quota_limit was decorative.
func TestQuotaBlocksWhenExhausted(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "q", BaseURL: up.URL, MatchPrefix: "/q", Upstream: "q",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	raw := "gw-quota-test"
	id := seedKey(t, rdb, raw, store.KeyInfo{QuotaLimit: 10})

	call := func() int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/q", strings.NewReader(`{"hello":"world"}`))
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := call(); got != http.StatusOK {
		t.Fatalf("first call = %d, want 200", got)
	}
	// Burn the rest of the allowance: 9 more units on a limit of 10.
	if err := store.QuotaAdd(context.Background(), rdb, id, 9); err != nil {
		t.Fatalf("quota add: %v", err)
	}
	if got := call(); got != http.StatusTooManyRequests {
		t.Errorf("call at limit = %d, want 429 (quota must actually block)", got)
	}
}

func TestQuotaUnlimitedWhenLimitNotPositive(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "u", BaseURL: up.URL, MatchPrefix: "/u", Upstream: "u",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	raw := "gw-unlimited-test"
	id := seedKey(t, rdb, raw, store.KeyInfo{QuotaLimit: 0})
	if err := store.QuotaAdd(context.Background(), rdb, id, 5_000_000); err != nil {
		t.Fatalf("quota add: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/u", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (limit 0 means unlimited)", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// P0-3 key expiry and IP allowlist
// ---------------------------------------------------------------------------

func TestExpiredKeyRejected(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "e", BaseURL: up.URL, MatchPrefix: "/e", Upstream: "e",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(time.Hour).Unix()
	expired := "gw-expired-test"
	valid := "gw-valid-test"
	seedKey(t, rdb, expired, store.KeyInfo{ExpiresAt: past})
	seedKey(t, rdb, valid, store.KeyInfo{ExpiresAt: future})

	call := func(raw string) int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/e", strings.NewReader(`{}`))
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := call(expired); got != http.StatusForbidden {
		t.Errorf("expired key = %d, want 403", got)
	}
	if got := call(valid); got != http.StatusOK {
		t.Errorf("valid key = %d, want 200", got)
	}
}

func TestIPAllowlistEnforced(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "ip", BaseURL: up.URL, MatchPrefix: "/ip", Upstream: "ip",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	blocked := "gw-ip-blocked"
	allowed := "gw-ip-allowed"
	cidr := "gw-ip-cidr"
	seedKey(t, rdb, blocked, store.KeyInfo{AllowedIPs: []string{"10.0.0.1"}})
	seedKey(t, rdb, allowed, store.KeyInfo{AllowedIPs: []string{"127.0.0.1"}})
	seedKey(t, rdb, cidr, store.KeyInfo{AllowedIPs: []string{"127.0.0.0/8"}})

	call := func(raw string) int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/ip", strings.NewReader(`{}`))
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := call(blocked); got != http.StatusForbidden {
		t.Errorf("non-matching IP = %d, want 403", got)
	}
	if got := call(allowed); got != http.StatusOK {
		t.Errorf("exact IP match = %d, want 200", got)
	}
	if got := call(cidr); got != http.StatusOK {
		t.Errorf("CIDR match = %d, want 200", got)
	}
}

// Forwarded headers must not be trusted by default: otherwise any client could
// forge X-Forwarded-For and walk through an IP allowlist.
func TestForwardedHeaderIgnoredWithoutTrustProxy(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "xff", BaseURL: up.URL, MatchPrefix: "/xff", Upstream: "xff",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	raw := "gw-xff-test"
	seedKey(t, rdb, raw, store.KeyInfo{AllowedIPs: []string{"10.0.0.1"}})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/xff", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("spoofed XFF = %d, want 403 (headers must not be trusted by default)", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// P0-2 model allowlist
// ---------------------------------------------------------------------------

// The allowlist used to be stored and echoed back but never compared to the
// request, so it protected nothing.
func TestModelAllowlistBlocksUnlistedModel(t *testing.T) {
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "m", BaseURL: up.URL, MatchPrefix: "/m", Upstream: "m",
		APIFormat: "openai-chat", Models: []string{"gpt-4o"}, UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	call := func(model string) (int, string) {
		body := fmt.Sprintf(`{"model":%q,"messages":[]}`, model)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/m/chat/completions", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, _ := call("gpt-4o"); code != http.StatusOK {
		t.Errorf("allowlisted model = %d, want 200", code)
	}
	if code, body := call("claude-3-5-sonnet"); code != http.StatusForbidden {
		t.Errorf("unlisted model = %d, want 403 (body %q)", code, body)
	}
}

// The critical regression guard: reading the body to check the model must not
// consume it. Before the NopCloser refill this test would see an empty body.
func TestModelCheckLeavesRequestBodyIntact(t *testing.T) {
	var got atomic.Value
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(string(b))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "b", BaseURL: up.URL, MatchPrefix: "/b", Upstream: "b",
		APIFormat: "openai-chat", Models: []string{"gpt-4o"}, UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	payload := `{"model":"gpt-4o","messages":[{"role":"user","content":"hello world"}]}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/b/chat/completions", strings.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	seen, _ := got.Load().(string)
	if seen != payload {
		t.Fatalf("upstream received body %q, want the original %q - model check consumed the body", seen, payload)
	}
}

// A GET carries no model and must not be blocked: /v1/models is the discovery
// call every client makes.
func TestModelCheckSkipsGetRequests(t *testing.T) {
	up := echoUpstream()
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "g", BaseURL: up.URL, MatchPrefix: "/g", Upstream: "g",
		APIFormat: "openai-chat", Models: []string{"gpt-4o"}, UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/g/models", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /g/models = %d, want 200 (GET has no model to check)", resp.StatusCode)
	}
}

func TestGenericRouteNeverInspectsBody(t *testing.T) {
	var got atomic.Value
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(string(b))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "gen", BaseURL: up.URL, MatchPrefix: "/gen", Upstream: "gen",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	// Not valid JSON at all: a generic relay must pass it through untouched.
	payload := "not-json-at-all"
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/gen/thing", strings.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if seen, _ := got.Load().(string); seen != payload {
		t.Errorf("upstream got %q, want %q", seen, payload)
	}
}

// ---------------------------------------------------------------------------
// P1-1 / P1-2 channels: load balancing and failover
// ---------------------------------------------------------------------------

// When a channel fails, the retry must replay the ORIGINAL body. Sending r.Body
// again after the first attempt drained it would turn a failover into a 400.
func TestFailoverReplaysRequestBody(t *testing.T) {
	var hits [2]int64
	var bodies [2]atomic.Value

	newUp := func(i int, status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits[i], 1)
			b, _ := io.ReadAll(r.Body)
			bodies[i].Store(string(b))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"channel":%d}`, i)))
		}))
	}
	bad := newUp(0, http.StatusInternalServerError)
	defer bad.Close()
	good := newUp(1, http.StatusOK)
	defer good.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, MaxAttempts: 3}}
	s.Routes = []Route{{
		Name: "fo", BaseURL: bad.URL, MatchPrefix: "/fo", Upstream: "fo",
		APIFormat: "openai-chat", UpstreamRPS: 1000,
		Channels: []Channel{
			{ID: 1, Name: "bad", BaseURL: bad.URL, Weight: 1, Priority: 0, Enabled: true},
			{ID: 2, Name: "good", BaseURL: good.URL, Weight: 1, Priority: 1, Enabled: true},
		},
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	payload := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/fo/chat/completions", strings.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 after failing over (body %q)", resp.StatusCode, body)
	}
	if hits[0] != 1 || hits[1] != 1 {
		t.Errorf("hits = [%d %d], want [1 1]", hits[0], hits[1])
	}
	if got, _ := bodies[1].Load().(string); got != payload {
		t.Errorf("second channel received %q, want the original body %q", got, payload)
	}
}

// A 4xx is the caller's problem: replaying it against another channel just
// burns quota and hides the real error.
func TestNoFailoverOnClientError(t *testing.T) {
	var hits [2]int64
	newUp := func(i int, status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits[i], 1)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{}`))
		}))
	}
	a := newUp(0, http.StatusBadRequest)
	defer a.Close()
	b := newUp(1, http.StatusOK)
	defer b.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, MaxAttempts: 3}}
	s.Routes = []Route{{
		Name: "nx", BaseURL: a.URL, MatchPrefix: "/nx", Upstream: "nx",
		APIFormat: "generic", UpstreamRPS: 1000,
		Channels: []Channel{
			{ID: 1, Name: "a", BaseURL: a.URL, Weight: 1, Enabled: true},
			{ID: 2, Name: "b", BaseURL: b.URL, Weight: 1, Priority: 1, Enabled: true},
		},
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nx", strings.NewReader(`{}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if hits[1] != 0 {
		t.Errorf("backup channel hit %d times, want 0 (4xx must not trigger failover)", hits[1])
	}
}

// Priority tiers: everything goes to the lowest-numbered tier while it is up.
func TestChannelPriorityPrefersLowerTier(t *testing.T) {
	var hits [2]int64
	newUp := func(i int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits[i], 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
	}
	primary := newUp(0)
	defer primary.Close()
	backup := newUp(1)
	defer backup.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, MaxAttempts: 3}}
	s.Routes = []Route{{
		Name: "pr", BaseURL: primary.URL, MatchPrefix: "/pr", Upstream: "pr",
		APIFormat: "generic", UpstreamRPS: 1000,
		Channels: []Channel{
			{ID: 1, Name: "primary", BaseURL: primary.URL, Weight: 1, Priority: 0, Enabled: true},
			{ID: 2, Name: "backup", BaseURL: backup.URL, Weight: 100, Priority: 5, Enabled: true},
		},
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	for i := 0; i < 30; i++ {
		resp, err := http.Post(ts.URL+"/pr", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
	}
	if hits[0] != 30 || hits[1] != 0 {
		t.Errorf("hits = [%d %d], want [30 0]: priority must beat weight", hits[0], hits[1])
	}
}

// Weight only splits traffic inside the same priority tier.
func TestChannelWeightSplitsTrafficWithinTier(t *testing.T) {
	var hits [2]int64
	newUp := func(i int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits[i], 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
	}
	a := newUp(0)
	defer a.Close()
	b := newUp(1)
	defer b.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, MaxAttempts: 1}}
	s.Routes = []Route{{
		Name: "w", BaseURL: a.URL, MatchPrefix: "/w", Upstream: "w",
		APIFormat: "generic", UpstreamRPS: 1000,
		Channels: []Channel{
			{ID: 1, Name: "a", BaseURL: a.URL, Weight: 3, Priority: 0, Enabled: true},
			{ID: 2, Name: "b", BaseURL: b.URL, Weight: 1, Priority: 0, Enabled: true},
		},
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	const n = 400
	for i := 0; i < n; i++ {
		resp, err := http.Post(ts.URL+"/w", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
	}
	// Expect roughly 3:1. The bound is generous because this is a random
	// permutation, but 4:1 or 1:1 would indicate weights are being ignored.
	if hits[0] < n/2 || hits[1] < n/10 {
		t.Errorf("hits = [%d %d], want roughly 300/100 for weights 3:1", hits[0], hits[1])
	}
}

func TestDisabledChannelIsSkipped(t *testing.T) {
	var hits [2]int64
	newUp := func(i int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits[i], 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
	}
	off := newUp(0)
	defer off.Close()
	on := newUp(1)
	defer on.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "d", BaseURL: off.URL, MatchPrefix: "/d", Upstream: "d",
		APIFormat: "generic", UpstreamRPS: 1000,
		Channels: []Channel{
			// The disabled channel sits in the BETTER tier on purpose: if the
			// selector ever stop skipping it, every request lands here and the
			// assertion fails deterministically instead of 50% of the time.
			{ID: 1, Name: "off", BaseURL: off.URL, Weight: 1, Priority: 0, Enabled: false},
			{ID: 2, Name: "on", BaseURL: on.URL, Weight: 1, Priority: 5, Enabled: true},
		},
	}}
	ts := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer ts.Close()

	for i := 0; i < 20; i++ {
		resp, err := http.Post(ts.URL+"/d", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
	}
	if hits[0] != 0 {
		t.Errorf("disabled channel hit %d times, want 0", hits[0])
	}
	if hits[1] != 20 {
		t.Errorf("enabled channel hit %d times, want 20", hits[1])
	}
}

// ---------------------------------------------------------------------------
// P1-3 usage metering
// ---------------------------------------------------------------------------

func TestUsageFromJSONProviderShapes(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantModel  string
		wantPrompt int64
		wantComp   int64
	}{
		{
			name:       "openai chat",
			body:       `{"model":"gpt-4o","usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}`,
			wantModel:  "gpt-4o",
			wantPrompt: 11, wantComp: 22,
		},
		{
			name:       "anthropic messages",
			body:       `{"model":"claude-3","usage":{"input_tokens":5,"output_tokens":7}}`,
			wantModel:  "claude-3",
			wantPrompt: 5, wantComp: 7,
		},
		{
			name:       "anthropic sse message_start",
			body:       `{"type":"message_start","message":{"model":"claude-3","usage":{"input_tokens":9}}}`,
			wantModel:  "claude-3",
			wantPrompt: 9, wantComp: 0,
		},
		{
			name:       "no usage",
			body:       `{"model":"x","choices":[]}`,
			wantModel:  "x",
			wantPrompt: 0, wantComp: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := usageFromJSON([]byte(c.body))
			if f.model != c.wantModel || f.prompt != c.wantPrompt || f.completion != c.wantComp {
				t.Errorf("usageFromJSON = (%q,%d,%d), want (%q,%d,%d)",
					f.model, f.prompt, f.completion, c.wantModel, c.wantPrompt, c.wantComp)
			}
		})
	}
}

// A stream reports tokens only in its final frame (and only when
// include_usage was requested), so the sniffer has to scan as it copies.
func TestUsageSnifferReadsFinalStreamFrame(t *testing.T) {
	var out bytes.Buffer
	sniff := &usageSniffer{w: &out}

	frames := []string{
		`data: {"id":"1","model":"gpt-4o","choices":[{"delta":{"content":"he"}}]}` + "\n\n",
		`data: {"id":"1","model":"gpt-4o","choices":[{"delta":{"content":"llo"}}]}` + "\n\n",
		`data: {"id":"1","model":"gpt-4o","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}
	for _, f := range frames {
		if _, err := sniff.Write([]byte(f)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if sniff.prompt != 12 || sniff.completion != 8 {
		t.Errorf("usage = (%d,%d), want (12,8): streaming usage must be metered", sniff.prompt, sniff.completion)
	}
	if sniff.model != "gpt-4o" {
		t.Errorf("model = %q, want gpt-4o", sniff.model)
	}
	if out.Len() != len(strings.Join(frames, "")) {
		t.Errorf("bytes forwarded = %d, want %d: sniffer must not alter the stream",
			out.Len(), len(strings.Join(frames, "")))
	}
}

func TestProxyRecordsTokensFromJSONResponse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":3,"completion_tokens":4}}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}}
	s.Routes = []Route{{
		Name: "tok", BaseURL: up.URL, MatchPrefix: "/tok", Upstream: "tok",
		APIFormat: "openai-chat", UpstreamRPS: 1000,
	}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tok/chat/completions", strings.NewReader(`{"model":"gpt-4o"}`))
	req = req.WithContext(withMeta(req.Context(), &meta{}))
	m := metaFrom(req)
	s.proxy(rec, req)

	if m.PromptTokens != 3 || m.CompletionTokens != 4 {
		t.Errorf("tokens = (%d,%d), want (3,4)", m.PromptTokens, m.CompletionTokens)
	}
	if m.Model != "gpt-4o" {
		t.Errorf("model = %q, want gpt-4o", m.Model)
	}
	if !m.Billed {
		t.Error("Billed = false, want true for a request that reached upstream")
	}
}

// ---------------------------------------------------------------------------
// stream_options injection
// ---------------------------------------------------------------------------

func TestInjectStreamUsage(t *testing.T) {
	cases := []struct {
		name string
		// want is the expected value of stream_options.include_usage:
		// true = we injected it, false = caller's value left alone,
		// nil = stream_options must not appear at all.
		format string
		body   string
		want   interface{}
	}{
		{"streaming chat", "openai-chat", `{"model":"gpt-4o","stream":true}`, true},
		{"non streaming", "openai-chat", `{"model":"gpt-4o","stream":false}`, nil},
		{"caller opted out", "openai-chat", `{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":false}}`, false},
		{"generic format", "generic", `{"stream":true}`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(c.body))
			out := injectStreamUsage(req, &Route{APIFormat: c.format}, []byte(c.body))
			var m map[string]interface{}
			if err := json.Unmarshal(out, &m); err != nil {
				t.Fatalf("bad json out: %v", err)
			}
			var got interface{}
			if so, ok := m["stream_options"].(map[string]interface{}); ok {
				got = so["include_usage"]
			}
			if got != c.want {
				t.Errorf("stream_options.include_usage = %v, want %v (body %s)", got, c.want, out)
			}
			// The r.Body handed to the upstream must match what we return.
			b, _ := io.ReadAll(req.Body)
			if string(b) != string(out) {
				t.Errorf("r.Body = %s, want %s: injected body must reach upstream", b, out)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// peekBody
// ---------------------------------------------------------------------------

func TestPeekBodyRestoresSmallBody(t *testing.T) {
	payload := `{"model":"gpt-4o","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(payload))
	raw, oversized, err := peekBody(req, 1024)
	if err != nil || oversized {
		t.Fatalf("peekBody = (%q,%v,%v)", raw, oversized, err)
	}
	if string(raw) != payload {
		t.Errorf("raw = %q, want %q", raw, payload)
	}
	again, _ := io.ReadAll(req.Body)
	if string(again) != payload {
		t.Errorf("restored body = %q, want the original %q", again, payload)
	}
}

// A body bigger than the cap must still reach upstream byte-for-byte; it just
// cannot be replayed. The cap only bounds what is *inspected*, so the peeked
// prefix is handed back too — discarding it is what made every large request
// look like it carried no model.
func TestPeekBodyOversizedStillStreamsWholeBody(t *testing.T) {
	payload := strings.Repeat("x", 3000)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(payload))
	raw, oversized, err := peekBody(req, 1024)
	if err != nil {
		t.Fatalf("peekBody err: %v", err)
	}
	if !oversized {
		t.Fatal("oversized = false, want true for a 3000-byte body with a 1024 cap")
	}
	if len(raw) > 1024 {
		t.Errorf("raw = %d bytes, want at most the 1024-byte cap (only the head is buffered)", len(raw))
	}
	again, _ := io.ReadAll(req.Body)
	if string(again) != payload {
		t.Errorf("restored %d bytes, want %d: oversized bodies must not be truncated", len(again), len(payload))
	}
}

// ---------------------------------------------------------------------------
// key cache encoding
// ---------------------------------------------------------------------------

func TestDecodeKeyInfoAcceptsLegacyBareID(t *testing.T) {
	ki, ok := store.DecodeKeyInfo("42")
	if !ok || ki.ID != 42 {
		t.Errorf("DecodeKeyInfo(\"42\") = (%+v,%v), want id 42", ki, ok)
	}
	ki, ok = store.DecodeKeyInfo(`{"id":7,"exp":1,"ql":5,"ips":["1.2.3.4"],"models":["gpt-4o"]}`)
	if !ok || ki.ID != 7 || ki.ExpiresAt != 1 || ki.QuotaLimit != 5 ||
		len(ki.AllowedIPs) != 1 || len(ki.AllowedModels) != 1 {
		t.Errorf("DecodeKeyInfo(json) = %+v, want the full policy", ki)
	}
	if _, ok := store.DecodeKeyInfo(""); ok {
		t.Error("empty value should not decode")
	}
}

func TestKeyInfoIPAllowed(t *testing.T) {
	cases := []struct {
		rules []string
		ip    string
		want  bool
	}{
		{nil, "1.2.3.4", true},
		{[]string{"1.2.3.4"}, "1.2.3.4", true},
		{[]string{"1.2.3.4"}, "1.2.3.5", false},
		{[]string{"10.0.0.0/8"}, "10.9.9.9", true},
		{[]string{"10.0.0.0/8"}, "11.0.0.1", false},
		{[]string{"bogus"}, "1.2.3.4", false}, // a typo must block, not open
	}
	for _, c := range cases {
		ki := store.KeyInfo{AllowedIPs: c.rules}
		if got := ki.IPAllowed(c.ip); got != c.want {
			t.Errorf("IPAllowed(%q) with %v = %v, want %v", c.ip, c.rules, got, c.want)
		}
	}
}

// noLogServer builds a one-route gateway whose audit sink is a buffered
// channel, so tests can assert on what did and did not get recorded. It hands
// back its Redis client too: seedKey and QuotaUsed have to target the same
// instance the middleware reads from.
func noLogServer(t *testing.T, up *httptest.Server) (*Server, *redis.Client, chan store.LogEntry, *httptest.Server) {
	t.Helper()
	rdb := testRedis(t)
	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		Name: "nl", BaseURL: up.URL, MatchPrefix: "/nl", Upstream: "nl",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}
	audited := make(chan store.LogEntry, 8)
	s.Auditor = audited
	ts := httptest.NewServer(testChain(s))
	t.Cleanup(ts.Close)
	return s, rdb, audited, ts
}

// TestNoLogKeySuppressedFromAuditButStillBilled pins the subtle half of the
// no_log flag: it silences the record-keeping, not the billing.
//
// Smoke-test keys exist to provoke failures, so their 403/429/400 must stay
// out of the request log. But the e2e suite asserts that a key runs out of
// quota — if no_log also waived the charge, that assertion would pass for the
// wrong reason (nothing was ever counted) or fail outright.
func TestNoLogKeySuppressedFromAuditButStillBilled(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	_, rdb, audited, ts := noLogServer(t, up)

	raw := "gw-nolog-test"
	id := seedKey(t, rdb, raw, store.KeyInfo{NoLog: true})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{"ping":1}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	select {
	case e := <-audited:
		t.Errorf("no_log key was audited (%s %s -> %d): the flag is meant to keep "+
			"self-inflicted test traffic out of the request log", e.Method, e.Path, e.StatusCode)
	default:
	}

	// Silence is not immunity.
	if used := store.QuotaUsed(context.Background(), rdb, nil, id); used <= 0 {
		t.Errorf("quota_used = %d, want > 0: a no_log key must still be charged, or the "+
			"e2e quota assertion would pass while counting nothing", used)
	}
}

// TestNormalKeyStillAudited is the control for the test above. Without this
// half, making every key silent would still leave the suite green.
func TestNormalKeyStillAudited(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	_, rdb, audited, ts := noLogServer(t, up)

	raw := "gw-logged-test"
	seedKey(t, rdb, raw, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{"ping":1}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	select {
	case e := <-audited:
		if e.StatusCode != http.StatusOK {
			t.Errorf("audited status = %d, want 200", e.StatusCode)
		}
	default:
		t.Error("an ordinary key was NOT audited - no_log must stay opt-in")
	}
}

// TestKeylessRequestStillAudited guards the boundary: a request with no
// credential never populates KeyInfo, so it cannot be marked silent and must
// stay in the log. Credential failures are exactly what an operator needs to
// see, so a broader "silent" rule would quietly blind the console.
func TestKeylessRequestStillAudited(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	_, _, audited, ts := noLogServer(t, up)

	resp, err := http.Post(ts.URL+"/nl", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}

	select {
	case e := <-audited:
		if e.StatusCode != http.StatusUnauthorized {
			t.Errorf("audited status = %d, want 401", e.StatusCode)
		}
	default:
		t.Error("a keyless request was NOT audited - auth failures must always be recorded")
	}
}

// TestNoLogKeyRejectedByPolicyStaysSilent covers an ordering bug: mwAuth used
// to enforce expiry and the IP allowlist before recording the caller's
// identity, so mwLogging could not tell the key was flagged and the rejection
// landed in the log. The e2e suite provokes both of those on purpose, which
// made them the noisiest rows of all.
func TestNoLogKeyRejectedByPolicyStaysSilent(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	_, rdb, audited, ts := noLogServer(t, up)

	// A key usable from nowhere: its own allowlist excludes every address, so
	// the request is refused by policy rather than by authentication.
	raw := "gw-nolog-rejected"
	seedKey(t, rdb, raw, store.KeyInfo{
		NoLog:      true,
		AllowedIPs: []string{"10.99.99.99"},
	})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (the key must actually be rejected)", resp.StatusCode)
	}

	select {
	case e := <-audited:
		t.Errorf("a no_log key refused by its own policy was audited (%s %s -> %d): "+
			"mwAuth has to record the identity before enforcing it", e.Method, e.Path, e.StatusCode)
	default:
	}
}

// TestNormalKeyRejectedByPolicyIsAudited is the control for the above: the very
// same rejection with an unflagged key must still be recorded, or operators
// would lose sight of blocked access attempts.
func TestNormalKeyRejectedByPolicyIsAudited(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	_, rdb, audited, ts := noLogServer(t, up)

	raw := "gw-logged-rejected"
	seedKey(t, rdb, raw, store.KeyInfo{AllowedIPs: []string{"10.99.99.99"}})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}

	select {
	case e := <-audited:
		if e.StatusCode != http.StatusForbidden {
			t.Errorf("audited status = %d, want 403", e.StatusCode)
		}
	default:
		t.Error("a blocked ordinary key was NOT audited - only flagged keys may go unrecorded")
	}
}
