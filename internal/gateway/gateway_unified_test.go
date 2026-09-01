package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// upstreamRecorder is an upstream that records the path it was called with.
type upstreamRecorder struct {
	name     string
	srv      *httptest.Server
	paths    atomic.Value // []string
	requests atomic.Int64
}

func newUpstream(t *testing.T, name string, status int, body string) *upstreamRecorder {
	t.Helper()
	u := &upstreamRecorder{name: name}
	u.paths.Store([]string{})
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.requests.Add(1)
		u.paths.Store(append(u.paths.Load().([]string), r.URL.Path))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"served_by":%q}`, name)))
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *upstreamRecorder) seen() []string { return u.paths.Load().([]string) }

// newUnifiedServer builds a gateway with two providers behind the /v1 entry.
func newUnifiedServer(t *testing.T, prefix string, a, b *upstreamRecorder) *httptest.Server {
	t.Helper()
	s := &Server{Cfg: &config.Config{
		UpstreamTimeoutSec: 10,
		MaxAttempts:        1,
		UnifiedPrefix:      prefix,
	}}
	s.Routes = []Route{
		{
			ID: 1, Name: "provider-a", BaseURL: a.srv.URL, MatchPrefix: "/provider-a",
			Upstream: "provider-a", APIFormat: "openai-chat", UpstreamRPS: 1000,
			Models: []string{"model-a"},
		},
		{
			ID: 2, Name: "provider-b", BaseURL: b.srv.URL, MatchPrefix: "/provider-b",
			Upstream: "provider-b", APIFormat: "openai-chat", UpstreamRPS: 1000,
			Models: []string{"model-b"},
		},
	}
	return httptest.NewServer(http.HandlerFunc(s.proxy))
}

// The whole point of the unified entry: one base_url, provider chosen by the
// model name in the body.
func TestUnifiedEntryRoutesByModel(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	call := func(model string) (int, string) {
		body := fmt.Sprintf(`{"model":%q,"messages":[]}`, model)
		resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	code, body := call("model-a")
	if code != http.StatusOK || !strings.Contains(body, `"served_by":"a"`) {
		t.Errorf("model-a -> %d %s, want 200 from provider a", code, body)
	}
	code, body = call("model-b")
	if code != http.StatusOK || !strings.Contains(body, `"served_by":"b"`) {
		t.Errorf("model-b -> %d %s, want 200 from provider b", code, body)
	}
	if a.requests.Load() != 1 || b.requests.Load() != 1 {
		t.Errorf("requests a=%d b=%d, want 1 1", a.requests.Load(), b.requests.Load())
	}
}

// The /v1 is a virtual version number: it must NOT be forwarded, otherwise the
// upstream sees /v1/chat/completions and 404s (the bug this entry exists to fix).
func TestUnifiedEntryStripsVirtualPrefix(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"model-a","messages":[]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	seen := a.seen()
	if len(seen) != 1 {
		t.Fatalf("upstream called %d times, want 1", len(seen))
	}
	if seen[0] != "/chat/completions" {
		t.Errorf("upstream received %q, want %q - the virtual version prefix must be stripped",
			seen[0], "/chat/completions")
	}
}

func TestUnifiedEntryUnknownModelListsAvailable(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"nope","messages":[]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if !strings.Contains(string(body), "model-a") || !strings.Contains(string(body), "model-b") {
		t.Errorf("404 body %q should list the available models so the client can recover", body)
	}
	if a.requests.Load() != 0 || b.requests.Load() != 0 {
		t.Error("unknown model must not be forwarded upstream")
	}
}

func TestUnifiedEntryMissingModelRejected(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (cannot route without a model)", resp.StatusCode)
	}
}

// Clients probe GET /v1/models on connect; it must be answered by the gateway
// and must aggregate every provider, not just one.
func TestUnifiedEntryListsModels(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/models")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Object != "list" {
		t.Errorf("object = %q, want \"list\"", out.Object)
	}
	var ids []string
	for _, d := range out.Data {
		ids = append(ids, d.ID)
	}
	if len(ids) != 2 || ids[0] != "model-a" || ids[1] != "model-b" {
		t.Errorf("models = %v, want [model-a model-b]", ids)
	}
	// Discovery must not be forwarded to any provider.
	if a.requests.Load() != 0 || b.requests.Load() != 0 {
		t.Error("/v1/models must be answered locally, not forwarded")
	}
}

// The old per-provider prefixes are the escape hatch and must keep working.
func TestPerRoutePrefixStillWorksWithUnifiedEnabled(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/provider-b/chat/completions", "application/json",
		strings.NewReader(`{"model":"model-b","messages":[]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"served_by":"b"`) {
		t.Errorf("per-route prefix -> %d %s, want 200 from b", resp.StatusCode, body)
	}
	// Per-route addressing strips the route prefix, not the virtual version.
	if seen := b.seen(); len(seen) != 1 || seen[0] != "/chat/completions" {
		t.Errorf("upstream saw %v, want [/chat/completions]", seen)
	}
}

// UNIFIED_PREFIX=off must leave the gateway behaving exactly as before.
func TestUnifiedEntryCanBeDisabled(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "off", a, b)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"model-a","messages":[]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 when the unified entry is off", resp.StatusCode)
	}
}

// A key-level model restriction must still apply on the unified entry: routing
// picks the provider, but the key's own permission is checked on top.
func TestUnifiedEntryHonorsKeyModelRestriction(t *testing.T) {
	rdb := testRedis(t)
	a := newUpstream(t, "a", http.StatusOK, "")

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, UnifiedPrefix: "/v1"}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "provider-a", BaseURL: a.srv.URL, MatchPrefix: "/provider-a",
		Upstream: "provider-a", APIFormat: "openai-chat", UpstreamRPS: 1000,
		Models: []string{"model-a", "model-b"},
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	raw := "gw-unified-restricted"
	seedKey(t, rdb, raw, store.KeyInfo{AllowedModels: []string{"model-a"}})

	call := func(model string) int {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
			strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[]}`, model)))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := call("model-a"); got != http.StatusOK {
		t.Errorf("permitted model = %d, want 200", got)
	}
	if got := call("model-b"); got != http.StatusForbidden {
		t.Errorf("model outside the key's allowlist = %d, want 403", got)
	}
}

// resolveTarget unit checks: the path/model combinations that decide routing.
func TestResolveTarget(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	s := &Server{Cfg: &config.Config{UnifiedPrefix: "/v1"}}
	s.Routes = []Route{{
		ID: 1, Name: "provider-a", BaseURL: a.srv.URL, MatchPrefix: "/provider-a",
		Upstream: "provider-a", Models: []string{"model-a"},
	}}

	cases := []struct {
		path, model string
		wantRoute   string
		wantSuffix  string
		wantStatus  int
		wantList    bool
	}{
		{"/v1/chat/completions", "model-a", "provider-a", "/chat/completions", 0, false},
		{"/v1/models", "", "", "", 0, true},
		{"/v1", "", "", "", 0, true},
		{"/v1/chat/completions", "", "", "", http.StatusBadRequest, false},
		{"/v1/chat/completions", "nope", "", "", http.StatusNotFound, false},
		{"/provider-a/chat/completions", "model-a", "provider-a", "/chat/completions", 0, false},
		{"/unknown", "", "", "", http.StatusNotFound, false},
	}
	for _, c := range cases {
		tgt, herr := s.resolveTarget(c.path, c.model)
		if c.wantStatus != 0 {
			if herr == nil || herr.status != c.wantStatus {
				t.Errorf("resolveTarget(%q,%q) err = %v, want status %d", c.path, c.model, herr, c.wantStatus)
			}
			continue
		}
		if herr != nil {
			t.Errorf("resolveTarget(%q,%q) unexpected err: %v", c.path, c.model, herr)
			continue
		}
		if c.wantList {
			if !tgt.listModels {
				t.Errorf("resolveTarget(%q) listModels = false, want true", c.path)
			}
			continue
		}
		if tgt.route == nil || tgt.route.Name != c.wantRoute {
			t.Errorf("resolveTarget(%q,%q) route = %v, want %s", c.path, c.model, tgt.route, c.wantRoute)
			continue
		}
		if tgt.suffix != c.wantSuffix {
			t.Errorf("resolveTarget(%q,%q) suffix = %q, want %q", c.path, c.model, tgt.suffix, c.wantSuffix)
		}
	}
}

// A route with no model allowlist is invisible to the unified entry: letting it
// in would make "which route owns this model" ambiguous.
func TestRouteWithoutAllowlistIsNotReachableViaUnified(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	s := &Server{Cfg: &config.Config{UnifiedPrefix: "/v1"}}
	s.Routes = []Route{{
		ID: 1, Name: "no-models", BaseURL: a.srv.URL, MatchPrefix: "/no-models",
		Upstream: "no-models", APIFormat: "generic",
	}}
	if len(s.availableModels()) != 0 {
		t.Errorf("availableModels = %v, want empty", s.availableModels())
	}
	if _, herr := s.resolveTarget("/v1/chat/completions", "whatever"); herr == nil {
		t.Error("resolveTarget should reject when no route declares models")
	}
}

// The official OpenAI SDK only sends "Authorization: Bearer <key>". Accepting
// it is what makes the unified entry drop-in — but the header must then be
// consumed, or the caller's gateway key gets relayed to the provider.
func TestBearerAuthAcceptedAndStrippedFromUpstream(t *testing.T) {
	rdb := testRedis(t)
	const gwKey = "gw-bearer-auth-test"
	const providerKey = "sk-provider-secret"

	var seenAuth, seenXAPIKey, seenProviderKey string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		seenXAPIKey = r.Header.Get("X-API-Key")
		seenProviderKey = r.Header.Get("X-Provider-Key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, UnifiedPrefix: "/v1"}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "p", BaseURL: up.URL, MatchPrefix: "/p", Upstream: "p",
		APIFormat: "openai-chat", UpstreamRPS: 1000,
		DownstreamAuthKey: providerKey, Models: []string{"m"},
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()
	seedKey(t, rdb, gwKey, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+gwKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (Bearer must authenticate)", resp.StatusCode)
	}
	// Leak check by VALUE, not by header name: http.Header.Get is
	// case-insensitive, so a renamed header would slip past a name check.
	for name, vals := range map[string][]string{
		"Authorization":  {seenAuth},
		"X-API-Key":      {seenXAPIKey},
		"X-Provider-Key": {seenProviderKey},
	} {
		for _, v := range vals {
			if v == gwKey {
				t.Errorf("gateway key leaked upstream via %s", name)
			}
		}
	}
	if seenAuth != "Bearer "+providerKey {
		t.Errorf("upstream Authorization = %q, want the provider key %q", seenAuth, "Bearer "+providerKey)
	}
}

// X-API-Key must keep working exactly as before - it is the documented spelling.
func TestXAPIKeyAuthStillWorksAndIsStripped(t *testing.T) {
	rdb := testRedis(t)
	const gwKey = "gw-xapikey-auth-test"

	var seenXAPIKey, seenAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenXAPIKey = r.Header.Get("X-API-Key")
		seenAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, UnifiedPrefix: "/v1"}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "p", BaseURL: up.URL, MatchPrefix: "/p", Upstream: "p",
		APIFormat: "generic", UpstreamRPS: 1000, Models: []string{"m"},
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()
	seedKey(t, rdb, gwKey, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m"}`))
	req.Header.Set("X-API-Key", gwKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if seenXAPIKey != "" {
		t.Errorf("X-API-Key reached upstream as %q, want empty", seenXAPIKey)
	}
	if seenAuth != "" {
		t.Errorf("Authorization reached upstream as %q, want empty", seenAuth)
	}
}

func TestExtractGatewayKeyPrefersXAPIKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	r.Header.Set("X-API-Key", "from-x-api-key")
	r.Header.Set("Authorization", "Bearer from-bearer")
	if k, w := extractGatewayKey(r); k != "from-x-api-key" || w != authXAPIKey {
		t.Errorf("extractGatewayKey = (%q,%v), want the X-API-Key value", k, w)
	}

	r2 := httptest.NewRequest(http.MethodPost, "/x", nil)
	r2.Header.Set("Authorization", "Bearer from-bearer")
	if k, w := extractGatewayKey(r2); k != "from-bearer" || w != authBearer {
		t.Errorf("extractGatewayKey = (%q,%v), want the Bearer value", k, w)
	}

	r3 := httptest.NewRequest(http.MethodPost, "/x", nil)
	if k, w := extractGatewayKey(r3); k != "" || w != authNone {
		t.Errorf("extractGatewayKey = (%q,%v), want none", k, w)
	}

	// A non-Bearer Authorization (e.g. Basic) is not a gateway credential.
	r4 := httptest.NewRequest(http.MethodPost, "/x", nil)
	r4.Header.Set("Authorization", "Basic abc")
	if _, w := extractGatewayKey(r4); w != authNone {
		t.Errorf("Basic auth should not be treated as a gateway key")
	}
}

// The credential header is stripped twice on purpose: once in mwAuth (so no
// later refactor can accidentally forward it) and once in forward (so a
// hand-built request that skips mwAuth is still safe). This test pins the mwAuth
// half specifically - the upstream-visible half is covered above.
func TestAuthConsumesCredentialHeader(t *testing.T) {
	rdb := testRedis(t)
	const gwKey = "gw-consume-header-test"
	s := &Server{Cfg: &config.Config{}, RDB: rdb}
	seedKey(t, rdb, gwKey, store.KeyInfo{})

	run := func(set func(*http.Request)) (auth, xkey string, code int) {
		h := s.mwAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth = r.Header.Get("Authorization")
			xkey = r.Header.Get("X-API-Key")
			w.WriteHeader(http.StatusOK)
		}))
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{}`))
		set(req)
		ctx := withMeta(req.Context(), &meta{})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req.WithContext(ctx))
		return auth, xkey, rec.Code
	}

	_, xkey, code := run(func(r *http.Request) { r.Header.Set("X-API-Key", gwKey) })
	if code != http.StatusOK {
		t.Fatalf("X-API-Key auth status = %d, want 200", code)
	}
	if xkey != "" {
		t.Errorf("X-API-Key still on the request after auth (%q) - mwAuth must consume it", xkey)
	}

	auth, _, code := run(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+gwKey) })
	if code != http.StatusOK {
		t.Fatalf("Bearer auth status = %d, want 200", code)
	}
	if auth != "" {
		t.Errorf("Authorization still on the request after auth (%q) - mwAuth must consume it", auth)
	}
	if strings.Contains(auth, gwKey) || strings.Contains(xkey, gwKey) {
		t.Error("gateway key survived authentication")
	}

	// Several clients send both spellings at once. Deleting only the one that
	// authenticated left the other in place, and that one rode upstream: the
	// provider was handed the caller's gateway key and answered
	// 401 Invalid API key, because injectCredentials skips a slot that is
	// already occupied. The gateway meanwhile had authenticated the very same
	// call, so the log showed a healthy request failing at the provider.
	bothAuth, bothXKey, bothCode := run(func(r *http.Request) {
		r.Header.Set("X-API-Key", gwKey)
		r.Header.Set("Authorization", "Bearer "+gwKey)
	})
	if bothCode != http.StatusOK {
		t.Fatalf("both-header auth status = %d, want 200", bothCode)
	}
	if bothAuth != "" || bothXKey != "" {
		t.Errorf("both headers set -> Authorization=%q X-API-Key=%q, want both consumed", bothAuth, bothXKey)
	}
}

// Two replicas sharing one Postgres and one Redis. A route change applied
// through replica A must eventually appear in replica B's in-memory table —
// that is the whole point of the shared revision counter.
func TestRouteReloaderPicksUpPeerChanges(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	ctx := context.Background()

	// Start from a clean revision counter, i.e. a brand-new deployment. Whether
	// the key happens to exist in a shared Redis decides whether "unknown ->
	// known" is even exercised, so leaving it to chance makes this test pass
	// even when that branch is broken.
	rdb.Del(ctx, "routes:version")
	t.Cleanup(func() { rdb.Del(ctx, "routes:version") })

	const routeName = "reload-sync-test"
	// Seed the route directly in SQL, the way an operator's change would land.
	var routeID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO routes (name, base_url, match_path, api_format, models, status)
		VALUES ($1,'https://example.invalid','/reload-sync-test','openai-chat','["sync-model"]',1)
		RETURNING id`, routeName).Scan(&routeID); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM channels WHERE route_id=$1`, routeID)
		db.ExecContext(ctx, `DELETE FROM routes WHERE id=$1`, routeID)
	})

	// Replica A applies an admin change; replica B only watches.
	a := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db}
	b := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db, Routes: []Route{}}
	stop := b.StartRouteReloader(20*time.Millisecond, 0)
	defer stop()

	if b.routeCount() != 0 {
		t.Fatalf("replica B started with %d routes, want 0", b.routeCount())
	}
	a.reloadRoutes() // admin change on A -> bumps the shared revision

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b.routeCount() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(fmt.Sprint(b.availableModels()), "sync-model") {
		t.Fatalf("replica B never picked up the change; models = %v", b.availableModels())
	}

	// No feedback loop: B reloaded via applyRoutes, which must NOT bump the
	// revision. If it did, replicas would reload each other forever.
	time.Sleep(150 * time.Millisecond)
	v1 := store.RouteVersion(ctx, rdb)
	time.Sleep(300 * time.Millisecond)
	if v2 := store.RouteVersion(ctx, rdb); v1 != v2 {
		t.Errorf("revision kept moving on its own (%d -> %d): replicas are ping-ponging", v1, v2)
	}
}

// The bug this guards: rate limiting used to be per-API-key only, so two keys
// each got their own bucket sized upstream_rps and the provider saw the SUM.
// upstream_rps is supposed to be a ceiling on the provider, not a per-key
// allowance, so the total across keys must stay inside it.
func TestUpstreamRateLimitCapsTotalAcrossKeys(t *testing.T) {
	rdb := testRedis(t)
	up := newUpstream(t, "u", http.StatusOK, "")
	const dim = "ratelimit-global-test"

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "u", BaseURL: up.srv.URL, MatchPrefix: "/u", Upstream: dim,
		APIFormat: "openai-chat", UpstreamRPS: 2, Models: []string{"m"},
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	// Buckets live an hour in Redis; a leftover from another test would skew
	// the count, so start from empty.
	rdb.Del(context.Background(), "bucket:up:"+dim)

	k1 := "gw-ratelimit-key-1"
	k2 := "gw-ratelimit-key-2"
	id1 := seedKey(t, rdb, k1, store.KeyInfo{})
	id2 := seedKey(t, rdb, k2, store.KeyInfo{})
	rdb.Del(context.Background(), "bucket:key:"+itoa(id1)+":"+dim)
	rdb.Del(context.Background(), "bucket:key:"+itoa(id2)+":"+dim)

	allowed := 0
	for i := 0; i < 12; i++ {
		key := k1
		if i%2 == 1 {
			key = k2
		}
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/u/chat/completions",
			strings.NewReader(`{"model":"m","messages":[]}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			allowed++
		}
	}
	// Capacity is rps*2 = 4 for the shared upstream bucket. Two independent
	// per-key buckets would have let 8 through. Allow a couple for refill
	// during the loop, but 8+ means the global dimension is not applied.
	if allowed > 6 {
		t.Errorf("allowed %d/12 across two keys, want <= 6: the upstream cap must be shared, not per key", allowed)
	}
	if allowed == 0 {
		t.Error("nothing got through - the limiter is blocking everything")
	}
}

// Third dimension: per client IP. Off by default (IP_RPS=0) because everything
// behind one NAT would share it, but it should work when enabled.
func TestIPRateLimitDimension(t *testing.T) {
	rdb := testRedis(t)
	up := newUpstream(t, "u", http.StatusOK, "")
	const dim = "ratelimit-ip-test"

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, IPRPS: 1}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "u", BaseURL: up.srv.URL, MatchPrefix: "/u", Upstream: dim,
		APIFormat: "openai-chat", UpstreamRPS: 1000, Models: []string{"m"},
	}}
	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	key := "gw-ratelimit-ip-key"
	id := seedKey(t, rdb, key, store.KeyInfo{})
	rdb.Del(context.Background(), "bucket:up:"+dim)
	rdb.Del(context.Background(), "bucket:key:"+itoa(id)+":"+dim)
	// The IP bucket is derived from the real remote address of the test client.
	rdb.Del(context.Background(), "bucket:ip:127.0.0.1")

	var blocked int
	for i := 0; i < 6; i++ {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/u/chat/completions",
			strings.NewReader(`{"model":"m","messages":[]}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			blocked++
		}
	}
	// IP_RPS=1 -> capacity 2. Six rapid calls from one address must be throttled
	// even though the key's own bucket is wide open.
	if blocked == 0 {
		t.Error("per-IP limit never triggered; the third dimension is not being checked")
	}
}

// chainWithCache adds the cache layer that testChain leaves out. Kept separate
// because a shared cache would make the other tests order-dependent.
func chainWithCache(s *Server) http.Handler {
	var h http.Handler = http.HandlerFunc(s.proxy)
	h = s.mwCache(h)
	h = s.mwRateLimit(h)
	h = s.mwQuota(h)
	h = s.mwAuth(h)
	h = s.mwLogging(h)
	return h
}

// Two keys hitting the same GET endpoint. With the default "global" scope the
// second caller is served the first caller's cached response; with "key" scope
// each caller gets a private entry and the second one reaches the upstream.
func TestCacheScopeGlobalVsKey(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()

	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"call":%d}`, calls.Add(1))
	}))
	defer up.Close()

	newSrv := func(scope string) *httptest.Server {
		s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb}
		s.Routes = []Route{{
			ID: 1, Name: "c", BaseURL: up.URL, MatchPrefix: "/c", Upstream: "cache-" + scope,
			APIFormat: "generic", UpstreamRPS: 1000, CacheTTL: 60, CacheScope: scope,
		}}
		return httptest.NewServer(chainWithCache(s))
	}

	// Unique path per scope so the two cases do not share a Redis key.
	for _, tc := range []struct {
		scope     string
		path      string
		wantCache string // expected X-Cache on the SECOND caller
	}{
		{"global", "/c/global", "HIT"},
		{"key", "/c/perkey", "MISS"},
	} {
		ts := newSrv(tc.scope)
		rdb.Del(ctx, "cache:"+hashOf("GET"+tc.path+"?"))
		k1 := "gw-cache-a"
		k2 := "gw-cache-b"
		id1 := seedKey(t, rdb, k1, store.KeyInfo{})
		id2 := seedKey(t, rdb, k2, store.KeyInfo{})
		rdb.Del(ctx, "cache:key"+itoa(id1)+":"+hashOf("GET"+tc.path+"?"))
		rdb.Del(ctx, "cache:key"+itoa(id2)+":"+hashOf("GET"+tc.path+"?"))

		get := func(key string) string {
			req, _ := http.NewRequest(http.MethodGet, ts.URL+tc.path, nil)
			req.Header.Set("X-API-Key", key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			return resp.Header.Get("X-Cache")
		}

		if got := get(k1); got != "MISS" {
			t.Fatalf("[%s] first caller X-Cache = %q, want MISS", tc.scope, got)
		}
		if got := get(k2); got != tc.wantCache {
			t.Errorf("[%s] second caller X-Cache = %q, want %s", tc.scope, got, tc.wantCache)
		}
		ts.Close()
	}
}

// The model listing must reflect what the calling key is allowed to use, and
// must never be served from a shared cache entry.
func TestModelListHonoursKeyRestrictionAndIsNotCached(t *testing.T) {
	rdb := testRedis(t)
	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10, UnifiedPrefix: "/v1"}, RDB: rdb}
	s.Routes = []Route{{
		ID: 1, Name: "p", BaseURL: "https://example.invalid", MatchPrefix: "/p",
		Upstream: "p", APIFormat: "openai-chat", UpstreamRPS: 1000,
		Models: []string{"alpha", "beta"},
	}}
	ts := httptest.NewServer(chainWithCache(s))
	defer ts.Close()

	free := "gw-models-free"
	limited := "gw-models-limited"
	seedKey(t, rdb, free, store.KeyInfo{})
	seedKey(t, rdb, limited, store.KeyInfo{AllowedModels: []string{"beta"}})

	list := func(key string) []string {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		var out struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		var ids []string
		for _, d := range out.Data {
			ids = append(ids, d.ID)
		}
		return ids
	}

	if got := list(free); len(got) != 2 {
		t.Errorf("unrestricted key saw %v, want both models", got)
	}
	if got := list(limited); len(got) != 1 || got[0] != "beta" {
		t.Errorf("restricted key saw %v, want only [beta]", got)
	}
	// Second call through the cache layer must still be restricted.
	if got := list(limited); len(got) != 1 || got[0] != "beta" {
		t.Errorf("restricted key saw %v on the cached call, want only [beta]", got)
	}
}

// Scope normalisation is what stops a garbage value from silently choosing a
// behaviour. "key" is the sensitive one (per-key entries), so anything
// unrecognised must fall back to the shared default rather than to it.
func TestNormaliseCacheScope(t *testing.T) {
	cases := []struct{ in, want string }{
		{"key", "key"},
		{"KEY", "key"},
		{" key ", "key"},
		{"global", "global"},
		{"", "global"},
		{"banana", "global"},
		{"per-user", "global"},
	}
	for _, c := range cases {
		if got := normaliseCacheScope(c.in); got != c.want {
			t.Errorf("normaliseCacheScope(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// A partial update must be able to say "leave the stored value alone" —
	// which is why blank stays blank here, and only here.
	if got := normaliseScopeOrBlank(""); got != "" {
		t.Errorf("normaliseScopeOrBlank(\"\") = %q, want \"\"", got)
	}
	if got := normaliseScopeOrBlank("bogus"); got != "global" {
		t.Errorf("normaliseScopeOrBlank(\"bogus\") = %q, want global", got)
	}
}

// A malformed body must be rejected, not ignored. These payloads carry
// restrictions, so falling back to defaults would issue a key that is more
// permissive than the caller asked for — and they would never know.
func TestMalformedKeyPayloadIsRejected(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db}

	srv := httptest.NewServer(http.HandlerFunc(s.adminKeys))
	defer srv.Close()

	// Keys issued by the 200 case are real rows; remove them afterwards.
	issued := []int64{}
	defer func() {
		for _, id := range issued {
			db.ExecContext(context.Background(), `DELETE FROM api_keys WHERE id=$1`, id)
		}
	}()

	post := func(body string) int {
		resp, err := http.Post(srv.URL+"/admin/keys", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var out struct {
				ID int64 `json:"id"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			if out.ID != 0 {
				issued = append(issued, out.ID)
			}
		}
		return resp.StatusCode
	}

	// allowed_ips sent as a string instead of an array is the mistake this
	// catches: it used to decode to a nil slice and issue an unrestricted key.
	if got := post(`{"owner":"x","allowed_ips":"[\"10.0.0.1\"]"}`); got != http.StatusBadRequest {
		t.Errorf("allowed_ips as string -> %d, want 400", got)
	}
	if got := post(`{"owner":"x","quota_limit":"three"}`); got != http.StatusBadRequest {
		t.Errorf("quota_limit as string -> %d, want 400", got)
	}
	if got := post(`{not json`); got != http.StatusBadRequest {
		t.Errorf("garbage -> %d, want 400", got)
	}
	// An empty body stays valid: the console issues keys with no options.
	if got := post(``); got != http.StatusOK {
		t.Errorf("empty body -> %d, want 200 (defaults are legitimate)", got)
	}
}

// A key's quota ceiling has three different requests that all used to reach
// the server looking identical: "no ceiling", "100000 please" and "I did not
// say". Only the middle one can be expressed without thinking about it.
//
// The bug: QuotaLimit was a plain int64, so a JSON 0 and an absent field both
// decoded to zero, and CreateAPIKey rewrote both to the default (1M). There
// was no way to issue an uncapped key at all, even though mwQuota, the console
// and the Prometheus gauge had long agreed that <=0 means unlimited.
func TestKeyQuotaLimitAtIssuance(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db}

	srv := httptest.NewServer(http.HandlerFunc(s.adminKeys))
	defer srv.Close()

	ctx := context.Background()
	issued := []int64{}
	defer func() {
		for _, id := range issued {
			db.ExecContext(ctx, `DELETE FROM api_keys WHERE id=$1`, id)
		}
	}()

	issue := func(t *testing.T, body string) int64 {
		t.Helper()
		resp, err := http.Post(srv.URL+"/admin/keys", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("issue %s -> %d, want 200", body, resp.StatusCode)
		}
		var out struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.ID == 0 {
			t.Fatalf("issue %s: no id in response", body)
		}
		issued = append(issued, out.ID)
		return out.ID
	}

	stored := func(t *testing.T, id int64) int64 {
		t.Helper()
		var got int64
		if err := db.QueryRowContext(ctx,
			`SELECT quota_limit FROM api_keys WHERE id=$1`, id).Scan(&got); err != nil {
			t.Fatalf("read back key %d: %v", id, err)
		}
		return got
	}

	cases := []struct {
		name string
		body string
		want int64
	}{
		// The point of the change: an explicit zero has to survive as zero
		// instead of being rewritten into a 100k ceiling.
		{"explicit zero is unlimited", `{"owner":"q","quota_limit":0}`, 0},
		// A negative ceiling is someone asking for "no ceiling", not for an
		// error, so it folds to the same sentinel.
		{"negative folds to unlimited", `{"owner":"q","quota_limit":-1}`, 0},
		// An absent field keeps the historical default, so every caller that
		// does not care about quota keeps getting the key it always got.
		{"absent keeps the default", `{"owner":"q"}`, 1000000},
		{"an explicit ceiling is stored", `{"owner":"q","quota_limit":5000}`, 5000},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := issue(t, tc.body)
			if got := stored(t, id); got != tc.want {
				t.Errorf("stored quota_limit = %d, want %d", got, tc.want)
			}
		})
	}

	// Raising the ceiling on a key that already exists, in both directions.
	// The console's 改配额 action sends exactly this payload.
	t.Run("a capped key can be made unlimited and back", func(t *testing.T) {
		id := issue(t, `{"owner":"q","quota_limit":5000}`)

		setQuota := func(limit int64) {
			t.Helper()
			req, err := http.NewRequest(http.MethodPut,
				fmt.Sprintf("%s/admin/keys?id=%d", srv.URL, id),
				strings.NewReader(fmt.Sprintf(`{"quota_limit":%d}`, limit)))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("put: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("put quota_limit=%d -> %d, want 200", limit, resp.StatusCode)
			}
		}

		setQuota(0)
		if got := stored(t, id); got != 0 {
			t.Errorf("after PUT 0: stored quota_limit = %d, want 0 (unlimited)", got)
		}
		// Lifting a limit is easy to get right; putting one back is the
		// direction where a COALESCE that treats 0 as "unset" would silently
		// leave the key uncapped.
		setQuota(8000)
		if got := stored(t, id); got != 8000 {
			t.Errorf("after PUT 8000: stored quota_limit = %d, want 8000", got)
		}
	})
}

// A 429 in the request log used to be unreadable. Raising a quota and backing
// off a rate limit are unrelated fixes, and the status code cannot tell you
// which one applies — which is exactly how "am I being throttled?" turned
// into a half-hour of digging through middleware.
//
// This pins the reason onto the row itself, for two gates that share no
// remedy: an exhausted allowance (quota) and an unroutable path (no_route).
func TestRejectionReasonReachesTheLogRow(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	up := echoUpstream()
	defer up.Close()

	ctx := context.Background()
	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 10}, RDB: rdb, DB: db}
	s.Routes = []Route{{
		Name: "q", BaseURL: up.URL, MatchPrefix: "/q", Upstream: "q",
		APIFormat: "generic", UpstreamRPS: 1000,
	}}

	ch, stopAudit := store.StartAuditor(db, 32)
	defer stopAudit()
	s.Auditor = ch

	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	// Two keys, because a key that has spent its allowance can no longer
	// reach the routing stage — mwQuota runs first, so every later rejection
	// on it would be reported as "quota" regardless of the real cause.
	capped := "gw-reject-capped"
	open := "gw-reject-open"
	cappedID := seedKey(t, rdb, capped, store.KeyInfo{QuotaLimit: 1})
	openID := seedKey(t, rdb, open, store.KeyInfo{QuotaLimit: 0})
	defer func() {
		db.ExecContext(ctx, `DELETE FROM request_logs WHERE api_key_id IN ($1,$2)`, cappedID, openID)
	}()

	call := func(key, path string) int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(`{"a":1}`))
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// Spend the single unit of allowance, then ask for one more.
	if got := call(capped, "/q"); got != http.StatusOK {
		t.Fatalf("first call = %d, want 200", got)
	}
	if got := call(capped, "/q"); got != http.StatusTooManyRequests {
		t.Fatalf("call past the limit = %d, want 429", got)
	}
	// A path no route claims, on a key that is not out of allowance.
	if got := call(open, "/nope"); got != http.StatusNotFound {
		t.Fatalf("unroutable call = %d, want 404", got)
	}

	// The auditor writes on its own goroutine and now in batches, so poll
	// rather than sleep once. Every wait is scoped to this test's own keys:
	// an unscoped query can be satisfied by a row an earlier run left
	// behind, which reads as a pass right up until the assertion that
	// checks whose key the row actually names.
	waitRow := func(keyID int64, cond string) {
		t.Helper()
		for i := 0; i < 200; i++ {
			var n int
			err := db.QueryRowContext(ctx,
				`SELECT count(*) FROM request_logs WHERE api_key_id=$1 AND `+cond, keyID).Scan(&n)
			if err == nil && n > 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("no request_logs row for key %d matching %s", keyID, cond)
	}

	waitRow(cappedID, "reject_reason='quota'")
	waitRow(openID, "reject_reason='no_route'")
	// Wait for the served row too, or the check below would pass against an
	// empty table instead of against the request it claims to inspect.
	waitRow(cappedID, "status_code=200")

	// A rejection must not be the only thing recorded: the row still has to
	// name the key, or the reason is unusable.
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM request_logs
		WHERE api_key_id=$1 AND reject_reason='quota'`, cappedID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n == 0 {
		t.Errorf("no quota rejection attributed to key %d", cappedID)
	}

	// And a request that reached an upstream must stay blank: an empty reason
	// is a positive statement, not missing data.
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM request_logs
		WHERE api_key_id=$1 AND status_code=200 AND COALESCE(reject_reason,'')<>''`,
		cappedID).Scan(&n); err != nil {
		t.Fatalf("count served: %v", err)
	}
	if n != 0 {
		t.Errorf("%d successful rows carry a reject_reason, want 0", n)
	}
}

// A partial PUT must not reset fields it did not mention.
//
// The bug: models was a plain []string, so a PUT carrying only cache_scope
// marshalled a nil slice to "null" and stored that — wiping the allowlist and
// quietly turning a restricted route into one that accepts every model. The
// other fields (name, base_url, cache_scope) already treat blank as "keep".
func TestRoutePartialUpdateKeepsModelAllowlist(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	var id int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO routes (name, base_url, match_path, api_format, models, status)
		VALUES ('partial-update-test','https://example.invalid','/partial-update-test',
		        'openai-chat','["keep-me"]',1) RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM channels WHERE route_id=$1`, id)
		db.ExecContext(ctx, `DELETE FROM routes WHERE id=$1`, id)
	})

	s := &Server{Cfg: &config.Config{UnifiedPrefix: "/v1"}, DB: db}
	ts := httptest.NewServer(http.HandlerFunc(s.adminRoutes))
	defer ts.Close()

	put := func(body string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut,
			ts.URL+"/admin/routes?id="+strconv.FormatInt(id, 10), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			buf, _ := io.ReadAll(resp.Body)
			t.Fatalf("PUT %s -> %d (%s), want 200", body, resp.StatusCode, buf)
		}
	}

	stored := func() string {
		var v sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT models FROM routes WHERE id=$1`, id).Scan(&v); err != nil {
			t.Fatalf("read models: %v", err)
		}
		return v.String
	}

	// 1. Only cache_scope -> the allowlist must survive.
	put(`{"cache_scope":"key"}`)
	if got := stored(); got != `["keep-me"]` {
		t.Errorf("models after a cache_scope-only PUT = %q, want [\"keep-me\"]", got)
	}
	// 2. Only a rename -> still survives.
	put(`{"name":"renamed"}`)
	if got := stored(); got != `["keep-me"]` {
		t.Errorf("models after a name-only PUT = %q, want [\"keep-me\"]", got)
	}
	// 3. An explicit empty array means "clear it" and must be honoured.
	put(`{"models":[]}`)
	if got := stored(); got != `[]` {
		t.Errorf("models after an explicit [] = %q, want []", got)
	}
	// 4. Setting a new list must stick.
	put(`{"models":["new-model"]}`)
	if got := stored(); got != `["new-model"]` {
		t.Errorf("models after an explicit list = %q, want [\"new-model\"]", got)
	}
}

func TestUnifiedPrefixNormalisation(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/v1", "/v1"},
		{"/v1/", "/v1"},
		{"off", ""},
		{"", ""},
		{"v1", ""}, // no leading slash -> disabled
	}
	for _, c := range cases {
		s := &Server{Cfg: &config.Config{UnifiedPrefix: c.in}}
		if got := s.unifiedPrefix(); got != c.want {
			t.Errorf("unifiedPrefix(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if (&Server{}).unifiedPrefix() != "" {
		t.Error("nil config must disable the unified entry rather than panic")
	}
}

// TestFaviconServedWithoutAuth proves the browser's icon probe is answered
// without a key instead of being rejected.
//
// Before this was handled, every page load produced a 401: the probe is issued
// by the browser, which has no API key to send.
func TestFaviconServedWithoutAuth(t *testing.T) {
	rdb := testRedis(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/favicon.ico", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /favicon.ico -> %d, want 200 (the probe carries no API key)", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "svg") {
		t.Errorf("Content-Type = %q, want an svg icon", ct)
	}
	if len(rec.Body.Bytes()) == 0 {
		t.Error("icon body is empty")
	}
	// Without this the browser re-probes on every navigation, so the request
	// volume stays high even though it is no longer logged.
	if cc := rec.Header().Get("Cache-Control"); cc == "" {
		t.Error("Cache-Control missing: browsers will re-probe on every navigation")
	}
}

// TestFaviconNotAudited is the one that actually matters for log noise: the
// probe must never reach mwLogging, so it produces no audit row and no
// Prometheus counter. A 200 alone would not prove that.
func TestFaviconNotAudited(t *testing.T) {
	rdb := testRedis(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb}

	// Stands in for the audit writer. Anything the logging middleware sees
	// lands here, so a non-empty receive means the favicon is still inside the
	// instrumented chain.
	audited := make(chan store.LogEntry, 8)
	s.Auditor = audited

	s.Handler().ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/favicon.ico", nil))

	select {
	case e := <-audited:
		t.Errorf("favicon reached the audit log (%s %s -> %d); it must be served "+
			"outside the logging middleware, otherwise every page load is a logged failure",
			e.Method, e.Path, e.StatusCode)
	default:
	}
}

// TestFaviconRouteDoesNotShadowAPI guards the registration itself.
//
// /favicon.ico is an exact-match pattern; the catch-all "/" must still take
// every other path. The contrast with TestFaviconNotAudited is deliberate: a
// keyless API call IS a real failure and has to stay in the log, unlike the
// icon probe. Both halves must hold or the log is either noisy or blind.
func TestFaviconRouteDoesNotShadowAPI(t *testing.T) {
	rdb := testRedis(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb}
	audited := make(chan store.LogEntry, 8)
	s.Auditor = audited

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/models without a key -> %d, want 401: the favicon route "+
			"must not shadow the public chain", rec.Code)
	}
	select {
	case e := <-audited:
		if e.StatusCode != http.StatusUnauthorized {
			t.Errorf("audited status = %d, want 401", e.StatusCode)
		}
	default:
		t.Error("keyless API call was NOT audited - the logging middleware is broken")
	}
}
