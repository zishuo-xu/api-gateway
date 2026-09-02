package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

// The connection test exists because a broken route used to be discoverable
// only by sending real traffic and reading the log afterwards. These tests pin
// the three things that make the probe trustworthy:
//
//   - it sends the credential injectCredentials would send (same precedence)
//   - it surfaces the upstream's own reason, not just the status code
//   - it says so when the config it tested is not the config that is live
//
// If any of those regress the console starts lying, which is worse than having
// no test button at all.

// probeUpstream is a stand-in provider that records what it was sent.
type probeUpstream struct {
	status  int
	body    string
	gotAuth string
	gotKey  string
	gotPath string
	hits    int
}

func (p *probeUpstream) start(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.hits++
		p.gotPath = r.URL.Path
		p.gotAuth = r.Header.Get("Authorization")
		p.gotKey = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(p.status)
		_, _ = io.WriteString(w, p.body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func testProbeServer() *Server {
	return &Server{Cfg: &config.Config{UpstreamTimeoutSec: 5}}
}

// TestRouteTestSendsTheKeyTheForwarderWouldSend pins the probe's credential to
// injectCredentials' precedence: channel first, route only as a fallback.
//
// This is the whole point of the feature. The k3 outage was a stale key on the
// channel while the correct one sat on the route — a probe that averaged the
// two, or preferred the route, would have reported green.
func TestRouteTestSendsTheKeyTheForwarderWouldSend(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	route := &Route{ID: 1, Name: "kimi", APIFormat: "openai-chat",
		DownstreamAuthKey: "sk-route-key-0000000000"}
	ch := Channel{ID: 66, RouteID: 1, Name: "moonshot",
		BaseURL: ts.URL + "/v1", DownstreamAuthKey: "sk-channel-key-111111"}

	res := testProbeServer().probeChannel(context.Background(), route, ch)

	if !res.OK || res.Kind != "ok" {
		t.Fatalf("kind = %q, want ok (summary: %s)", res.Kind, res.Summary)
	}
	if up.gotAuth != "Bearer sk-channel-key-111111" {
		t.Errorf("Authorization = %q, want the channel key — the probe must prefer "+
			"it exactly the way injectCredentials does", up.gotAuth)
	}
	if res.KeySource != "channel" {
		t.Errorf("key_source = %q, want channel", res.KeySource)
	}
	if up.gotPath != "/v1/models" {
		t.Errorf("probe path = %q, want /v1/models", up.gotPath)
	}

	// Without a channel key the route's must be the one that goes out.
	route2 := &Route{ID: 2, Name: "kimi2", APIFormat: "openai-chat",
		DownstreamAuthKey: "sk-route-key-0000000000"}
	ch2 := Channel{ID: 67, RouteID: 2, Name: "moonshot2", BaseURL: ts.URL + "/v1"}
	res2 := testProbeServer().probeChannel(context.Background(), route2, ch2)
	if up.gotAuth != "Bearer sk-route-key-0000000000" {
		t.Errorf("fallback Authorization = %q, want the route key", up.gotAuth)
	}
	if res2.KeySource != "route" {
		t.Errorf("key_source = %q, want route", res2.KeySource)
	}
}

// TestRouteTestReportsTheUpstreamReason pins the part that turns "401" into an
// actionable message. A status code alone sent the k3 investigation down the
// wrong path for a whole evening.
func TestRouteTestReportsTheUpstreamReason(t *testing.T) {
	up := &probeUpstream{status: 401, body: `{"error":{"type":"invalid_authentication_error",` +
		`"code":"3001","message":"Invalid Authentication"}}`}
	ts := up.start(t)

	route := &Route{ID: 3, Name: "kimi", APIFormat: "openai-chat"}
	ch := Channel{ID: 68, RouteID: 3, Name: "moonshot",
		BaseURL: ts.URL, DownstreamAuthKey: "sk-dead-key"}

	res := testProbeServer().probeChannel(context.Background(), route, ch)

	if res.OK {
		t.Fatal("a 401 must not be reported as a pass")
	}
	if res.Kind != "auth_fail" {
		t.Errorf("kind = %q, want auth_fail", res.Kind)
	}
	if res.StatusCode != 401 {
		t.Errorf("status = %d, want 401", res.StatusCode)
	}
	for _, want := range []string{"invalid_authentication_error", "3001", "Invalid Authentication"} {
		if !strings.Contains(res.UpstreamMsg, want) {
			t.Errorf("upstream_msg = %q, want it to contain %q", res.UpstreamMsg, want)
		}
	}
}

// TestRouteTestFlagsConfigThatIsNotLiveYet pins the drift check.
//
// The route table is in memory, so a direct SQL edit is invisible for up to five
// minutes. Without this warning a correct fix reads as a failed fix — which is
// exactly what happened: the key was right, the process just had not loaded it.
func TestRouteTestFlagsConfigThatIsNotLiveYet(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	dbKey := "sk-freshly-fixed-key-999"
	route := &Route{ID: 4, Name: "kimi", APIFormat: "openai-chat"}
	ch := Channel{ID: 69, RouteID: 4, Name: "moonshot", BaseURL: ts.URL, DownstreamAuthKey: dbKey}

	// What the process is still serving: the old, broken key.
	s := testProbeServer()
	s.Routes = []Route{{
		ID: 4, Name: "kimi", APIFormat: "openai-chat",
		Channels: []Channel{{ID: 69, RouteID: 4, Name: "moonshot",
			BaseURL: ts.URL, DownstreamAuthKey: "sk-stale-key-000"}},
	}}

	res := s.probeChannel(context.Background(), route, ch)

	if !res.Stale {
		t.Fatal("stale = false: the probe tested a key the process is not using, " +
			"and said nothing about it")
	}
	if !strings.Contains(res.StaleNote, "routes:version") {
		t.Errorf("stale_note = %q, want it to name the immediate-reload command", res.StaleNote)
	}
	if res.LiveKeyFP == "" || res.LiveKeyFP == res.KeyFP {
		t.Errorf("live_key_fp = %q vs tested %q; the console must be able to show they differ",
			res.LiveKeyFP, res.KeyFP)
	}

	// Matching config must not cry wolf.
	s.Routes = []Route{{
		ID: 4, Name: "kimi", APIFormat: "openai-chat",
		Channels: []Channel{{ID: 69, RouteID: 4, Name: "moonshot",
			BaseURL: ts.URL, DownstreamAuthKey: dbKey}},
	}}
	res2 := s.probeChannel(context.Background(), route, ch)
	if res2.Stale {
		t.Errorf("stale = true for a config that is live; note = %q", res2.StaleNote)
	}
}

// TestRouteTestMissingModelsEndpointIsInconclusive keeps a generic upstream from
// being flagged red for the crime of not being OpenAI.
func TestRouteTestMissingModelsEndpointIsInconclusive(t *testing.T) {
	for _, status := range []int{404, 405, 501} {
		up := &probeUpstream{status: status, body: `{"detail":"not found"}`}
		ts := up.start(t)
		route := &Route{ID: 5, Name: "weather", APIFormat: "generic"}
		ch := Channel{ID: 70, RouteID: 5, Name: "wttr", BaseURL: ts.URL}

		res := testProbeServer().probeChannel(context.Background(), route, ch)
		if res.Kind != "no_probe" {
			t.Errorf("status %d: kind = %q, want no_probe — the address is reachable, "+
				"only the probe endpoint is missing", status, res.Kind)
		}
		if res.OK {
			t.Errorf("status %d: ok = true, want false (inconclusive is not a pass)", status)
		}
	}
}

// TestRouteTestWithoutAnyKey pins the "none" case: the probe still runs, but it
// must not invent an Authorization header, and it must say no auth was sent.
func TestRouteTestWithoutAnyKey(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	route := &Route{ID: 6, Name: "open", APIFormat: "openai-chat"}
	ch := Channel{ID: 71, RouteID: 6, Name: "anon", BaseURL: ts.URL}

	res := testProbeServer().probeChannel(context.Background(), route, ch)

	if up.gotAuth != "" {
		t.Errorf("Authorization = %q, want empty: fabricating a header would turn "+
			"'no key configured' into a confusing upstream 401", up.gotAuth)
	}
	if res.KeySource != "none" || res.KeyFP != "" {
		t.Errorf("key_source = %q, key_fp = %q; want none/empty", res.KeySource, res.KeyFP)
	}
	if !strings.Contains(res.Summary, "不会带任何鉴权头") {
		t.Errorf("summary = %q, want it to warn that no credential is sent", res.Summary)
	}
}

// TestRouteTestUnreachableUpstream pins the network-failure wording: "连不上"
// is a routing/DNS problem, not a credential problem, and the console colours
// on kind — mislabelling it would send people to rotate a valid key.
func TestRouteTestUnreachableUpstream(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead := ts.URL
	ts.Close() // nothing is listening now

	route := &Route{ID: 7, Name: "gone", APIFormat: "openai-chat"}
	ch := Channel{ID: 72, RouteID: 7, Name: "ghost", BaseURL: dead,
		DownstreamAuthKey: "sk-irrelevant"}

	res := testProbeServer().probeChannel(context.Background(), route, ch)
	if res.Kind != "unreachable" {
		t.Errorf("kind = %q, want unreachable", res.Kind)
	}
	if res.NetError == "" {
		t.Error("net_error is empty: the console would show a failure with no cause")
	}
	if res.OK {
		t.Error("ok = true for an unreachable upstream")
	}
}

// TestKeyFingerprintNeverRevealsTheKey is the security backstop for a feature
// whose whole job is to handle provider credentials.
func TestKeyFingerprintNeverRevealsTheKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"long", "sk-live-abcdefghijklmnopqrstuvwxyz0123456789"},
		{"medium", "sk-1234567890abcd"},
		{"short", "abc123"},
		{"empty", ""},
	}
	for _, tc := range cases {
		fp := keyFingerprint(tc.key)
		if tc.key == "" {
			if fp != "" {
				t.Errorf("%s: fingerprint = %q, want empty", tc.name, fp)
			}
			continue
		}
		if fp == tc.key {
			t.Errorf("%s: fingerprint is the whole key", tc.name)
		}
		if !strings.Contains(fp, "字符") {
			t.Errorf("%s: fingerprint = %q, want the length so two keys can be told apart",
				tc.name, fp)
		}
	}
	// Two keys sharing a prefix must still be distinguishable.
	if keyFingerprint("sk-aaaaaaaaaaaaaaaaaaaa") == keyFingerprint("sk-aaaaaaaaaaaaaaaaaaab") {
		t.Error("two keys differing only at the end share a fingerprint")
	}
}

func TestUpstreamMessageParsesProviderEnvelopes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"openai style", `{"error":{"type":"invalid_request_error","message":"bad model"}}`,
			"invalid_request_error · bad model"},
		{"anthropic style", `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`,
			"authentication_error · invalid x-api-key"},
		{"bare message", `{"message":"rate limited"}`, "rate limited"},
		{"msg alias", `{"msg":"upstream down"}`, "upstream down"},
		{"not json", `upstream exploded`, "upstream exploded"},
		{"empty", ``, ""},
		{"json but uninformative", `{"foo":1}`, ""},
	}
	for _, tc := range cases {
		if got := upstreamMessage([]byte(tc.body)); got != tc.want {
			t.Errorf("%s: upstream_message = %q, want %q", tc.name, got, tc.want)
		}
	}
	// A runaway body must not be echoed whole into the console.
	long := `{"error":{"message":"` + strings.Repeat("x", 5000) + `"}}`
	if got := upstreamMessage([]byte(long)); len(got) > 400 {
		t.Errorf("upstream_message is %d chars; want it truncated", len(got))
	}
}

// TestAdminRouteTestGuards pins the endpoint's contract: admin-only, and it
// refuses to guess what to probe.
func TestAdminRouteTestGuards(t *testing.T) {
	s, ts := newAdminTestServer(t)
	post := func(token, body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/routes/test",
			strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("X-Admin-Token", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		return resp
	}

	anon := post("", `{"route_id":1}`)
	anon.Body.Close()
	if anon.StatusCode != http.StatusForbidden {
		t.Errorf("anonymous test = %d, want 403", anon.StatusCode)
	}

	empty := post(s.Cfg.AdminToken, `{}`)
	empty.Body.Close()
	if empty.StatusCode != http.StatusBadRequest {
		t.Errorf("empty target = %d, want 400: with neither id the endpoint would "+
			"have to guess what to probe", empty.StatusCode)
	}

	missing := post(s.Cfg.AdminToken, `{"route_id":99999999}`)
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("nonexistent route = %d, want 404", missing.StatusCode)
	}

	get := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/routes/test", nil)
		req.Header.Set("X-Admin-Token", s.Cfg.AdminToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		return resp
	}()
	get.Body.Close()
	if get.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d, want 405: a probe has side effects upstream and must "+
			"not be reachable by a prefetch or a crawler", get.StatusCode)
	}
}

// TestAdminRouteTestEndToEnd drives the real endpoint against a seeded route
// and channel, asserting the JSON the console actually consumes.
func TestAdminRouteTestEndToEnd(t *testing.T) {
	s, ts := newAdminTestServer(t)
	ctx := context.Background()

	up := &probeUpstream{status: 200, body: `{"data":[{"id":"k3"}]}`}
	ups := up.start(t)

	var routeID int64
	if err := s.DB.QueryRowContext(ctx,
		`INSERT INTO routes (name, base_url, match_path, api_format, downstream_auth_key, status)
		 VALUES ('zt-probe-route',$1,'/zt-probe','openai-chat','sk-route-level',1) RETURNING id`,
		ups.URL).Scan(&routeID); err != nil {
		t.Fatalf("insert route: %v", err)
	}
	cleanup := func() {
		bg := context.Background()
		_, _ = s.DB.ExecContext(bg, `DELETE FROM channels WHERE route_id=$1`, routeID)
		_, _ = s.DB.ExecContext(bg, `DELETE FROM routes WHERE id=$1`, routeID)
	}
	t.Cleanup(cleanup)

	var chanID int64
	if err := s.DB.QueryRowContext(ctx,
		`INSERT INTO channels (route_id, name, base_url, api_format, downstream_auth_key, enabled)
		 VALUES ($1,'zt-probe-chan',$2,'openai-chat','sk-channel-level',true) RETURNING id`,
		routeID, ups.URL+"/v1").Scan(&chanID); err != nil {
		t.Fatalf("insert channel: %v", err)
	}

	// Route-level: every channel gets a result.
	body := `{"route_id":` + itoa64(routeID) + `}`
	resp := postRouteTest(t, ts.URL, s.Cfg.AdminToken, body)
	defer resp.Body.Close()
	var out routeTestResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Results) != 1 {
		t.Fatalf("results = %d, want 1 (one channel)", len(out.Results))
	}
	res := out.Results[0]
	if !res.OK {
		t.Errorf("ok = false, summary %q, upstream_msg %q", res.Summary, res.UpstreamMsg)
	}
	if res.ChannelID != chanID {
		t.Errorf("channel_id = %d, want %d", res.ChannelID, chanID)
	}
	if up.gotAuth != "Bearer sk-channel-level" {
		t.Errorf("Authorization = %q, want the channel key", up.gotAuth)
	}
	if out.ProbeHint == "" {
		t.Error("probe_hint is empty: the console would show a verdict without its caveat")
	}

	// Channel-level: only the named channel is probed.
	resp2 := postRouteTest(t, ts.URL, s.Cfg.AdminToken, `{"channel_id":`+itoa64(chanID)+`}`)
	defer resp2.Body.Close()
	var out2 routeTestResponse
	if err := json.NewDecoder(resp2.Body).Decode(&out2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out2.Results) != 1 || out2.Results[0].ChannelID != chanID {
		t.Fatalf("channel-level results = %+v, want exactly channel %d", out2.Results, chanID)
	}

	// The result must never carry the key itself — it is rendered into HTML.
	raw, _ := json.Marshal(out)
	for _, secret := range []string{"sk-channel-level", "sk-route-level"} {
		if strings.Contains(string(raw), secret) {
			t.Errorf("response contains the provider key %q", secret)
		}
	}
}

func postRouteTest(t *testing.T, base, token, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/admin/routes/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("routes/test = %d, want 200: %s", resp.StatusCode, b)
	}
	return resp
}

func itoa64(n int64) string {
	return strconv.FormatInt(n, 10)
}
