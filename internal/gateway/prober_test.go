package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

// The periodic prober exists so a dead upstream shows up in the console before
// a user request pays for the discovery. These tests pin the three properties
// that make the prober trustworthy rather than just busy:
//
//   - it records one verdict per channel, addressable from the console's data
//   - a success closes a circuit that is standing open, instead of leaving the
//     recovered upstream fenced off until the open window elapses
//   - a failure never records into the breaker, so a probe that disagrees
//     with the live config cannot flap a circuit real traffic keeps closed
//
// The prober reuses probeChannel, which is already covered against a fake
// upstream in routetest_test.go; here a stubbed probeChannel would hide the
// wiring, so the tests drive the real one against httptest servers.

// testProberServer builds a Server with a memory breaker, so the prober can
// be driven without Redis or Postgres. Threshold 1: a single recorded
// failure opens the circuit, which is what makes the negative assertions
// below worth anything.
func testProberServer() *Server {
	return &Server{
		Cfg:     &config.Config{UpstreamTimeoutSec: 5},
		Breaker: newMemBreaker(1, time.Hour, time.Hour),
	}
}

// TestProbeOneRecordsTheVerdict pins the store contract: after a probe the
// console's data source can answer "how is this channel" for the exact key
// listRoutes looks up.
func TestProbeOneRecordsTheVerdict(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	s := testProberServer()
	route := &Route{ID: 11, Name: "kimi", APIFormat: "openai-chat", CBEnabled: true}
	ch := Channel{ID: 601, RouteID: 11, Name: "moonshot",
		BaseURL: ts.URL, DownstreamAuthKey: "sk-live-key-000000"}

	s.probeOne(context.Background(), route, ch)

	res, ok := s.healthResults().get(11, 601)
	if !ok {
		t.Fatal("no verdict stored for channel 601 — the console column would stay blank")
	}
	if !res.OK || res.Kind != "ok" {
		t.Errorf("verdict = %v/%q, want ok (summary %q)", res.OK, res.Kind, res.Summary)
	}
	if res.ChannelName != "moonshot" || res.RouteName != "kimi" {
		t.Errorf("names = %q/%q, want the labels the console renders", res.ChannelName, res.RouteName)
	}
	if res.TestedAt == "" {
		t.Error("tested_at is empty: the console cannot say how fresh the verdict is")
	}

	// A failing probe must overwrite the row, not pile up history.
	up2 := &probeUpstream{status: 401, body: `{"error":{"type":"invalid_authentication_error",` +
		`"code":"3001","message":"Invalid Authentication"}}`}
	ts2 := up2.start(t)
	ch.BaseURL = ts2.URL
	s.probeOne(context.Background(), route, ch)

	res2, _ := s.healthResults().get(11, 601)
	if res2.OK || res2.Kind != "auth_fail" {
		t.Errorf("second verdict = %v/%q, want the auth_fail to replace the ok", res2.OK, res2.Kind)
	}
	if !strings.Contains(res2.Summary, "密钥被上游拒绝") {
		t.Errorf("summary = %q, want the credential verdict", res2.Summary)
	}
}

// TestProbeSuccessClosesAnOpenCircuit pins the recovery path: a circuit that
// is standing open must not wait out its window when the prober can already
// see the upstream answering. Without this the fastest recovery is "one
// unlucky user pays for the half-open probe".
func TestProbeSuccessClosesAnOpenCircuit(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	s := testProberServer()
	route := &Route{ID: 12, Name: "kimi", APIFormat: "openai-chat", CBEnabled: true}
	ch := Channel{ID: 602, RouteID: 12, Name: "moonshot",
		BaseURL: ts.URL, DownstreamAuthKey: "sk-live-key-000000"}
	upstream := ch.upstreamKey(route.Name)

	// Trip the circuit the way real traffic would.
	s.recordFailure(context.Background(), upstream)
	if st := s.circuitState(context.Background(), upstream); st != stateOpen {
		t.Fatalf("state after failure = %q, want open (threshold is 1)", st)
	}

	s.probeOne(context.Background(), route, ch)

	if st := s.circuitState(context.Background(), upstream); st != stateClosed {
		t.Errorf("state after a successful probe = %q, want closed: the probe just "+
			"watched the upstream answer, keeping it fenced off helps nobody", st)
	}
}

// TestProbeFailureNeverTripsTheCircuit pins the asymmetry: the probe reads
// the database, which may disagree with the live config for minutes. A probe
// failure is not evidence about the upstream the gateway is actually using,
// so it must not count against the circuit.
func TestProbeFailureNeverTripsTheCircuit(t *testing.T) {
	up := &probeUpstream{status: 401, body: `{"error":{"message":"bad key"}}`}
	ts := up.start(t)

	s := testProberServer()
	route := &Route{ID: 13, Name: "kimi", APIFormat: "openai-chat", CBEnabled: true}
	ch := Channel{ID: 603, RouteID: 13, Name: "moonshot",
		BaseURL: ts.URL, DownstreamAuthKey: "sk-dead-key"}
	upstream := ch.upstreamKey(route.Name)

	// Threshold is 1: a single recorded failure would open the circuit.
	s.probeOne(context.Background(), route, ch)
	s.probeOne(context.Background(), route, ch)

	if st := s.circuitState(context.Background(), upstream); st != stateClosed {
		t.Errorf("state after failing probes = %q, want closed: probes must never "+
			"take a circuit down, only real traffic may", st)
	}
	// The verdict is still recorded — the console must show the failure even
	// though the breaker ignores it.
	res, ok := s.healthResults().get(13, 603)
	if !ok || res.OK {
		t.Errorf("verdict = %+v (found %v), want a stored auth failure", res, ok)
	}
}

// TestProbeOneRespectsCBOptOut: a route that opted out of circuit breaking
// must not gain circuit state from the prober either — the opt-out has to be
// honoured on every path that could record an outcome (same rule noteOutcome
// follows for real traffic).
func TestProbeOneRespectsCBOptOut(t *testing.T) {
	up := &probeUpstream{status: 200, body: `{"data":[]}`}
	ts := up.start(t)

	s := testProberServer()
	route := &Route{ID: 14, Name: "open", APIFormat: "openai-chat", CBEnabled: false}
	ch := Channel{ID: 604, RouteID: 14, Name: "anon", BaseURL: ts.URL}
	upstream := ch.upstreamKey(route.Name)

	// Pre-existing open state is impossible via noteOutcome for an opted-out
	// route, but nothing stops a leftover from an earlier configuration. The
	// prober must leave it alone rather than "helpfully" closing it.
	s.recordFailure(context.Background(), upstream)
	s.probeOne(context.Background(), route, ch)

	if st := s.circuitState(context.Background(), upstream); st != stateOpen {
		t.Errorf("state = %q, want open: an opted-out route's circuit state is "+
			"not the prober's business", st)
	}
}

// TestAdminHealthEndpoint pins the endpoint's contract: admin-only, GET only,
// and it reports whether the prober is even running so the console can
// distinguish "no verdicts yet" from "nobody is probing".
func TestAdminHealthEndpoint(t *testing.T) {
	s, ts := newAdminTestServer(t)
	s.Cfg.HealthProbeSec = 300

	// Seed one verdict directly; the sweep itself is covered above.
	s.healthResults().put(healthResult{
		RouteID: 21, RouteName: "kimi", ChannelID: 611, ChannelName: "moonshot",
		OK: true, Kind: "ok", Summary: "连通，密钥有效", StatusCode: 200,
		LatencyMS: 42, TestedAt: time.Now().Format(time.RFC3339),
	})

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anonymous = %d, want 403", resp.StatusCode)
	}

	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/health", nil)
	req2.Header.Set("X-Admin-Token", s.Cfg.AdminToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("health = %d, want 200: %s", resp2.StatusCode, b)
	}
	var out healthResponse
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Enabled || out.IntervalS != 300 {
		t.Errorf("enabled = %v interval = %d, want true/300 — the console needs "+
			"this to tell 'no verdicts yet' from 'prober off'", out.Enabled, out.IntervalS)
	}
	if len(out.Results) != 1 || out.Results[0].ChannelID != 611 {
		t.Fatalf("results = %+v, want the seeded verdict", out.Results)
	}

	req3, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/health", nil)
	req3.Header.Set("X-Admin-Token", s.Cfg.AdminToken)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d, want 405", resp3.StatusCode)
	}
}

// TestLoadAllProbeTargetsSkipsDisabledChannels pins the sweep's target
// selection against a real database: disabled channels are never probed
// (flagging red for a channel nobody can route to would cry wolf), and a
// channelless route still gets its synthetic target.
func TestLoadAllProbeTargetsSkipsDisabledChannels(t *testing.T) {
	s, _ := newAdminTestServer(t)
	ctx := context.Background()

	var routeID int64
	if err := s.DB.QueryRowContext(ctx,
		`INSERT INTO routes (name, base_url, match_path, api_format, status)
		 VALUES ('zt-health-route','http://127.0.0.1:9/up','/zt-health','openai-chat',1)
		 RETURNING id`).Scan(&routeID); err != nil {
		t.Fatalf("insert route: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = s.DB.ExecContext(bg, `DELETE FROM channels WHERE route_id=$1`, routeID)
		_, _ = s.DB.ExecContext(bg, `DELETE FROM routes WHERE id=$1`, routeID)
	})
	if _, err := s.DB.ExecContext(ctx,
		`INSERT INTO channels (route_id, name, base_url, api_format, enabled)
		 VALUES ($1,'zt-health-on','http://127.0.0.1:9/on','openai-chat',true),
		        ($1,'zt-health-off','http://127.0.0.1:9/off','openai-chat',false)`, routeID); err != nil {
		t.Fatalf("insert channels: %v", err)
	}

	targets, err := s.loadAllProbeTargets()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var got []string
	for _, tg := range targets {
		if tg.route.ID == routeID {
			got = append(got, tg.ch.Name)
		}
	}
	if len(got) != 1 || got[0] != "zt-health-on" {
		t.Errorf("targets for the seeded route = %v, want exactly [zt-health-on] — "+
			"a disabled channel must never be probed", got)
	}
}
