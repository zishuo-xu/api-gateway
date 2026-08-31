package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

// The console talks to mihomo over the container network with no
// authentication and no schema versioning, so nothing stops a mihomo upgrade
// from renaming a field and leaving the egress page quietly blank. These tests
// pin the contract: which endpoint holds which data, and the fact that a
// manual switch is carried in a JSON body rather than a URL path.

// twoLayerProxies mirrors the real 2026-09-01 layout: PROXY is the select
// group the console switches, AUTO is the url-test group underneath it.
//
// Note what is deliberately absent: the nodes themselves. mihomo's
// GET /proxies lists only the policy groups — on the live config that is nine
// entries and not one of the subscription's 71 nodes — so node details have
// to come from the providers payload below. An earlier version of this code
// read node delays out of here, found nothing at every single key, and duly
// reported all 72 egress options as unreachable.
const twoLayerProxies = `{
  "proxies": {
    "PROXY": {"type":"Selector","now":"AUTO","all":["AUTO","node-a","node-b","node-c"]},
    "AUTO":  {"type":"URLTest","now":"node-b","all":["node-a","node-b","node-c"]}
  }
}`

// realProviders is where the nodes actually live.
//
// node-a was probed twice and got slower, so reading the oldest entry instead
// of the newest would understate it. node-c's newest probe failed, which is
// the ordinary state of a flaky node — and by far the most common tail in the
// live data — so a reader that trusts the last entry reports every such node
// as dead.
const realProviders = `{
  "providers": {
    "airport": {
      "proxies": [
        {"name":"node-a","type":"Shadowsocks","history":[{"delay":100,"time":"2026-08-31T17:00:00Z"},{"delay":120,"time":"2026-08-31T17:05:00Z"}]},
        {"name":"node-b","type":"Vless","history":[{"delay":40,"time":"2026-08-31T17:05:00Z"}]},
        {"name":"node-c","type":"Hysteria2","history":[{"delay":90,"time":"2026-08-31T17:00:00Z"},{"delay":0,"time":"2026-08-31T17:05:00Z"}]}
      ]
    }
  }
}`

// noHistoryProviders is the worst case: every node exists but nothing has ever
// been measured. The page has to render that as "unknown", not as an outage.
const noHistoryProviders = `{
  "providers": {
    "airport": {
      "proxies": [
        {"name":"node-a","type":"Shadowsocks","history":[]},
        {"name":"node-b","type":"Vless","history":[]},
        {"name":"node-c","type":"Hysteria2","history":[]}
      ]
    }
  }
}`

// switchLog records the calls a test made into the stub controller.
type switchLog struct {
	method string
	path   string
	body   string
}

// mihomoStub stands in for the controller. It serves the endpoints the console
// depends on and records anything addressed to the switch group.
//
// Deliberately absent unless a test passes it in: /providers/proxies. A
// controller that does not serve it is a real state worth testing, because the
// delays must survive it.
func mihomoStub(t *testing.T, proxiesJSON, providersJSON string) (*Server, *switchLog) {
	t.Helper()
	log := &switchLog{}
	mux := http.NewServeMux()
	if providersJSON != "" {
		mux.HandleFunc("/providers/proxies", func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, providersJSON)
		})
	}
	mux.HandleFunc("/proxies", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, proxiesJSON)
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"meta":true,"version":"v1.19.30"}`)
	})
	mux.HandleFunc("/group/AUTO/delay", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"node-a":150,"node-b":45,"node-c":0}`)
	})
	mux.HandleFunc("/proxies/PROXY", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		log.method, log.path, log.body = r.Method, r.URL.Path, strings.TrimSpace(string(b))
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	s := &Server{Cfg: &config.Config{
		MihomoAPI:       ts.URL,
		MihomoGroup:     "PROXY",
		MihomoAutoGroup: "AUTO",
	}}
	return s, log
}

func nodeDelays(st proxyStatus) map[string]int {
	got := map[string]int{}
	for _, n := range st.Nodes {
		got[n.Name] = n.DelayMS
	}
	return got
}

func TestProxyStatusDisabledWithoutConfig(t *testing.T) {
	s := &Server{Cfg: &config.Config{}}
	st := s.proxyStatus(context.Background(), false)
	if st.Configured {
		t.Error("configured=true with MIHOMO_API unset: the page would show an " +
			"empty egress panel to every deployment that runs no mihomo")
	}
	if st.Error != "" {
		t.Errorf("Error = %q, want empty: absence of a proxy is not a failure", st.Error)
	}
}

func TestProxyStatusReadsTwoLayerGroups(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	st := s.proxyStatus(context.Background(), false)

	if st.Error != "" {
		t.Fatalf("unexpected error: %s", st.Error)
	}
	if st.Current != "AUTO" {
		t.Errorf("Current = %q, want AUTO", st.Current)
	}
	if !st.IsAuto {
		t.Error("IsAuto = false, want true: PROXY is pointing at the url-test " +
			"group, so the console must offer switching away from auto")
	}
	if st.AutoNode != "node-b" {
		t.Errorf("AutoNode = %q, want node-b: that is the node actually in use", st.AutoNode)
	}
	if st.GroupType != "Selector" {
		t.Errorf("GroupType = %q, want Selector", st.GroupType)
	}
	if st.Version != "v1.19.30" {
		t.Errorf("Version = %q, want v1.19.30", st.Version)
	}
	if st.Total != 4 {
		t.Errorf("Total = %d, want 4 (AUTO plus three nodes)", st.Total)
	}
	// Without a sweep of our own, all three nodes fall back on what mihomo
	// last measured — including node-c, whose newest probe failed but whose
	// previous one did not.
	if st.Alive != 3 {
		t.Errorf("Alive = %d, want 3", st.Alive)
	}
	if st.Tested {
		t.Error("Tested = true without a retest: delays came from mihomo's own " +
			"schedule and must not be presented as freshly measured")
	}
	// The type also only exists in the providers payload.
	if got := nodeDelays(st); got["node-a"] != 120 {
		t.Errorf("delay[node-a] = %d, want 120", got["node-a"])
	}
	var sawType bool
	for _, n := range st.Nodes {
		if n.Name == "node-b" && n.Type == "Vless" {
			sawType = true
		}
	}
	if !sawType {
		t.Error("node-b type missing: node protocols come from /providers/proxies, " +
			"not from /proxies, which does not list nodes at all")
	}
}

// Reading the first history entry instead of the last would report a node as
// faster than it is, and on a group whose url-test picks the fastest node that
// is the difference between the node mihomo chose and the one the page says it
// should have chosen.
func TestProxyStatusUsesLatestHistoryDelay(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	st := s.proxyStatus(context.Background(), false)

	got := nodeDelays(st)
	if got["node-a"] != 120 {
		t.Errorf("delay[node-a] = %d, want 120 (the newest probe, not the 100 before it)", got["node-a"])
	}
}

// The live data is full of histories whose tail is a run of zeros: mihomo
// appends one every time a probe times out. Taking the last entry at face
// value would paint most of the subscription as dead.
func TestProxyStatusSkipsTrailingFailedProbes(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	st := s.proxyStatus(context.Background(), false)

	if got := nodeDelays(st); got["node-c"] != 90 {
		t.Errorf("delay[node-c] = %d, want 90: the newest probe failed, so the "+
			"last successful one is the freshest real answer", got["node-c"])
	}
}

// With nothing measured anywhere, "unreachable" and "unknown" look identical
// in the JSON. HistoryAgeSec is what lets the page tell them apart — say
// "never measured" instead of showing 72 dead nodes.
func TestProxyStatusWithNoHistoryAnywhere(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, noHistoryProviders)
	st := s.proxyStatus(context.Background(), false)

	if st.Alive != 0 {
		t.Errorf("Alive = %d, want 0", st.Alive)
	}
	if st.DelaysAgeSec != -1 {
		t.Errorf("DelaysAgeSec = %d, want -1: nothing has been measured", st.DelaysAgeSec)
	}
	if st.HistoryAgeSec != -1 {
		t.Errorf("HistoryAgeSec = %d, want -1: mihomo has no record either", st.HistoryAgeSec)
	}
	if st.Total != 4 {
		t.Errorf("Total = %d, want 4", st.Total)
	}
}

func TestProxyRetestProbesTheAutoGroup(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	st := s.proxyStatus(context.Background(), true)

	if !st.Tested {
		t.Fatal("Tested = false after a retest")
	}
	got := nodeDelays(st)
	want := map[string]int{"node-a": 150, "node-b": 45, "node-c": 0}
	for name, d := range want {
		if got[name] != d {
			t.Errorf("delay[%s] = %d, want %d", name, got[name], d)
		}
	}
	// node-c timed out in the live probe, so it is down *now* even though its
	// history says it worked ten minutes ago.
	if st.Alive != 2 {
		t.Errorf("Alive = %d, want 2: a failed live probe must not be papered "+
			"over by an older success", st.Alive)
	}
	if st.DelaysAgeSec != 0 {
		t.Errorf("DelaysAgeSec = %d, want 0 right after a sweep", st.DelaysAgeSec)
	}
}

// A live zero is a real answer and must stand on its own. Falling back to
// history here would tell someone debugging a dead node that it is fine.
func TestProxyRetestZeroBeatsStaleHistory(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	st := s.proxyStatus(context.Background(), true)

	for _, n := range st.Nodes {
		if n.Name == "node-c" {
			if n.DelayMS != 0 {
				t.Errorf("delay[node-c] = %d, want 0: our own probe failed, and "+
					"that outranks the 90 mihomo recorded earlier", n.DelayMS)
			}
			if !n.Measured {
				t.Error("node-c Measured = false: a zero from a real sweep is a " +
					"result, and dropping it would make it indistinguishable from never having looked")
			}
		}
	}
}

// The stub only registers /group/AUTO/delay. Probing PROXY instead would 404,
// fall back to cached delays, and look like a successful retest that simply
// never happened — so assert the live numbers actually arrived.
func TestProxyRetestFallsBackToHistoryWhenProbeFails(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	s.Cfg.MihomoAutoGroup = "MISSING" // no such group -> probe 404s
	st := s.proxyStatus(context.Background(), true)

	if st.Tested {
		t.Error("Tested = true although the probe endpoint did not exist")
	}
	if st.Error == "" {
		t.Error("Error is empty: an aborted retest must be visible, otherwise " +
			"the operator sees stale delays and believes they are current")
	}
	// Still usable: history delays survive the failed sweep.
	if st.Total != 4 {
		t.Errorf("Total = %d, want 4: a failed probe must not empty the list", st.Total)
	}
	if st.Alive != 3 {
		t.Errorf("Alive = %d, want 3: the history delays should still show", st.Alive)
	}
}

// The regression that shipped: the delays were read from a map that mihomo
// does not populate, so a perfectly healthy 72-node subscription was reported
// as entirely unreachable. Node metadata is decoration; it must never be able
// to suppress a delay.
func TestProxyDelaysSurviveMissingProvidersEndpoint(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, "") // no /providers/proxies at all
	st := s.proxyStatus(context.Background(), true)

	if st.Error != "" {
		t.Fatalf("a missing metadata endpoint must not fail the page: %s", st.Error)
	}
	if !st.Tested {
		t.Fatal("Tested = false: the sweep ran, it just has no node types to show")
	}
	got := nodeDelays(st)
	if got["node-a"] != 150 || got["node-b"] != 45 {
		t.Errorf("delays = %v, want node-a=150 node-b=45", got)
	}
	if st.Alive != 2 {
		t.Errorf("Alive = %d, want 2", st.Alive)
	}
}

// mihomo keeps no readable record of the delays it measured on demand, so the
// gateway has to remember them itself. This pins the part that makes the page
// honest: an age of -1 means "never measured", and the console must render
// that as unknown rather than as every node being down.
func TestProxyDelaysCachedAcrossCalls(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, noHistoryProviders)

	before := s.proxyStatus(context.Background(), false)
	if before.DelaysAgeSec != -1 {
		t.Errorf("DelaysAgeSec = %d, want -1 before any sweep", before.DelaysAgeSec)
	}
	if before.Alive != 0 {
		t.Errorf("Alive = %d, want 0: with nothing measured, claiming nodes are "+
			"down would be a lie in the other direction", before.Alive)
	}

	swept := s.proxyStatus(context.Background(), true)
	if swept.DelaysAgeSec != 0 {
		t.Errorf("DelaysAgeSec = %d, want 0 right after a sweep", swept.DelaysAgeSec)
	}

	// A plain read later must recall the numbers without probing again.
	after := s.proxyStatus(context.Background(), false)
	if after.Tested {
		t.Error("Tested = true on a plain read: it should be reporting the cache")
	}
	if after.DelaysAgeSec < 0 {
		t.Fatal("DelaysAgeSec = -1: the sweep was not remembered")
	}
	got := nodeDelays(after)
	if got["node-a"] != 150 || got["node-b"] != 45 {
		t.Errorf("cached delays = %v, want node-a=150 node-b=45", got)
	}
	if after.Alive != 2 {
		t.Errorf("Alive = %d, want 2", after.Alive)
	}
}

// A sweep that fails must leave an earlier good result in place. Wiping the
// cache on error would turn a transient mihomo hiccup into "every node is
// down", which is exactly the wrong thing to show someone mid-incident.
func TestProxyFailedSweepKeepsPreviousDelays(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	if st := s.proxyStatus(context.Background(), true); st.Alive != 2 {
		t.Fatalf("warm-up sweep: Alive = %d, want 2", st.Alive)
	}

	s.Cfg.MihomoAutoGroup = "MISSING"
	st := s.proxyStatus(context.Background(), true)
	if st.Tested {
		t.Error("Tested = true after a failed sweep")
	}
	if st.Error == "" {
		t.Error("Error is empty after a failed sweep")
	}
	if st.DelaysAgeSec < 0 {
		t.Error("delays were dropped: the previous good sweep should survive")
	}
	if got := nodeDelays(st); got["node-a"] != 150 {
		t.Errorf("delay[node-a] = %d, want the cached 150", got["node-a"])
	}
}

func TestProxyStatusReportsMissingGroup(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	s.Cfg.MihomoGroup = "NOPE"
	st := s.proxyStatus(context.Background(), false)
	if st.Error == "" {
		t.Fatal("Error is empty for a group mihomo does not have")
	}
	if !strings.Contains(st.Error, "NOPE") {
		t.Errorf("Error = %q, want it to name the missing group", st.Error)
	}
}

// A controller that is simply gone must degrade the page, not break it: the
// dashboard polls every two seconds and one dead dependency cannot take the
// rest of the console down with it.
// (A controller that accepts then hangs is the same code path — bounded by
// mihomoStatusTimeout — but costs the full six seconds to prove, so it is
// left to that constant rather than to a slow test.)
func TestProxyStatusSurvivesUnreachableMihomo(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ts.Close() // nothing is listening on the port any more

	s := &Server{Cfg: &config.Config{MihomoAPI: ts.URL, MihomoGroup: "PROXY"}}
	st := s.proxyStatus(context.Background(), false)
	if st.Error == "" {
		t.Fatal("Error is empty although mihomo never answered")
	}
	if !st.Configured {
		t.Error("Configured = false: it is configured, just unreachable, and the " +
			"page needs to tell those two apart")
	}
}

func TestProxySwitchSendsNameInBody(t *testing.T) {
	// The node name is the whole point of this test: it carries an emoji and a
	// pipe, exactly the characters that break when a name is put in a URL path
	// (mihomo answers "Resource not found" for /proxies/{name}/delay). It has
	// to be in the stub's own payload, because proxySwitch re-reads the option
	// list before acting and rejects anything it cannot find there.
	const node = "🇭🇰香港HKT | 高速专线-hy2"
	s, log := mihomoStub(t, `{
      "proxies": {
        "PROXY": {"type":"Selector","now":"AUTO","all":["AUTO","`+node+`"]},
        "AUTO":  {"type":"URLTest","now":"node-b","all":["node-b"]}
      }
    }`, `{"providers":{"airport":{"proxies":[
      {"name":"`+node+`","type":"Hysteria2","history":[{"delay":60,"time":"2026-08-31T17:00:00Z"}]}
    ]}}}`)

	body, _ := json.Marshal(map[string]string{"name": node})
	req := httptest.NewRequest(http.MethodPut, "/admin/proxy", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	s.proxySwitch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if log.method != http.MethodPut {
		t.Errorf("method = %q, want PUT", log.method)
	}
	if log.path != "/proxies/PROXY" {
		t.Errorf("path = %q, want /proxies/PROXY", log.path)
	}
	if !strings.Contains(log.body, node) {
		t.Errorf("body = %q, want it to carry the node name verbatim", log.body)
	}
}

func TestProxySwitchRejectsUnknownTarget(t *testing.T) {
	s, log := mihomoStub(t, twoLayerProxies, realProviders)

	body := `{"name":"node-does-not-exist"}`
	req := httptest.NewRequest(http.MethodPut, "/admin/proxy", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.proxySwitch(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: an unknown node is a bad request, not "+
			"something to forward and let mihomo 404 on", rec.Code)
	}
	if log.body != "" {
		t.Errorf("forwarded %q to mihomo although the target was invalid", log.body)
	}
}

func TestProxySwitchRejectsEmptyName(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	req := httptest.NewRequest(http.MethodPut, "/admin/proxy", strings.NewReader(`{"name":"  "}`))
	rec := httptest.NewRecorder()
	s.proxySwitch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// A trailing slash in MIHOMO_API is the sort of thing that gets typed during
// setup and produces a confusing double-slash URL.
func TestProxyStatusToleratesTrailingSlash(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies, realProviders)
	s.Cfg.MihomoAPI += "/"
	st := s.proxyStatus(context.Background(), false)
	if st.Error != "" {
		t.Fatalf("trailing slash broke the request: %s", st.Error)
	}
	if st.Total != 4 {
		t.Errorf("Total = %d, want 4", st.Total)
	}
}

// HistoryAgeSec is how the page says "mihomo measured this four minutes ago"
// instead of leaving the operator to guess whether a number is current.
func TestProxyStatusReportsHistoryAge(t *testing.T) {
	stamp := time.Now().Add(-4 * time.Minute).UTC().Format(time.RFC3339)
	s, _ := mihomoStub(t, twoLayerProxies, `{"providers":{"airport":{"proxies":[
      {"name":"node-a","type":"Vless","history":[{"delay":120,"time":"`+stamp+`"}]},
      {"name":"node-b","type":"Vless","history":[{"delay":40,"time":"`+stamp+`"}]},
      {"name":"node-c","type":"Vless","history":[]}
    ]}}}`)

	st := s.proxyStatus(context.Background(), false)
	if st.HistoryAgeSec < 230 || st.HistoryAgeSec > 250 {
		t.Errorf("HistoryAgeSec = %d, want ~240 (four minutes)", st.HistoryAgeSec)
	}
}
