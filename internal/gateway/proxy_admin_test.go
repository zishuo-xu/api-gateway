package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

// The console talks to mihomo over the container network with no
// authentication and no schema versioning, so nothing stops a mihomo upgrade
// from renaming a field and leaving the egress page quietly blank. These tests
// pin the contract: the shape we read, and the fact that a manual switch is
// carried in a JSON body rather than a URL path.

// twoLayerProxies mirrors the real 2026-09-01 layout: PROXY is the select
// group the console switches, AUTO is the url-test group underneath it.
const twoLayerProxies = `{
  "proxies": {
    "PROXY":  {"type":"Selector","now":"AUTO","all":["AUTO","node-a","node-b","node-c"]},
    "AUTO":   {"type":"URLTest","now":"node-b","all":["node-a","node-b","node-c"]},
    "node-a": {"type":"Shadowsocks","history":[{"delay":100},{"delay":120}]},
    "node-b": {"type":"Shadowsocks","history":[{"delay":40}]},
    "node-c": {"type":"Shadowsocks","history":[{"delay":0}]}
  }
}`

// noHistoryProxies is what mihomo actually returns: the history array that
// looks like the natural place to read past delays from is simply empty. The
// console therefore cannot rely on it, which is the whole reason the gateway
// caches sweeps itself.
const noHistoryProxies = `{
  "proxies": {
    "PROXY":  {"type":"Selector","now":"AUTO","all":["AUTO","node-a","node-b","node-c"]},
    "AUTO":   {"type":"URLTest","now":"node-b","all":["node-a","node-b","node-c"]},
    "node-a": {"type":"Shadowsocks","history":[]},
    "node-b": {"type":"Shadowsocks","history":[]},
    "node-c": {"type":"Shadowsocks","history":[]}
  }
}`

// switchLog records the calls a test made into the stub controller.
type switchLog struct {
	method string
	path   string
	body   string
}

// mihomoStub stands in for the controller. It serves the same three endpoints
// the console depends on and records anything addressed to the switch group.
func mihomoStub(t *testing.T, proxiesJSON string) (*Server, *switchLog) {
	t.Helper()
	log := &switchLog{}
	mux := http.NewServeMux()
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
	s, _ := mihomoStub(t, twoLayerProxies)
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
	// node-c's only history entry is a failed probe (delay 0), so it must not
	// count towards the healthy total.
	if st.Alive != 2 {
		t.Errorf("Alive = %d, want 2: a zero delay means unreachable, not fast", st.Alive)
	}
	if st.Tested {
		t.Error("Tested = true without a retest: delays came from mihomo's own " +
			"schedule and must not be presented as freshly measured")
	}
}

func TestProxyStatusUsesLatestHistoryDelay(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies)
	st := s.proxyStatus(context.Background(), false)

	want := map[string]int{"node-a": 120, "node-b": 40, "node-c": 0}
	got := map[string]int{}
	for _, n := range st.Nodes {
		got[n.Name] = n.DelayMS
	}
	for name, d := range want {
		if got[name] != d {
			t.Errorf("delay[%s] = %d, want %d", name, got[name], d)
		}
	}
	// node-a has two history entries (100 then 120); taking the first instead
	// of the last would report a stale number.
	if got["node-a"] == 100 {
		t.Error("read the oldest history entry instead of the most recent one")
	}
}

func TestProxyRetestProbesTheAutoGroup(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies)
	st := s.proxyStatus(context.Background(), true)

	if !st.Tested {
		t.Fatal("Tested = false after a retest")
	}
	want := map[string]int{"node-a": 150, "node-b": 45, "node-c": 0}
	got := map[string]int{}
	for _, n := range st.Nodes {
		got[n.Name] = n.DelayMS
	}
	for name, d := range want {
		if got[name] != d {
			t.Errorf("delay[%s] = %d, want %d", name, got[name], d)
		}
	}
	// node-c times out in both the cached history and the fresh probe.
	if st.Alive != 2 {
		t.Errorf("Alive = %d, want 2", st.Alive)
	}
}

// The stub only registers /group/AUTO/delay. Probing PROXY instead would 404,
// fall back to cached delays, and look like a successful retest that simply
// never happened — so assert the live numbers actually arrived.
func TestProxyRetestFallsBackToHistoryWhenProbeFails(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies)
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
}

// mihomo keeps no usable record of the delays it measured, so the gateway has
// to remember them itself. This pins the part that makes the page honest: an
// age of -1 means "never measured", and the console must render that as
// unknown rather than as every node being down.
func TestProxyDelaysCachedAcrossCalls(t *testing.T) {
	s, _ := mihomoStub(t, noHistoryProxies)

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
	got := map[string]int{}
	for _, n := range after.Nodes {
		got[n.Name] = n.DelayMS
	}
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
	s, _ := mihomoStub(t, twoLayerProxies)
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
	got := map[string]int{}
	for _, n := range st.Nodes {
		got[n.Name] = n.DelayMS
	}
	if got["node-a"] != 150 {
		t.Errorf("delay[node-a] = %d, want the cached 150", got["node-a"])
	}
}

func TestProxyStatusReportsMissingGroup(t *testing.T) {
	s, _ := mihomoStub(t, twoLayerProxies)
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
        "PROXY":  {"type":"Selector","now":"AUTO","all":["AUTO","`+node+`"]},
        "AUTO":   {"type":"URLTest","now":"node-b","all":["node-b"]},
        "`+node+`": {"type":"Shadowsocks","history":[{"delay":60}]},
        "node-b": {"type":"Shadowsocks","history":[{"delay":40}]}
      }
    }`)

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
	s, log := mihomoStub(t, twoLayerProxies)

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
	s, _ := mihomoStub(t, twoLayerProxies)
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
	s, _ := mihomoStub(t, twoLayerProxies)
	s.Cfg.MihomoAPI += "/"
	st := s.proxyStatus(context.Background(), false)
	if st.Error != "" {
		t.Fatalf("trailing slash broke the request: %s", st.Error)
	}
	if st.Total != 4 {
		t.Errorf("Total = %d, want 4", st.Total)
	}
}
