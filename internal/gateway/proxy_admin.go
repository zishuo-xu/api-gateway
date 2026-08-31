package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ----- mihomo egress console -----
//
// Why this is a gateway endpoint and not a separate page: the mihomo external
// controller has no authentication whatsoever. Anyone who can reach it can read
// every upstream credential and rewrite the routing, so the only safe way to
// talk to it is over the container network — which means the caller has to be a
// container too. The gateway already sits on that network, and routing the
// calls through it means the console inherits the admin session instead of
// growing a second login prompt.
//
// Nothing here is required for the gateway to work. With MIHOMO_API unset the
// endpoint reports configured:false and the console hides the tab.

const (
	mihomoStatusTimeout = 6 * time.Second
	mihomoSwitchTimeout = 6 * time.Second
	// A full-group probe walks every node in the subscription. 70-odd nodes
	// tested concurrently is fast on a healthy link but each one can burn the
	// full per-node timeout when the airport is having a bad day, so this has
	// to be generous or the button "fails" on exactly the occasions you need it.
	mihomoTestTimeout = 120 * time.Second
	// Per-node budget handed to mihomo. Keep it in step with the guard script's
	// timeout so the console and the cron job agree on what "unreachable" means.
	mihomoTestNodeTimeout = 8000
)

// proxyDelayCache holds one full sweep of per-node delays.
//
// It exists because mihomo offers no way to read the delays it already knows.
// Its per-node history array is empty in practice, and the periodic url-test
// that picks the fastest node does not record what it measured. So the only
// way to show a delay is to measure it — and measuring 70 nodes on every
// two-second dashboard tick is not an option. Measure when asked, remember it,
// and say how old the number is.
type proxyDelayCache struct {
	mu     sync.Mutex
	delays map[string]int
	at     time.Time
}

func (c *proxyDelayCache) load() (map[string]int, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.delays, c.at
}

func (c *proxyDelayCache) store(d map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delays = d
	c.at = time.Now()
}

// delays returns the process-wide sweep cache, creating it on first use.
func (s *Server) delays() *proxyDelayCache {
	s.proxyOnce.Do(func() { s.proxyDelays = &proxyDelayCache{} })
	return s.proxyDelays
}

// proxyNode is one selectable egress target, trimmed to what the page draws.
// mihomo's /proxies payload carries far more per node (uuid, tfo, mptcp, full
// history arrays...) and forwarding all of it would push several hundred KB to
// the browser on every refresh tick.
type proxyNode struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	DelayMS int    `json:"delay_ms"`
}

// proxyStatus is the payload behind /admin/proxy.
type proxyStatus struct {
	Configured bool        `json:"configured"`
	Error      string      `json:"error,omitempty"`
	Version    string      `json:"version,omitempty"`
	Group      string      `json:"group"`
	GroupType  string      `json:"group_type"`
	AutoGroup  string      `json:"auto_group,omitempty"`
	Current    string      `json:"current,omitempty"`
	AutoNode   string      `json:"auto_node,omitempty"`
	IsAuto     bool        `json:"is_auto"`
	Nodes      []proxyNode `json:"nodes"`
	Total      int         `json:"total"`
	Alive      int         `json:"alive"`
	// Tested marks the delays as measured by this very request rather than
	// recalled from an earlier one.
	Tested bool `json:"tested"`
	// DelaysAgeSec is how stale the delays are, or -1 if this process has
	// never run a sweep. The page needs it to tell "not measured yet" apart
	// from "measured and did not answer" — two very different reasons to see
	// a zero, and only one of them means the node is broken.
	DelaysAgeSec int `json:"delays_age_sec"`
}

// mihomoProxies is the subset of GET /proxies we parse. The real payload has
// dozens of fields per node; unmarshalling into a map of this shape drops the
// rest without having to enumerate it.
type mihomoProxies struct {
	Proxies map[string]struct {
		Type string   `json:"type"`
		Now  string   `json:"now"`
		All  []string `json:"all"`
		// History holds mihomo's past probe results, oldest first. Only the
		// tail is read: one number per node telling us how it did last time
		// mihomo measured it.
		History []struct {
			Delay int `json:"delay"`
		} `json:"history"`
	} `json:"proxies"`
}

// adminProxy serves GET (status) and PUT (switch node) for the egress page.
func (s *Server) adminProxy(w http.ResponseWriter, r *http.Request) {
	if s.Cfg.MihomoAPI == "" {
		writeJSON(w, proxyStatus{Configured: false})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.proxyStatus(r.Context(), false))
	case http.MethodPut:
		s.proxySwitch(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// adminProxyRetest re-probes every node, then reports the fresh delays.
//
// It is a separate endpoint because it is slow by nature: a full sweep is
// seconds-to-minutes of work, which must not sit in front of a page that
// repaints every two seconds.
func (s *Server) adminProxyRetest(w http.ResponseWriter, r *http.Request) {
	if s.Cfg.MihomoAPI == "" {
		writeJSON(w, proxyStatus{Configured: false})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	st := s.proxyStatus(r.Context(), true)
	// Retest is a deliberate button press, so a failure here should look like
	// one instead of blending into a normal refresh.
	if st.Error != "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(st)
		return
	}
	writeJSON(w, st)
}

// proxySwitch moves the egress group to a named option.
func (s *Server) proxySwitch(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if err := decodeOptional(r, &in); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	st := s.proxyStatus(r.Context(), false)
	if st.Error != "" {
		writeJSON(w, st)
		return
	}
	// Reject unknown targets here rather than letting mihomo answer with a
	// bare 404: the caller needs to know the option list is stale, not that
	// something went wrong mid-flight.
	found := false
	if st.AutoGroup != "" && name == st.AutoGroup {
		found = true
	}
	for _, n := range st.Nodes {
		if n.Name == name {
			found = true
			break
		}
	}
	if !found {
		http.Error(w, "no such egress option: "+name, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), mihomoSwitchTimeout)
	defer cancel()

	body, err := json.Marshal(map[string]string{"name": name})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	code, _, err := s.mihomoDo(ctx, http.MethodPut, "/proxies/"+s.Cfg.MihomoGroup, body)
	if err != nil {
		log.Printf("proxy switch err: %v", err)
		writeJSON(w, proxyStatus{Configured: true, Error: "切换失败: " + err.Error()})
		return
	}
	if code < 200 || code >= 300 {
		log.Printf("proxy switch upstream status: %d", code)
		writeJSON(w, proxyStatus{Configured: true, Error: "mihomo 拒绝了这次切换（HTTP " + strconv.Itoa(code) + "）"})
		return
	}
	log.Printf("proxy egress switched to %q by admin", name)
	writeJSON(w, s.proxyStatus(r.Context(), false))
}

// proxyStatus assembles the egress snapshot. With test enabled it runs a live
// probe of every node first, which is the slow path.
func (s *Server) proxyStatus(ctx context.Context, test bool) proxyStatus {
	// Checked here as well as in the handlers: a future caller reaching this
	// directly would otherwise fire a request at an empty base URL and report
	// it as a connection failure, which reads like an outage rather than an
	// unconfigured feature.
	if s.Cfg.MihomoAPI == "" {
		return proxyStatus{Configured: false}
	}
	group := s.Cfg.MihomoGroup
	if group == "" {
		group = "PROXY"
	}
	st := proxyStatus{Configured: true, Group: group, AutoGroup: s.Cfg.MihomoAutoGroup}

	statusCtx, cancel := context.WithTimeout(ctx, mihomoStatusTimeout)
	defer cancel()

	raw, err := s.mihomoGet(statusCtx, "/proxies")
	if err != nil {
		st.Error = "连不上 mihomo: " + err.Error()
		return st
	}
	var mp mihomoProxies
	if err := json.Unmarshal(raw, &mp); err != nil {
		st.Error = "mihomo 返回了无法解析的数据: " + err.Error()
		return st
	}

	g, ok := mp.Proxies[group]
	if !ok {
		st.Error = "mihomo 里没有名为 " + group + " 的代理组"
		return st
	}
	st.GroupType = g.Type
	st.Current = g.Now

	// The group being a url-test one is the single mistake that makes this
	// whole page useless, and it is invisible from the outside: the switch
	// succeeds, then mihomo quietly re-picks the fastest node at the next
	// interval and the operator concludes the button is broken. Say so plainly.
	st.IsAuto = st.AutoGroup != "" && st.Current == st.AutoGroup
	if st.AutoGroup != "" {
		if ag, ok := mp.Proxies[st.AutoGroup]; ok {
			st.AutoNode = ag.Now
		}
	}

	// Delays come from a fresh probe when asked for, and from the last sweep
	// this process remembers otherwise. No url parameter on the probe path on
	// purpose: mihomo then tests against the URL configured on the group, so
	// the console and mihomo's own scheduling judge nodes by the same target.
	delays, delaysAt := s.delays().load()
	if test {
		testCtx, testCancel := context.WithTimeout(ctx, mihomoTestTimeout)
		defer testCancel()
		probe := st.AutoGroup
		if probe == "" {
			probe = group
		}
		path := "/group/" + probe + "/delay?timeout=" + strconv.Itoa(mihomoTestNodeTimeout)
		fresh := map[string]int{}
		if raw, err := s.mihomoGet(testCtx, path); err == nil {
			if err := json.Unmarshal(raw, &fresh); err != nil {
				log.Printf("proxy retest decode err: %v", err)
				st.Error = "测速结果无法解析: " + err.Error()
			} else {
				delays = fresh
				s.delays().store(delays)
				delaysAt = time.Now()
				st.Tested = true
			}
		} else {
			// A failed sweep is not fatal: fall through and report whatever
			// the cache still holds, flagged as stale, rather than blanking
			// the page.
			log.Printf("proxy retest err: %v", err)
			st.Error = "测速未完成: " + err.Error()
		}
	}
	st.DelaysAgeSec = -1
	if !delaysAt.IsZero() {
		st.DelaysAgeSec = int(time.Since(delaysAt).Seconds())
	}

	for _, name := range g.All {
		n := proxyNode{Name: name}
		if p, ok := mp.Proxies[name]; ok {
			n.Type = p.Type
			// mihomo's history is normally empty, but if a build does populate
			// it, a recorded result still beats nothing.
			if d, ok := delays[name]; ok {
				n.DelayMS = d
			} else if len(p.History) > 0 {
				n.DelayMS = p.History[len(p.History)-1].Delay
			}
		}
		st.Nodes = append(st.Nodes, n)
	}
	st.Total = len(st.Nodes)
	for _, n := range st.Nodes {
		if n.DelayMS > 0 {
			st.Alive++
		}
	}

	if v, err := s.mihomoGet(statusCtx, "/version"); err == nil {
		var ver struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(v, &ver); err == nil {
			st.Version = ver.Version
		}
	}
	return st
}

// mihomoGet performs an authenticated-by-network GET against the controller.
//
// A non-2xx answer is turned into an error rather than handed back as a body.
// It matters most for the delay probe: mihomo answers 404 for an unknown group
// with a plain-text body, and treating that as a successful sweep would leave
// the page showing cached delays under a "just measured" label — the one
// situation where a stale number does real damage, because the operator is
// looking at it precisely to decide whether a node recovered.
func (s *Server) mihomoGet(ctx context.Context, path string) ([]byte, error) {
	code, body, err := s.mihomoDo(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if code < 200 || code >= 300 {
		return nil, fmt.Errorf("mihomo 返回 HTTP %d (%s)", code, path)
	}
	return body, nil
}

// mihomoDo issues a request to the mihomo controller. The trailing slash in
// the base URL is tolerated so MIHOMO_API can be written either way.
func (s *Server) mihomoDo(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	base := strings.TrimRight(s.Cfg.MihomoAPI, "/")
	url := base + path

	var rdr io.Reader
	if body != nil {
		rdr = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, out, nil
}
