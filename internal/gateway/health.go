package gateway

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"
)

// ---- periodic upstream health probes ----------------------------------------
//
// Why this exists: the route "测试连接" button is passive — it only answers
// when someone is already looking at the console. A dead channel used to
// announce itself through a failed user request, which is the worst possible
// health signal: the user pays for the discovery.
//
// The prober re-runs the exact same probe (probeChannel — same credential
// precedence, same /models endpoint, same verdicts) on every enabled channel
// of every active route, on a fixed cadence. Three things come out of that:
//
//  1. /admin/health answers "are my upstreams good right now" without anyone
//     pressing a button.
//  2. A failing channel shows up red in the console *before* traffic finds
//     out the hard way.
//  3. A successful probe closes a circuit that is standing open, so a
//     recovered upstream starts serving again at the next sweep instead of
//     waiting for the open window to elapse and one unlucky user to pay for
//     the half-open probe.
//
// What it deliberately does not do: record failures into the breaker. The
// probe is a GET /models against whatever the database says; if it disagrees
// with the live config (a fix that has not been reloaded yet) the probe's
// failure is not evidence about the upstream the gateway is actually using.
// Symmetric failure recording would let a stale probe flap a circuit that
// real traffic was keeping happily closed.

// healthResult is the stored outcome of one channel's last probe. It is a
// trimmed copy of routeTestResult: the console needs the verdict, the cause
// and the latency, never the credential fingerprints of a background job.
type healthResult struct {
	RouteID     int64  `json:"route_id"`
	RouteName   string `json:"route_name"`
	ChannelID   int64  `json:"channel_id"`
	ChannelName string `json:"channel_name"`
	OK          bool   `json:"ok"`
	Kind        string `json:"kind"`
	Summary     string `json:"summary"`
	StatusCode  int    `json:"status_code"`
	LatencyMS   int64  `json:"latency_ms"`
	Stale       bool   `json:"stale"` // probe ran against config the process has not loaded yet
	TestedAt    string `json:"tested_at"`
}

type healthResponse struct {
	Enabled   bool           `json:"enabled"`
	IntervalS int            `json:"interval_sec"`
	Results   []healthResult `json:"results"`
}

// healthStore is the prober's memory. Keyed by channel id; the synthetic
// channel of a channelless route is stored under -routeID, which can never
// collide with a real channel id.
type healthStore struct {
	mu sync.RWMutex
	m  map[int64]healthResult
}

// healthResults returns the server's store, building it on first use so a
// hand-constructed Server (tests, and the prober itself before main wires it)
// never has to care about initialisation order.
func (s *Server) healthResults() *healthStore {
	s.healthOnce.Do(func() { s.health = &healthStore{m: map[int64]healthResult{}} })
	return s.health
}

func healthKey(routeID, channelID int64) int64 {
	if channelID == 0 {
		return -routeID
	}
	return channelID
}

func (h *healthStore) put(res healthResult) {
	h.mu.Lock()
	h.m[healthKey(res.RouteID, res.ChannelID)] = res
	h.mu.Unlock()
}

func (h *healthStore) snapshot() map[int64]healthResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[int64]healthResult, len(h.m))
	for k, v := range h.m {
		out[k] = v
	}
	return out
}

// healthSnapshotFor read-only accessor used by listRoutes: one lookup per
// channel row, no lock held across the loop.
func (h *healthStore) get(routeID, channelID int64) (healthResult, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	res, ok := h.m[healthKey(routeID, channelID)]
	return res, ok
}

// StartHealthProber probes every enabled channel of every active route on a
// fixed cadence and remembers the latest verdict per channel. interval <= 0
// disables the prober entirely (the /admin/health endpoint still answers,
// with enabled=false, so the console can say why the column is empty).
//
// Targets are re-read from the database on every sweep, never from the
// in-memory route table: a fix written straight into SQL must show up at the
// next sweep, not after the reload window. The per-result Stale flag still
// tells the two apart.
//
// The returned function stops the goroutine and waits for any in-flight
// sweep to finish.
func (s *Server) StartHealthProber(interval time.Duration) func() {
	if interval <= 0 {
		return func() {}
	}
	s.healthResults()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		// Probe once at startup: the console's health column should have an
		// answer from the first minute, not stay blank for a whole interval.
		s.probeAll()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				s.probeAll()
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// probeAll runs one sweep: load every route + channels from the database,
// probe the enabled ones in parallel, store each verdict.
func (s *Server) probeAll() {
	targets, err := s.loadAllProbeTargets()
	if err != nil {
		log.Printf("health probe: load targets err: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), routeTestBudget)
	defer cancel()
	var wg sync.WaitGroup
	for _, tg := range targets {
		wg.Add(1)
		go func(route *Route, ch Channel) {
			defer wg.Done()
			s.probeOne(ctx, route, ch)
		}(tg.route, tg.ch)
	}
	wg.Wait()
}

// probeTarget pairs a channel with its route. probeChannel needs both (the
// route supplies the fallback key and format), and loadAllProbeTargets must
// not lose which route a channel came from.
type probeTarget struct {
	route *Route
	ch    Channel
}

// loadAllProbeTargets reads every active route with its channels from the
// database and flattens them into probe work items. Disabled channels are
// skipped — probing a channel nobody can route to would flag red for
// something that cannot hurt anyone.
func (s *Server) loadAllProbeTargets() ([]probeTarget, error) {
	routes, err := LoadRoutes(s.DB)
	if err != nil {
		return nil, err
	}
	var out []probeTarget
	for i := range routes {
		rt := &routes[i]
		if len(rt.Channels) == 0 {
			// Channelless routes forward to the route's own base_url; the
			// synthetic channel mirrors what loadTestTargets builds for the
			// manual test, so probe and button agree on what "the target" is.
			out = append(out, probeTarget{route: rt, ch: Channel{
				RouteID:           rt.ID,
				Name:              rt.Name,
				BaseURL:           rt.BaseURL,
				APIFormat:         rt.APIFormat,
				DownstreamAuthKey: rt.DownstreamAuthKey,
				Enabled:           true,
			}})
			continue
		}
		for _, ch := range rt.Channels {
			if !ch.Enabled {
				continue
			}
			out = append(out, probeTarget{route: rt, ch: ch})
		}
	}
	return out, nil
}

// probeOne probes a single channel and records the verdict. On success it
// also closes the circuit if one is standing open (see the file header for
// why failures are never recorded).
func (s *Server) probeOne(ctx context.Context, route *Route, ch Channel) {
	res := s.probeChannel(ctx, route, ch)
	s.healthResults().put(healthResult{
		RouteID:     res.RouteID,
		RouteName:   res.RouteName,
		ChannelID:   res.ChannelID,
		ChannelName: res.ChannelName,
		OK:          res.OK,
		Kind:        res.Kind,
		Summary:     res.Summary,
		StatusCode:  res.StatusCode,
		LatencyMS:   res.LatencyMS,
		Stale:       res.Stale,
		TestedAt:    time.Now().Format(time.RFC3339),
	})
	if res.OK {
		s.noteProbeSuccess(ctx, route, ch)
		return
	}
	log.Printf("health probe: route=%d(%s) ch=%d(%s) kind=%s status=%d stale=%v",
		res.RouteID, res.RouteName, res.ChannelID, res.ChannelName,
		res.Kind, res.StatusCode, res.Stale)
}

// noteProbeSuccess lets a good probe close a circuit that is standing open.
//
// The cool-down window exists to keep user traffic away from a flapping
// upstream; the probe is not user traffic. When the sweep just watched the
// upstream answer, fencing it off for the rest of the window — and then
// spending one real request on the half-open probe — buys nothing. The
// witnessed success is the strongest recovery signal available, so it closes
// the circuit on the spot.
//
// Failures are never recorded here on purpose (see the file header): the
// probe reads the database, which may disagree with the live config for
// minutes, so its failure is not evidence about the upstream the gateway is
// actually using.
func (s *Server) noteProbeSuccess(ctx context.Context, route *Route, ch Channel) {
	if !route.CBEnabled {
		return
	}
	upstream := ch.upstreamKey(route.Name)
	if s.circuit().closeOnWitnessedSuccess(ctx, upstream) {
		log.Printf("health probe: %s ok, circuit closed early", upstream)
	}
}

// adminHealth serves GET /admin/health: the latest probe verdict for every
// channel the prober has seen, plus whether the prober is running at all.
func (s *Server) adminHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := healthResponse{
		Enabled:   s.healthInterval() > 0,
		IntervalS: int(s.healthInterval().Seconds()),
		Results:   []healthResult{},
	}
	snap := s.healthResults().snapshot()
	for _, res := range snap {
		resp.Results = append(resp.Results, res)
	}
	writeJSON(w, resp)
}

// healthInterval reads the configured cadence. 0 (or a missing config) means
// the prober is disabled; there is no hard-coded default here because
// config.Load owns that — otherwise a hand-built Server in a test would
// silently imply a cadence production never agreed to.
func (s *Server) healthInterval() time.Duration {
	if s.Cfg == nil || s.Cfg.HealthProbeSec <= 0 {
		return 0
	}
	return time.Duration(s.Cfg.HealthProbeSec) * time.Second
}
