package gateway

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xuzishuo/api-gateway/internal/store"
)

// histogram is a fixed-bucket accumulator. The gateway keeps latency
// distribution in memory rather than in Redis because Redis only stores the
// per-second counters used by the live charts, and re-deriving percentiles from
// those would be both lossy and expensive.
type histogram struct {
	bounds []float64
	counts []int64
	sum    float64
	count  int64
}

// latencyBuckets spans fast cache hits through slow reasoning-model streams.
var latencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 180,
}

func newHistogram(bounds []float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]int64, len(bounds)+1)}
}

func (h *histogram) observe(v float64) {
	h.sum += v
	h.count++
	// Bounds are sorted, so the first bucket whose upper edge is >= v wins.
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.counts)-1]++ // +Inf
}

// observe records end-to-end latency and token totals for one request.
func (s *Server) observe(latency time.Duration, m *meta) {
	s.statMu.Lock()
	defer s.statMu.Unlock()
	if s.latency == nil {
		s.latency = newHistogram(latencyBuckets)
	}
	s.latency.observe(latency.Seconds())
	s.tokens[0] += m.PromptTokens
	s.tokens[1] += m.CompletionTokens
}

func (s *Server) observeUpstream(d time.Duration) {
	s.statMu.Lock()
	defer s.statMu.Unlock()
	if s.upstream == nil {
		s.upstream = newHistogram(latencyBuckets)
	}
	s.upstream.observe(d.Seconds())
}

type histSnapshot struct {
	counts []int64
	sum    float64
	count  int64
}

// snapshot copies the counters. The bucket slice is always full length, even
// before any observation: Prometheus treats a histogram with no _bucket series
// as malformed, and a freshly started gateway must still scrape cleanly.
func (h *histogram) snapshot() histSnapshot {
	if h == nil {
		return histSnapshot{counts: make([]int64, len(latencyBuckets)+1)}
	}
	return histSnapshot{
		counts: append([]int64(nil), h.counts...),
		sum:    h.sum,
		count:  h.count,
	}
}

// ----- Prometheus exposition -----

// prometheusMetrics renders the /metrics payload in the Prometheus text format.
//
// Counters come from Redis (they already survive restarts); the latency
// histograms come from process memory, which is the right trade-off for a
// single-instance dashboard - a multi-instance deployment would scrape each
// replica and aggregate in Prometheus anyway.
func (s *Server) prometheusMetrics(ctx context.Context) string {
	var b strings.Builder

	total := store.GetCounter(ctx, s.RDB, "stats:total")
	cached := store.GetCounter(ctx, s.RDB, "stats:cached")
	rejected := store.GetCounter(ctx, s.RDB, "stats:rejected")
	errs := store.GetCounter(ctx, s.RDB, "stats:errors")

	b.WriteString("# HELP gateway_uptime_seconds Time since the gateway process started.\n")
	b.WriteString("# TYPE gateway_uptime_seconds gauge\n")
	b.WriteString(fmt.Sprintf("gateway_uptime_seconds %.0f\n\n", time.Since(s.Start).Seconds()))

	b.WriteString("# HELP gateway_requests_total Requests accepted by the gateway.\n")
	b.WriteString("# TYPE gateway_requests_total counter\n")
	b.WriteString(fmt.Sprintf("gateway_requests_total %d\n\n", total))

	b.WriteString("# HELP gateway_requests_cached_total Requests served from cache.\n")
	b.WriteString("# TYPE gateway_requests_cached_total counter\n")
	b.WriteString(fmt.Sprintf("gateway_requests_cached_total %d\n\n", cached))

	b.WriteString("# HELP gateway_requests_rejected_total Requests rejected by rate limiting or quota.\n")
	b.WriteString("# TYPE gateway_requests_rejected_total counter\n")
	b.WriteString(fmt.Sprintf("gateway_requests_rejected_total %d\n\n", rejected))

	b.WriteString("# HELP gateway_requests_error_total Responses with status >= 500.\n")
	b.WriteString("# TYPE gateway_requests_error_total counter\n")
	b.WriteString(fmt.Sprintf("gateway_requests_error_total %d\n\n", errs))

	s.statMu.Lock()
	lat := s.latency.snapshot()
	up := s.upstream.snapshot()
	prompt, completion := s.tokens[0], s.tokens[1]
	s.statMu.Unlock()

	b.WriteString("# HELP gateway_request_duration_seconds End-to-end request latency.\n")
	b.WriteString("# TYPE gateway_request_duration_seconds histogram\n")
	writeHist(&b, "gateway_request_duration_seconds", lat)

	b.WriteString("# HELP gateway_upstream_duration_seconds Time spent waiting on the upstream provider.\n")
	b.WriteString("# TYPE gateway_upstream_duration_seconds histogram\n")
	writeHist(&b, "gateway_upstream_duration_seconds", up)

	b.WriteString("# HELP gateway_tokens_total Tokens relayed since process start.\n")
	b.WriteString("# TYPE gateway_tokens_total counter\n")
	b.WriteString(fmt.Sprintf("gateway_tokens_total{direction=\"prompt\"} %d\n", prompt))
	b.WriteString(fmt.Sprintf("gateway_tokens_total{direction=\"completion\"} %d\n\n", completion))

	b.WriteString("# HELP gateway_routes_active Routes currently loaded and active.\n")
	b.WriteString("# TYPE gateway_routes_active gauge\n")
	b.WriteString(fmt.Sprintf("gateway_routes_active %d\n\n", s.routeCount()))

	b.WriteString("# HELP gateway_channels_active Channels currently loaded, per route.\n")
	b.WriteString("# TYPE gateway_channels_active gauge\n")
	for _, c := range s.channelCounts() {
		b.WriteString(fmt.Sprintf("gateway_channels_active{route=%q} %d\n", c.name, c.n))
	}
	b.WriteString("\n")

	b.WriteString("# HELP gateway_circuit_open Whether a channel's circuit breaker is open (1) or closed (0).\n")
	b.WriteString("# TYPE gateway_circuit_open gauge\n")
	for _, k := range s.circuitKeys() {
		v := 0
		if s.isCircuitOpen(ctx, k) {
			v = 1
		}
		b.WriteString(fmt.Sprintf("gateway_circuit_open{channel=%q} %d\n", k, v))
	}
	b.WriteString("\n")

	b.WriteString("# HELP gateway_quota_used Consumed quota units per API key.\n")
	b.WriteString("# TYPE gateway_quota_used gauge\n")
	b.WriteString("# HELP gateway_quota_limit Configured quota ceiling per API key (0 = unlimited).\n")
	b.WriteString("# TYPE gateway_quota_limit gauge\n")
	for _, q := range s.quotaSnapshot(ctx) {
		b.WriteString(fmt.Sprintf("gateway_quota_used{key_id=%q,owner=%q} %d\n",
			q.id, q.owner, q.used))
	}
	for _, q := range s.quotaSnapshot(ctx) {
		b.WriteString(fmt.Sprintf("gateway_quota_limit{key_id=%q,owner=%q} %d\n",
			q.id, q.owner, q.limit))
	}

	return b.String()
}

func writeHist(b *strings.Builder, name string, h histSnapshot) {
	for i, n := range h.counts {
		le := "+Inf"
		if i < len(latencyBuckets) {
			le = strconv.FormatFloat(latencyBuckets[i], 'g', -1, 64)
		}
		b.WriteString(fmt.Sprintf("%s_bucket{le=%q} %d\n", name, le, n))
	}
	b.WriteString(fmt.Sprintf("%s_sum %s\n", name, strconv.FormatFloat(h.sum, 'g', -1, 64)))
	b.WriteString(fmt.Sprintf("%s_count %d\n\n", name, h.count))
}

type quotaPoint struct {
	id, owner   string
	used, limit int64
}

type channelCount struct {
	name string
	n    int
}

func (s *Server) routeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Routes)
}

func (s *Server) channelCounts() []channelCount {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]channelCount, 0, len(s.Routes))
	for _, rt := range s.Routes {
		out = append(out, channelCount{name: rt.Name, n: len(rt.Channels)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// quotaSnapshot pairs each key's allowance with what it has consumed, so a
// Prometheus alert can watch burn rate without touching Postgres.
func (s *Server) quotaSnapshot(ctx context.Context) []quotaPoint {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, COALESCE(owner,''), quota_limit FROM api_keys WHERE status=1`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []quotaPoint
	for rows.Next() {
		var (
			id    int64
			owner string
			limit int64
		)
		if err := rows.Scan(&id, &owner, &limit); err != nil {
			continue
		}
		out = append(out, quotaPoint{
			id:    strconv.FormatInt(id, 10),
			owner: owner,
			used:  store.QuotaUsed(ctx, s.RDB, s.DB, id),
			limit: limit,
		})
	}
	return out
}
