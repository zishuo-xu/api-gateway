package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---- route connection test -------------------------------------------------
//
// Why this exists: a misconfigured route used to be discoverable only by
// sending real traffic through it and reading the request log afterwards. That
// made a wrong provider key look like an application bug, and it made the route
// table's reload delay look like the fix "not working".
//
// The probe answers four questions in one click:
//
//  1. is the upstream reachable at all?
//  2. does it accept the credential we would send?
//  3. which credential is that — the channel's or the route's?
//  4. is it the one the running process would actually use right now?
//
// Point 3 is not academic. injectCredentials prefers ch.DownstreamAuthKey and
// only falls back to route.DownstreamAuthKey, so a key fixed on the route stays
// invisible while a stale channel key keeps answering 401. Point 4 exists for
// the mirror image: the route table lives in memory, so a direct SQL edit is
// invisible to the process until the next reload sweep.

const (
	// A probe is a deliberate button press, so a few seconds is acceptable —
	// but it must never be long enough to look like a hung console.
	routeTestTimeout = 15 * time.Second
	// Enough for any provider's error envelope. Never read a provider's whole
	// /models listing: some are megabytes and we throw the answer away anyway.
	routeTestBodyLimit = 8 << 10
	// Ceiling for a whole-route test. Channels are probed in parallel, so this
	// is wall-clock, not per-channel.
	routeTestBudget = 20 * time.Second
	// Longest error message rendered into the console. Providers occasionally
	// echo the request back inside the error, so this is a real limit, not a
	// theoretical one.
	routeTestMsgLimit = 300
)

// routeTestProbeHint explains what the test can and cannot prove. It travels
// with every response so the console never implies more than the probe did.
const routeTestProbeHint = "探针为 GET {base}/models：不消耗 token，密钥有效即返回 200。" +
	"没有该端点的上游（返回 404/405）只能确认地址可达，无法验证密钥。"

type routeTestRequest struct {
	RouteID   int64 `json:"route_id"`
	ChannelID int64 `json:"channel_id"`
}

// routeTestResult is the outcome of one probe.
//
// Kind is deliberately narrower than the HTTP status: for a connection test the
// useful answer is "will a real request through this route work", not "which
// status code came back". The console colours on Kind, not on StatusCode.
type routeTestResult struct {
	Target      string `json:"target"` // "channel", or "route" for the synthetic fallback
	RouteID     int64  `json:"route_id"`
	RouteName   string `json:"route_name"`
	ChannelID   int64  `json:"channel_id"`
	ChannelName string `json:"channel_name"`

	OK      bool   `json:"ok"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`

	StatusCode  int    `json:"status_code"`
	LatencyMS   int64  `json:"latency_ms"`
	ProbeURL    string `json:"probe_url"`
	NetError    string `json:"net_error"`
	UpstreamMsg string `json:"upstream_msg"`

	Format    string `json:"format"`
	KeySource string `json:"key_source"` // channel / route / none
	KeyFP     string `json:"key_fp"`     // prefix + length; never the key itself

	// What the running process would use right now. Equal to the values above
	// unless someone edited the database directly.
	LiveKeySource string `json:"live_key_source"`
	LiveKeyFP     string `json:"live_key_fp"`
	LiveBaseURL   string `json:"live_base_url"`
	Stale         bool   `json:"stale"`
	StaleNote     string `json:"stale_note"`
}

type routeTestResponse struct {
	Results   []routeTestResult `json:"results"`
	TestedAt  string            `json:"tested_at"`
	ProbeHint string            `json:"probe_hint"`
}

// adminRouteTest probes a route's channels (or a single channel) against the
// provider. It reads the database, not the in-memory table, so it reports on
// what was saved — and separately reports any drift from what is live.
func (s *Server) adminRouteTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in routeTestRequest
	if err := decodeOptional(r, &in); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if in.RouteID == 0 && in.ChannelID == 0 {
		http.Error(w, "route_id or channel_id is required", http.StatusBadRequest)
		return
	}

	route, channels, err := s.loadTestTargets(r.Context(), in)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "找不到这条路由或渠道（可能已被删除）", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := make([]routeTestResult, len(channels))
	ctx, cancel := context.WithTimeout(r.Context(), routeTestBudget)
	defer cancel()

	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(i int, ch Channel) {
			defer wg.Done()
			results[i] = s.probeChannel(ctx, route, ch)
		}(i, ch)
	}
	wg.Wait()

	for _, res := range results {
		log.Printf("route test: route=%d(%s) ch=%d(%s) kind=%s status=%d ok=%v stale=%v",
			res.RouteID, res.RouteName, res.ChannelID, res.ChannelName,
			res.Kind, res.StatusCode, res.OK, res.Stale)
	}

	writeJSON(w, routeTestResponse{
		Results:   results,
		TestedAt:  time.Now().Format(time.RFC3339),
		ProbeHint: routeTestProbeHint,
	})
}

// loadTestTargets reads the probe targets straight from the database.
//
// It never consults s.Routes for the values to send: the whole point is to
// validate what was saved. The live table is compared afterwards, in
// probeChannel, and any difference is surfaced as drift.
func (s *Server) loadTestTargets(ctx context.Context, in routeTestRequest) (*Route, []Channel, error) {
	if s.DB == nil {
		return nil, nil, errors.New("未配置数据库")
	}
	route := new(Route)
	id := in.RouteID
	if in.ChannelID != 0 {
		// Resolve the channel first so a channel-level test also pins the route
		// it belongs to. The console sends one id or the other, never both.
		if err := s.DB.QueryRowContext(ctx,
			`SELECT route_id FROM channels WHERE id=$1`, in.ChannelID).Scan(&id); err != nil {
			return nil, nil, fmt.Errorf("channel #%d: %w", in.ChannelID, err)
		}
	}
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, name, base_url, api_format, COALESCE(downstream_auth_key,'')
		FROM routes WHERE id=$1 AND status=1`, id).
		Scan(&route.ID, &route.Name, &route.BaseURL, &route.APIFormat, &route.DownstreamAuthKey)
	if err != nil {
		return nil, nil, fmt.Errorf("route #%d: %w", id, err)
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, route_id, COALESCE(name,''), base_url, api_format,
		       COALESCE(downstream_auth_key,''), weight, priority, enabled
		FROM channels WHERE route_id=$1 ORDER BY priority, id`, route.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("channels of route #%d: %w", route.ID, err)
	}
	defer rows.Close()
	var chans []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.RouteID, &c.Name, &c.BaseURL, &c.APIFormat,
			&c.DownstreamAuthKey, &c.Weight, &c.Priority, &c.Enabled); err != nil {
			return nil, nil, fmt.Errorf("channel row: %w", err)
		}
		chans = append(chans, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("channels of route #%d: %w", route.ID, err)
	}

	// A route with no channels forwards to its own base_url. That is the only
	// meaningful target, so synthesise the implicit channel the forwarding path
	// would build — otherwise a channelless route would report "nothing to test".
	if len(chans) == 0 {
		chans = []Channel{{
			RouteID:           route.ID,
			Name:              route.Name,
			BaseURL:           route.BaseURL,
			APIFormat:         route.APIFormat,
			DownstreamAuthKey: route.DownstreamAuthKey,
			Weight:            1,
			Enabled:           true,
		}}
	}
	if in.ChannelID != 0 {
		for _, c := range chans {
			if c.ID == in.ChannelID {
				return route, []Channel{c}, nil
			}
		}
		return nil, nil, fmt.Errorf("channel #%d does not belong to route #%d", in.ChannelID, route.ID)
	}
	return route, chans, nil
}

// probeChannel sends one GET /models and turns the answer into a verdict.
func (s *Server) probeChannel(ctx context.Context, route *Route, ch Channel) routeTestResult {
	// Per-probe ceiling on top of the caller's whole-route budget: one hung
	// provider must not eat the time the others need to answer at all.
	ctx, cancel := context.WithTimeout(ctx, routeTestTimeout)
	defer cancel()

	res := routeTestResult{
		Target:      "channel",
		RouteID:     route.ID,
		RouteName:   route.Name,
		ChannelID:   ch.ID,
		ChannelName: ch.Label(),
	}
	if ch.ID == 0 {
		res.Target = "route"
		res.ChannelName = route.Name + "（路由自身）"
	}

	base := strings.TrimRight(ch.BaseURL, "/")
	if base == "" {
		base = strings.TrimRight(route.BaseURL, "/")
	}
	// Key and format resolve exactly the way injectCredentials does. Copying
	// that precedence is the entire value of this test: if they ever diverge, a
	// green result here would be a lie.
	key, keySource := ch.DownstreamAuthKey, "channel"
	if key == "" {
		key, keySource = route.DownstreamAuthKey, "route"
	}
	if key == "" {
		keySource = "none"
	}
	format := ch.APIFormat
	if format == "" {
		format = route.APIFormat
	}
	res.Format = format
	res.KeySource = keySource
	res.KeyFP = keyFingerprint(key)
	res.attachLive(s, route, ch, base, key)

	if base == "" {
		res.Kind = "misconfig"
		res.Summary = "未配置 Base URL，转发时没有目标地址"
		return res
	}

	probeURL := base + "/models"
	res.ProbeURL = probeURL

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		res.Kind = "unreachable"
		res.NetError = err.Error()
		res.Summary = "无法构造探测请求：" + err.Error()
		return res
	}
	req.Header.Set("Accept", "application/json")
	setProbeAuth(req, format, key)

	start := time.Now()
	resp, err := s.httpClient().Do(req)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Kind = "unreachable"
		res.NetError = err.Error()
		res.Summary = "连不上上游：" + err.Error()
		return res
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, routeTestBodyLimit))
	res.StatusCode = resp.StatusCode
	res.UpstreamMsg = upstreamMessage(body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		res.OK = true
		res.Kind = "ok"
		if key == "" {
			res.Summary = "地址可达。注意：渠道和路由都没有配置密钥，转发时不会带任何鉴权头"
		} else {
			res.Summary = "连通，密钥有效"
		}
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		res.Kind = "auth_fail"
		res.Summary = "密钥被上游拒绝"
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusMethodNotAllowed,
		resp.StatusCode == http.StatusNotImplemented:
		// Reaching a 404 means the network path, DNS and TLS all worked — only
		// the probe endpoint is missing. Report it as inconclusive rather than
		// failed, so a generic upstream is not flagged red for being generic.
		res.Kind = "no_probe"
		res.Summary = "地址可达，但该上游没有 /models 端点，无法用探针验证密钥"
	case resp.StatusCode >= 500:
		res.Kind = "upstream_error"
		res.Summary = "上游服务异常"
	default:
		res.Kind = "upstream_error"
		res.Summary = "上游返回 " + strconv.Itoa(resp.StatusCode)
	}
	if !res.OK && res.Stale {
		// Drift is the first thing to rule out: a 401 against a key that is not
		// even loaded yet sends people chasing the wrong hypothesis.
		res.Summary += "。但这份配置尚未生效，测的是数据库里的值"
	}
	return res
}

// attachLive records what the process would actually use, and flags the drift.
func (res *routeTestResult) attachLive(s *Server, route *Route, ch Channel, base, key string) {
	if s == nil {
		return
	}
	live := s.liveSnapshot(route.ID, ch.ID)
	res.LiveKeySource = live.keySource
	res.LiveKeyFP = keyFingerprint(live.key)
	res.LiveBaseURL = live.baseURL
	switch {
	case !live.found:
		// Not in memory at all: brand new, disabled, or edited by hand. Either
		// way the operator must know this probe describes something the gateway
		// is not currently serving.
		res.Stale = true
		res.StaleNote = "内存中没有这份配置。路由表每 10 秒检查一次变更，最长 5 分钟强制重载；" +
			"想立即生效可在服务器执行 docker exec deploy-redis-1 redis-cli INCR routes:version"
	case live.key != key:
		res.Stale = true
		res.StaleNote = "内存里生效的密钥与刚测的不同。改动需等待重载（最长 5 分钟）；" +
			"想立即生效可在服务器执行 docker exec deploy-redis-1 redis-cli INCR routes:version"
	case live.baseURL != base:
		res.Stale = true
		res.StaleNote = "内存里生效的 Base URL 与刚测的不同，改动需等待重载"
	}
}

// setProbeAuth mirrors injectCredentials exactly. Sending the header a real
// request would send is the only way the verdict means anything.
func setProbeAuth(req *http.Request, format, key string) {
	if key == "" {
		return
	}
	switch format {
	case "anthropic-messages":
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
	case "openai-chat", "openai-responses", "openai-embeddings", "openai-completions", "embeddings":
		req.Header.Set("Authorization", "Bearer "+key)
	default:
		req.Header.Set("X-Provider-Key", key)
	}
}

type liveSnapshot struct {
	found     bool
	key       string
	keySource string
	baseURL   string
}

// liveSnapshot is the config the running process would use right now.
func (s *Server) liveSnapshot(routeID, channelID int64) liveSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.Routes {
		if rt.ID != routeID {
			continue
		}
		for _, c := range rt.Channels {
			if c.ID != channelID {
				continue
			}
			key, src := c.DownstreamAuthKey, "channel"
			if key == "" {
				key, src = rt.DownstreamAuthKey, "route"
			}
			if key == "" {
				src = "none"
			}
			base := strings.TrimRight(c.BaseURL, "/")
			if base == "" {
				base = strings.TrimRight(rt.BaseURL, "/")
			}
			return liveSnapshot{found: true, key: key, keySource: src, baseURL: base}
		}
	}
	return liveSnapshot{}
}

// keyFingerprint identifies a key well enough to tell two apart without ever
// revealing it. Short keys are shown in full — there is nothing left to hide.
func keyFingerprint(key string) string {
	if key == "" {
		return ""
	}
	const keep = 8
	n := strconv.Itoa(len(key))
	if len(key) <= keep+4 {
		return key + "（" + n + " 字符）"
	}
	return key[:keep] + "…" + key[len(key)-2:] + "（" + n + " 字符）"
}

// upstreamMessage pulls the human-readable reason out of an error body.
//
// Providers disagree on the envelope, so try the shapes that cover nearly
// everything and fall back to raw text. This is what turned a bare "401" into
// "invalid_authentication_error" during the k3 investigation.
func upstreamMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	// Applied to every path, not just the raw-text fallback: a provider that
	// echoes the offending prompt back inside its error message would otherwise
	// push the whole request body into the console.
	trim := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) > routeTestMsgLimit {
			s = s[:routeTestMsgLimit] + "…"
		}
		return s
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
		Message string `json:"message"`
		Msg     string `json:"msg"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		parts := make([]string, 0, 3)
		if envelope.Error.Type != "" {
			parts = append(parts, envelope.Error.Type)
		}
		if envelope.Error.Code != "" {
			parts = append(parts, envelope.Error.Code)
		}
		if envelope.Error.Message != "" {
			parts = append(parts, envelope.Error.Message)
		}
		if len(parts) == 0 && envelope.Message != "" {
			parts = append(parts, envelope.Message)
		}
		if len(parts) == 0 && envelope.Msg != "" {
			parts = append(parts, envelope.Msg)
		}
		if len(parts) > 0 {
			return trim(strings.Join(parts, " · "))
		}
		// Valid JSON we cannot summarise. Echoing the raw blob would fill the
		// dialog with noise; the status code is already on screen.
		return ""
	}
	// Not JSON at all — an HTML error page or a bare line of text. Worth
	// showing, since it is often the only clue a misconfigured proxy gives.
	return trim(string(body))
}
