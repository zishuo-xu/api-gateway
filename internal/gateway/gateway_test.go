package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

func TestIsStreamed(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{"text/event-stream", true},
		{"text/event-stream; charset=utf-8", true},
		{"application/x-ndjson", true},
		{"application/json", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isStreamed(c.ct); got != c.want {
			t.Errorf("isStreamed(%q) = %v, want %v", c.ct, got, c.want)
		}
	}
}

func TestUpstreamTimeout(t *testing.T) {
	// A server with no config must still get a generous default; a 5s-style
	// value is what truncates real LLM answers.
	if got := (&Server{}).upstreamTimeout(); got != 180*time.Second {
		t.Errorf("default timeout = %v, want 180s", got)
	}
	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 45}}
	if got := s.upstreamTimeout(); got != 45*time.Second {
		t.Errorf("configured timeout = %v, want 45s", got)
	}
}

func newTestServer(t *testing.T, upstreamURL string) *httptest.Server {
	t.Helper()
	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 30}}
	s.Routes = []Route{{
		Name:              "test",
		BaseURL:           upstreamURL,
		MatchPrefix:       "/t",
		Upstream:          "test",
		APIFormat:         "openai-chat",
		DownstreamAuthKey: "sk-downstream-secret",
	}}
	return httptest.NewServer(http.HandlerFunc(s.proxy))
}

// The regression this guards: proxying used to io.ReadAll the whole body before
// writing anything, so SSE output only appeared once generation finished (and
// was then cut off by the client timeout).
func TestProxyFlushesSSEAsItArrives(t *testing.T) {
	const chunkDelay = 150 * time.Millisecond
	const chunks = 3

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < chunks; i++ {
			fmt.Fprintf(w, "data: chunk-%d\n\n", i)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(chunkDelay)
		}
	}))
	defer up.Close()

	gw := newTestServer(t, up.URL)
	defer gw.Close()

	start := time.Now()
	resp, err := http.Post(gw.URL+"/t/chat/completions", "application/json", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		t.Errorf("Content-Length = %q, want it stripped so the stream can chunk", cl)
	}

	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	firstChunkAt := time.Since(start)

	// Upstream needs chunks*delay to finish. A buffered proxy would not surface
	// the first byte until then; a flushing one shows up almost immediately.
	if firstChunkAt > chunkDelay {
		t.Errorf("first chunk arrived after %v (> %v) — response is being buffered, not streamed",
			firstChunkAt, chunkDelay)
	}
	if !strings.Contains(string(buf[:n]), "chunk-0") {
		t.Errorf("first chunk = %q, want chunk-0", string(buf[:n]))
	}

	rest, _ := io.ReadAll(resp.Body)
	total := time.Since(start)
	if got := strings.Count(string(buf[:n])+string(rest), "data: chunk-"); got != chunks {
		t.Errorf("received %d chunks, want %d", got, chunks)
	}
	if total < chunkDelay*chunks/2 {
		t.Errorf("finished in %v — suspiciously fast, test may not be exercising delays", total)
	}
}

func TestProxyBuffersPlainJSON(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer up.Close()

	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/t/chat/completions", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %q, want %q", body, `{"ok":true}`)
	}
}

// The gateway must never leak its own user key upstream, and must swap in the
// provider key the user is not allowed to see.
func TestProxySwapsKeysByFormat(t *testing.T) {
	const userKey = "user-key-abc"
	cases := []struct {
		format     string
		wantHeader string
		wantValue  string
	}{
		{"openai-chat", "Authorization", "Bearer sk-downstream-secret"},
		{"anthropic-messages", "X-Api-Key", "sk-downstream-secret"},
		{"generic", "X-Provider-Key", "sk-downstream-secret"},
	}
	for _, c := range cases {
		t.Run(c.format, func(t *testing.T) {
			var got http.Header
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			defer up.Close()

			s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 30}}
			s.Routes = []Route{{
				Name: "test", BaseURL: up.URL, MatchPrefix: "/t", Upstream: "test",
				APIFormat: c.format, DownstreamAuthKey: "sk-downstream-secret",
			}}
			gw := httptest.NewServer(http.HandlerFunc(s.proxy))
			defer gw.Close()

			req, _ := http.NewRequest(http.MethodPost, gw.URL+"/t/x", strings.NewReader(`{}`))
			req.Header.Set("X-API-Key", userKey)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()

			if v := got.Get(c.wantHeader); v != c.wantValue {
				t.Errorf("upstream %s = %q, want %q", c.wantHeader, v, c.wantValue)
			}
			// Check by value, not by header name: for Anthropic routes the header
			// the gateway injects (x-api-key) is the same name it strips from the
			// incoming request (X-API-Key), since HTTP headers are case-insensitive.
			for k, vv := range got {
				for _, v := range vv {
					if strings.Contains(v, userKey) {
						t.Errorf("user key leaked upstream via header %s = %q", k, v)
					}
				}
			}
		})
	}
}

func TestProxyInjectsAnthropicVersion(t *testing.T) {
	var got http.Header
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer up.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 30}}
	s.Routes = []Route{{
		Name: "test", BaseURL: up.URL, MatchPrefix: "/t", Upstream: "test",
		APIFormat: "anthropic-messages", DownstreamAuthKey: "sk-x",
	}}
	gw := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/t/v1/messages", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if v := got.Get("Anthropic-Version"); v != "2023-06-01" {
		t.Errorf("Anthropic-Version = %q, want 2023-06-01", v)
	}
}

func TestMatchRoutePrefersLongestPrefix(t *testing.T) {
	short := Route{Name: "short", MatchPrefix: "/v1", Upstream: "short"}
	long := Route{Name: "long", MatchPrefix: "/v1/messages", Upstream: "long"}

	// Load order must not change the outcome: previously the first match won, so
	// "/v1" declared first made "/v1/messages" permanently unreachable.
	for _, order := range [][]Route{{short, long}, {long, short}} {
		s := &Server{Cfg: &config.Config{}}
		s.Routes = order

		cases := []struct {
			path string
			want string
		}{
			{"/v1/messages", "long"},
			{"/v1/messages/xyz", "long"},
			{"/v1", "short"},
			{"/v1/other", "short"},
		}
		for _, c := range cases {
			got := s.matchRoute(c.path)
			if got == nil {
				t.Fatalf("order %v: matchRoute(%q) = nil, want %s", order, c.path, c.want)
			}
			if got.Name != c.want {
				t.Errorf("order %v: matchRoute(%q) = %s, want %s", order, c.path, got.Name, c.want)
			}
		}
		if got := s.matchRoute("/nope"); got != nil {
			t.Errorf("order %v: matchRoute(/nope) = %s, want nil", order, got.Name)
		}
	}
}

// A blank prefix would otherwise match every path and swallow all traffic.
func TestMatchRouteIgnoresEmptyPrefix(t *testing.T) {
	s := &Server{Cfg: &config.Config{}}
	s.Routes = []Route{{Name: "empty", MatchPrefix: ""}}
	if got := s.matchRoute("/anything"); got != nil {
		t.Errorf("matchRoute(/anything) = %s, want nil", got.Name)
	}
}

// End-to-end: the request must reach the upstream behind the longer prefix even
// though a shorter, broader prefix is configured too.
func TestProxyPicksLongestMatchingRoute(t *testing.T) {
	var hitsA, hitsB int32

	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hitsA, 1)
		_, _ = w.Write([]byte(`A`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hitsB, 1)
		_, _ = w.Write([]byte(`B`))
	}))
	defer b.Close()

	s := &Server{Cfg: &config.Config{UpstreamTimeoutSec: 30}}
	// Intentionally declare the broader prefix first — the old first-match-wins
	// behaviour would send everything here.
	s.Routes = []Route{
		{Name: "a", BaseURL: a.URL, MatchPrefix: "/v1", Upstream: "a"},
		{Name: "b", BaseURL: b.URL, MatchPrefix: "/v1/messages", Upstream: "b"},
	}
	gw := httptest.NewServer(http.HandlerFunc(s.proxy))
	defer gw.Close()

	get := func(path string) string {
		resp, err := http.Post(gw.URL+path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	if got := get("/v1/messages"); got != "B" {
		t.Errorf("/v1/messages -> %q, want upstream B", got)
	}
	if got := get("/v1/chat"); got != "A" {
		t.Errorf("/v1/chat -> %q, want upstream A", got)
	}
	if hitsA != 1 || hitsB != 1 {
		t.Errorf("hits A=%d B=%d, want 1 and 1", hitsA, hitsB)
	}
}
