package gateway

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zishuo-xu/api-gateway/internal/store"
)

// TestUsageFromJSONReadsEveryPromptCacheDialect is the core of cache
// collection: all three provider shapes have to land on the same two numbers.
//
// The controls matter as much as the positives. An empty prompt_tokens_details
// is exactly what opencode.ai returns on a cold call, and it has to record 0
// rather than being treated as "no data" - a cold call is a real measurement,
// and it is the one someone staring at a cost dashboard most needs to see.
func TestUsageFromJSONReadsEveryPromptCacheDialect(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantPrompt int64
		wantHit    int64
		wantWrite  int64
	}{
		{
			name:       "deepseek hit/miss pair",
			body:       `{"model":"deepseek-chat","usage":{"prompt_tokens":41427,"completion_tokens":378,"prompt_cache_hit_tokens":38000,"prompt_cache_miss_tokens":3427}}`,
			wantPrompt: 41427, wantHit: 38000,
		},
		{
			// Anthropic splits the input three ways, so input_tokens alone is
			// only the tail after the last cache breakpoint.
			name:       "anthropic read",
			body:       `{"usage":{"input_tokens":50,"output_tokens":12,"cache_read_input_tokens":100000,"cache_creation_input_tokens":0}}`,
			wantPrompt: 100050, wantHit: 100000,
		},
		{
			name:       "anthropic write on a cold prefix",
			body:       `{"usage":{"input_tokens":50,"output_tokens":12,"cache_read_input_tokens":0,"cache_creation_input_tokens":2095}}`,
			wantPrompt: 2145, wantHit: 0, wantWrite: 2095,
		},
		{
			name:       "anthropic sse message_start",
			body:       `{"type":"message_start","message":{"model":"claude-3","usage":{"input_tokens":50,"cache_read_input_tokens":100000}}}`,
			wantPrompt: 100050, wantHit: 100000,
		},
		{
			name:       "openai nested details",
			body:       `{"model":"gpt-4o","usage":{"prompt_tokens":5000,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":4600}}}`,
			wantPrompt: 5000, wantHit: 4600,
		},
		{
			// The exact shape opencode.ai returned on a cold call: the key is
			// present but empty, so it must not be read as "provider has no
			// cache support".
			name:       "openai empty details means a cold call, not missing data",
			body:       `{"model":"deepseek-v4-flash","usage":{"prompt_tokens":84,"completion_tokens":33,"total_tokens":117,"prompt_tokens_details":{}}}`,
			wantPrompt: 84, wantHit: 0,
		},
		{
			name:       "no cache fields at all",
			body:       `{"model":"x","usage":{"prompt_tokens":10,"completion_tokens":2}}`,
			wantPrompt: 10, wantHit: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := usageFromJSON([]byte(c.body))
			if f.prompt != c.wantPrompt {
				t.Errorf("prompt = %d, want %d", f.prompt, c.wantPrompt)
			}
			if f.cacheHit != c.wantHit {
				t.Errorf("cacheHit = %d, want %d: the provider's cache column was dropped",
					f.cacheHit, c.wantHit)
			}
			if f.cacheWrite != c.wantWrite {
				t.Errorf("cacheWrite = %d, want %d", f.cacheWrite, c.wantWrite)
			}
		})
	}
}

// TestAnthropicTotalInputIsNotDoubleCounted pins the arithmetic that makes the
// Anthropic adjustment safe.
//
// A stream carries the same usage block more than once (message_start, then a
// final message), and the walk visits every occurrence. Summing on each visit
// would report 200050 input tokens for a 100050-token request - an overcharge
// that only appears under caching, which is exactly when the numbers are
// largest and least likely to be eyeballed.
func TestAnthropicTotalInputIsNotDoubleCounted(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int64
	}{
		{
			name: "same figures in two blocks",
			body: `{"type":"message_start","message":{"usage":{"input_tokens":50,"cache_read_input_tokens":100000}},` +
				`"usage":{"input_tokens":50,"cache_read_input_tokens":100000}}`,
			want: 100050,
		},
		{
			// The block carrying the cache figures is not always the one with
			// the largest tail, and by the time it is visited the running
			// prompt total may already be higher. Building the sum from the
			// block's own input_tokens - rather than from the running total -
			// is what keeps the two from being conflated.
			name: "cache block has a smaller tail than an earlier block",
			body: `{"usage":{"input_tokens":500},` +
				`"message":{"usage":{"input_tokens":50,"cache_read_input_tokens":100000}}}`,
			want: 100050,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := usageFromJSON([]byte(c.body))
			if f.prompt != c.want {
				t.Errorf("prompt = %d, want %d: the cached prefix was combined with "+
					"the wrong tail, so it was counted once per usage block or "+
					"against another block's input_tokens", f.prompt, c.want)
			}
		})
	}
}

// TestPromptCacheReachesTheLogRow is the end-to-end check: an upstream that
// reports a cached prefix must have that number survive parsing, the proxy and
// the audit record. Both response modes are covered because they take
// different code paths - the buffered one parses the whole body, the streamed
// one sniffs frames as they go by.
func TestPromptCacheReachesTheLogRow(t *testing.T) {
	t.Run("buffered", func(t *testing.T) {
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"model":"deepseek-chat","choices":[{"message":{"content":"ok"}}],` +
				`"usage":{"prompt_tokens":41427,"completion_tokens":378,` +
				`"prompt_cache_hit_tokens":38000,"prompt_cache_miss_tokens":3427}}`))
		}))
		defer up.Close()

		_, rdb, audited, ts := noLogServer(t, up)
		raw := "gw-cache-buffered"
		seedKey(t, rdb, raw, store.KeyInfo{})

		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		if _, cerr := io.ReadAll(resp.Body); cerr != nil {
			t.Fatalf("read body: %v", cerr)
		}

		e := auditedOne(t, audited)
		if e.PromptTokens != 41427 {
			t.Errorf("prompt_tokens = %d, want 41427", e.PromptTokens)
		}
		if e.CacheHitTokens != 38000 {
			t.Errorf("prompt_cache_hit_tokens = %d, want 38000: the discounted "+
				"share of the input was dropped on the way to the log",
				e.CacheHitTokens)
		}
		if e.CacheWriteTokens != 0 {
			t.Errorf("prompt_cache_write_tokens = %d, want 0: DeepSeek does not "+
				"report a write figure", e.CacheWriteTokens)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flush := func() {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			flush()
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
			flush()
			// Anthropic's dialect, delivered over SSE: read plus write, with
			// input_tokens carrying only the tail.
			_, _ = w.Write([]byte("data: {\"model\":\"claude-3\",\"usage\":{\"input_tokens\":50," +
				"\"output_tokens\":12,\"cache_read_input_tokens\":100000," +
				"\"cache_creation_input_tokens\":2095}}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flush()
		}))
		defer up.Close()

		_, rdb, audited, ts := noLogServer(t, up)
		raw := "gw-cache-stream"
		seedKey(t, rdb, raw, store.KeyInfo{})

		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{"stream":true}`))
		req.Header.Set("X-API-Key", raw)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		if _, cerr := io.ReadAll(resp.Body); cerr != nil {
			t.Fatalf("read stream: %v", cerr)
		}

		e := auditedOne(t, audited)
		if e.PromptTokens != 102145 {
			t.Errorf("prompt_tokens = %d, want 102145 (50 + 100000 read + 2095 write): "+
				"Anthropic reports only the tail in input_tokens, so the cached "+
				"prefix was never added", e.PromptTokens)
		}
		if e.CacheHitTokens != 100000 {
			t.Errorf("prompt_cache_hit_tokens = %d, want 100000", e.CacheHitTokens)
		}
		if e.CacheWriteTokens != 2095 {
			t.Errorf("prompt_cache_write_tokens = %d, want 2095", e.CacheWriteTokens)
		}
	})
}

// A provider that streams SSE while labelling the response application/json
// gets read by the buffered path, where a whole SSE transcript is not valid
// JSON. Before the fallback every token count came out zero and the request
// fell back to a flat 1-unit charge — on a 40k-token agent turn, that is the
// difference between an accurate bill and a rounding error.
func TestSSELabelledAsJSONStillYieldsUsage(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The header is the point of the test: it is what makes the gateway
		// treat this body as buffered.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(
			"data: {\"model\":\"deepseek-v4-flash\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
				"data: {\"model\":\"deepseek-v4-flash\",\"choices\":[]," +
				"\"usage\":{\"prompt_tokens\":41000,\"completion_tokens\":378," +
				"\"prompt_cache_hit_tokens\":38000}}\n\n" +
				"data: [DONE]\n\n"))
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-sse-as-json"
	seedKey(t, rdb, raw, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if _, cerr := io.ReadAll(resp.Body); cerr != nil {
		t.Fatalf("read body: %v", cerr)
	}

	e := auditedOne(t, audited)
	if e.Model != "deepseek-v4-flash" {
		t.Errorf("model = %q, want deepseek-v4-flash", e.Model)
	}
	if e.PromptTokens != 41000 {
		t.Errorf("prompt_tokens = %d, want 41000: an SSE body mislabelled as "+
			"JSON was metered as nothing", e.PromptTokens)
	}
	if e.CompletionTokens != 378 {
		t.Errorf("completion_tokens = %d, want 378", e.CompletionTokens)
	}
	if e.CacheHitTokens != 38000 {
		t.Errorf("prompt_cache_hit_tokens = %d, want 38000", e.CacheHitTokens)
	}
	// It was a stream regardless of what the header claimed, and the log
	// should say so: latency and throughput mean different things for one.
	if !e.IsStream {
		t.Errorf("IsStream = false, want true")
	}
}

// A gzipped upstream body reached the usage parsers as binary noise: the
// client's Accept-Encoding was relayed verbatim, and a transport that sees a
// caller-set Accept-Encoding assumes the caller handles decompression. The
// row then showed an empty model, zero tokens, and a quota charge silently
// falling back to a single unit.
//
// Two ways to be fixed, both covered: dropping the relayed header lets the
// transport negotiate and transparently decompress, and the magic-byte
// fallback catches an upstream that compresses without saying so.
func TestGzippedUpstreamBodyStillYieldsUsage(t *testing.T) {
	cases := []struct {
		name    string
		declare bool // advertise Content-Encoding: gzip, or compress silently
	}{
		{"declared", true},
		{"silent", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"model":"deepseek-v4-flash","choices":[{"message":{"content":"ok"}}],` +
				`"usage":{"prompt_tokens":41427,"completion_tokens":378}}`)
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, _ = zw.Write(payload)
			_ = zw.Close()

			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Compression is unconditional, exactly the upstream behaviour
				// that broke this: it gzips whatever it feels like.
				if tc.declare {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(buf.Bytes())
			}))
			defer up.Close()

			_, rdb, audited, ts := noLogServer(t, up)
			raw := "gw-gzip-" + tc.name
			seedKey(t, rdb, raw, store.KeyInfo{})

			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
			req.Header.Set("X-API-Key", raw)
			req.Header.Set("Accept-Encoding", "gzip") // what the real client sends
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			if _, cerr := io.ReadAll(resp.Body); cerr != nil {
				t.Fatalf("read body: %v", cerr)
			}

			e := auditedOne(t, audited)
			if e.Model != "deepseek-v4-flash" {
				t.Errorf("model = %q, want deepseek-v4-flash", e.Model)
			}
			if e.PromptTokens != 41427 {
				t.Errorf("prompt_tokens = %d, want 41427: a gzipped body was metered as nothing", e.PromptTokens)
			}
			if e.CompletionTokens != 378 {
				t.Errorf("completion_tokens = %d, want 378", e.CompletionTokens)
			}
		})
	}
}
