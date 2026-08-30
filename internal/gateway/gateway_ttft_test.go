package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/store"
)

// delayedSSEUpstream is a fake LLM backend shaped like a real one: it idles for
// `think` before the first token, then dribbles `chunks` frames spaced `gap`
// apart, then closes with an OpenAI-style usage frame carrying prompt/completion.
//
// The idle period before the first frame is the whole point — that window is
// TTFT, and without it every assertion below would pass on a proxy that
// buffers the entire answer.
func delayedSSEUpstream(think time.Duration, chunks int, gap, prompt, completion int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		flush()
		time.Sleep(think)
		for i := 0; i < chunks; i++ {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"tok%d\"}}]}\n\n", i)
			flush()
			if gap > 0 {
				time.Sleep(time.Duration(gap) * time.Millisecond)
			}
		}
		fmt.Fprintf(w,
			`data: {"model":"gpt-4o-test","usage":{"prompt_tokens":%d,"completion_tokens":%d}}`+"\n\n"+
				"data: [DONE]\n\n", prompt, completion)
		flush()
	}))
}

// auditedOne drains exactly one audit entry, failing the test if the request
// produced none or more than one.
func auditedOne(t *testing.T, ch chan store.LogEntry) store.LogEntry {
	t.Helper()
	select {
	case e := <-ch:
		if extra := len(ch); extra > 0 {
			t.Fatalf("got %d extra audit entries, want exactly 1", extra)
		}
		return e
	default:
		t.Fatal("no audit entry was recorded")
		return store.LogEntry{}
	}
}

// TestStreamingRecordsTTFTAndThroughput is the headline assertion for the new
// columns: a streamed answer must report how long the user waited for the first
// token, and how fast the rest arrived.
//
// Both are only meaningful because the two are measured against different
// clocks — TTFT starts at the request, throughput starts at the first token.
func TestStreamingRecordsTTFTAndThroughput(t *testing.T) {
	up := delayedSSEUpstream(200*time.Millisecond, 3, 10, 11, 42)
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-stream"
	seedKey(t, rdb, raw, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{"stream":true}`))
	req.Header.Set("X-API-Key", raw)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if _, cerr := io.ReadAll(resp.Body); cerr != nil {
		t.Fatalf("read stream: %v", cerr)
	}
	wall := time.Since(start)

	e := auditedOne(t, audited)

	if !e.IsStream {
		t.Error("IsStream = false, want true: an SSE response must be flagged as streamed")
	}
	if e.CompletionTokens != 42 || e.PromptTokens != 11 {
		t.Errorf("tokens = %d/%d, want 11/42: the usage frame was not sniffed",
			e.PromptTokens, e.CompletionTokens)
	}
	if e.Model != "gpt-4o-test" {
		t.Errorf("model = %q, want gpt-4o-test", e.Model)
	}

	// The upstream idles 200ms before its first frame. Allow slack for
	// scheduling, but anything under 150ms means the timestamp is not actually
	// measuring the wait.
	if e.TTFTMs < 150 {
		t.Errorf("TTFT = %dms, want >= 150ms: the upstream stalls 200ms before "+
			"writing, so a much smaller number means TTFT is being stamped on "+
			"the wrong event", e.TTFTMs)
	}
	if int64(wall.Milliseconds())+50 < e.TTFTMs {
		t.Errorf("TTFT = %dms exceeds the whole request (%v) — TTFT can never be "+
			"larger than total latency", e.TTFTMs, wall)
	}
	if e.TokensPerSec <= 0 {
		t.Errorf("TokensPerSec = %v, want > 0", e.TokensPerSec)
	}
}

// TestBufferedRequestHasNoTTFT is the control for the test above.
//
// TTFT is defined as "time to the first *token*". A buffered answer hands over
// everything at once, so its TTFT is indistinguishable from its total latency
// and is deliberately recorded as 0 rather than a duplicate of latency_ms.
// Without this half, stamping TTFT on every response would look correct.
func TestBufferedRequestHasNoTTFT(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"model":"gpt-4o-test","usage":{"prompt_tokens":7,"completion_tokens":9}}`))
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-buffered"
	seedKey(t, rdb, raw, store.KeyInfo{})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/nl", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	e := auditedOne(t, audited)

	if e.IsStream {
		t.Error("IsStream = true, want false for a JSON response")
	}
	if e.TTFTMs != 0 {
		t.Errorf("TTFT = %dms, want 0: a buffered response has no first-token "+
			"event, so reporting one would be noise", e.TTFTMs)
	}
	// Throughput still applies — just over the whole request, not a generation
	// window. 16 tokens across ~80ms is ~200/s; the only hard requirement is
	// that it is positive.
	if e.TokensPerSec <= 0 {
		t.Errorf("TokensPerSec = %v, want > 0 for a buffered answer too", e.TokensPerSec)
	}
}

// TestThroughputExcludesTheWaitForFirstToken pins the arithmetic that makes
// throughput useful.
//
// Tokens do not flow during TTFT — the model is thinking. Dividing by total
// latency would blame the model's thinking time on its typing speed, and the
// number would swing wildly with prompt length instead of measuring the thing
// the user actually perceives. So: very long think, very fast typing.
func TestThroughputExcludesTheWaitForFirstToken(t *testing.T) {
	up := delayedSSEUpstream(400*time.Millisecond, 4, 25, 11, 42)
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-window"
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

	// Generation window is 4 frames x 25ms = ~100ms; 42 tokens over that is
	// ~420/s. Folding in the 400ms think time would report ~84/s instead.
	if e.TokensPerSec < 200 {
		t.Errorf("TokensPerSec = %.1f, want >= 200: the 400ms wait for the first "+
			"token must not be charged against generation speed (that would "+
			"report ~84/s)", e.TokensPerSec)
	}
	if e.TTFTMs < 350 {
		t.Errorf("TTFT = %dms, want >= 350ms", e.TTFTMs)
	}
}

// TestStreamingThroughputCountsOnlyOutputTokens pins the other half of the
// formula: the numerator is completion_tokens, not total_tokens.
//
// This is the half that is easy to get wrong without noticing. The prompt is
// read by the provider in microseconds and never "streams", so charging it
// against the generation window inflates throughput by however long the prompt
// was. With a 1000-token prompt and a 42-token answer the mistake is a 25x
// overstatement — big enough to catch, which a prompt of 11 tokens is not.
//
// An earlier version of this test only asserted a lower bound and happily
// passed while the code divided by total tokens. The upper bound is what makes
// it meaningful.
func TestStreamingThroughputCountsOnlyOutputTokens(t *testing.T) {
	// 6 frames x 20ms = ~120ms of generation for 42 tokens = ~350/s.
	up := delayedSSEUpstream(150*time.Millisecond, 6, 20, 1000, 42)
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-numerator"
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

	if e.PromptTokens != 1000 || e.CompletionTokens != 42 {
		t.Fatalf("tokens = %d/%d, want 1000/42: the usage frame was not sniffed",
			e.PromptTokens, e.CompletionTokens)
	}
	// Correct:  42 / ~120ms  = ~350/s
	// Bug A:  1042 / ~120ms  = ~8700/s  (prompt counted as generated)
	// Bug B:    42 / ~270ms  = ~155/s   (TTFT folded back into the window)
	if e.TokensPerSec < 200 || e.TokensPerSec > 800 {
		t.Errorf("TokensPerSec = %.1f, want roughly 350 (strictly between 200 and "+
			"800): the numerator must be the 42 generated tokens over the ~120ms "+
			"generation window — not the 1042 total, and not a window that "+
			"includes the wait for the first token", e.TokensPerSec)
	}
}

// TestTTFTSkipsKeepaliveAndRoleDelta is the regression for the whole point of
// measuring TTFT properly.
//
// A real provider's stream opens with housekeeping, not with an answer:
// a ": ping" keepalive, then an empty role delta announcing the assistant's
// turn. Both arrive the moment the request is accepted — long before the model
// has generated anything. Stamping TTFT on the first byte off the wire turns
// the metric into "how fast did the provider acknowledge us", which is
// meaningless to the person watching a spinner.
func TestTTFTSkipsKeepaliveAndRoleDelta(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		// Speak immediately: a keepalive comment and an empty role delta.
		flush()
		fmt.Fprint(w, ": ping\n\n")
		fmt.Fprint(w, `data: {"id":"1","model":"gpt-4o-test",`+
			`"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`+"\n\n")
		flush()

		// Then actually think. This 300ms is the wait the user experiences.
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, `data: {"choices":[{"index":0,"delta":{"content":"Hel"}}]}`+"\n\n")
		flush()
		fmt.Fprint(w, `data: {"model":"gpt-4o-test","usage":`+
			`{"prompt_tokens":10,"completion_tokens":42}}`+"\n\n"+"data: [DONE]\n\n")
		flush()
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-keepalive"
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

	if e.TTFTMs < 250 {
		t.Errorf("TTFT = %dms, want >= 250ms: the upstream acknowledges instantly "+
			"but stalls 300ms before generating, so a small number means TTFT is "+
			"measuring the keepalive/role frame instead of the first real token",
			e.TTFTMs)
	}
}

// TestTTFTFallsBackToFirstFrameForUnknownShape covers the provider we cannot
// parse. The fallback matters because the two failure modes are not equally
// bad: reporting a slightly early TTFT is a rounding error, while reporting 0
// reads as "instant" and hides a slow provider completely.
func TestTTFTFallsBackToFirstFrameForUnknownShape(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		flush()
		fmt.Fprint(w, ": ping\n\n")
		flush()

		time.Sleep(300 * time.Millisecond)
		// A shape no provider rule recognises, and with no text delta.
		fmt.Fprint(w, `data: {"vendor_specific":{"chunk":1}}`+"\n\n")
		flush()
		fmt.Fprint(w, `data: {"model":"m","usage":{"prompt_tokens":1,"completion_tokens":2}}`+"\n\n")
		flush()
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-unknown"
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

	if e.TTFTMs < 250 {
		t.Errorf("TTFT = %dms, want >= 250ms: an unrecognised frame shape must "+
			"fall back to the first data frame, not report 0 and imply the "+
			"provider answered instantly", e.TTFTMs)
	}
}

// TestNDJSONStreamIsSniffedToo guards a format gap rather than a timing one.
//
// isStreamed() accepts two wire formats, but the sniffer only ever understood
// one of them: it looks for lines starting with "data:", which is SSE framing.
// NDJSON (application/x-ndjson) delivers the same payloads as bare JSON lines,
// so every such frame was dropped — no usage, no model, no TTFT. A stream that
// bills zero tokens and reports zero latency is worse than no stream support
// at all, because it looks like it worked.
func TestNDJSONStreamIsSniffedToo(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		flush()
		// No SSE field names anywhere - one JSON value per line.
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, `{"choices":[{"delta":{"content":"Hel"}}]}`+"\n")
		flush()
		fmt.Fprint(w, `{"model":"gpt-4o-test","usage":{"prompt_tokens":10,"completion_tokens":42}}`+"\n")
		flush()
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-ndjson"
	id := seedKey(t, rdb, raw, store.KeyInfo{})

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

	if !e.IsStream {
		t.Error("IsStream = false, want true for an NDJSON response")
	}
	if e.CompletionTokens != 42 || e.PromptTokens != 10 {
		t.Errorf("tokens = %d/%d, want 10/42: NDJSON usage frames are being dropped, "+
			"so the request bills as zero tokens", e.PromptTokens, e.CompletionTokens)
	}
	if e.Model != "gpt-4o-test" {
		t.Errorf("model = %q, want gpt-4o-test", e.Model)
	}
	if e.TTFTMs < 250 {
		t.Errorf("TTFT = %dms, want >= 250ms: NDJSON frames are not being recognised, "+
			"so a streamed answer reports no first-token time at all", e.TTFTMs)
	}

	// The reason this is a bug and not a cosmetic gap: quota is charged from
	// the sniffed token count, and mwLogging falls back to 1 when it finds
	// none. An NDJSON call used to cost one token instead of fifty-two.
	used := store.QuotaUsed(context.Background(), rdb, nil, id)
	if used != 52 {
		t.Errorf("quota_used = %d, want 52 (10 prompt + 42 completion): the usage "+
			"frame was missed, so the request was charged the 1-token fallback", used)
	}
}

// TestEmptyStreamReportsNoTTFT covers the boundary where the upstream opens a
// stream then writes nothing — a cancelled generation, or a provider that
// sends only headers. There is no first byte, so there is no TTFT, and the
// row must not invent one.
func TestEmptyStreamReportsNoTTFT(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-ttft-empty"
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

	if !e.IsStream {
		t.Error("IsStream = false, want true: the response was still SSE")
	}
	if e.TTFTMs != 0 {
		t.Errorf("TTFT = %dms, want 0: nothing was ever written, so there is no "+
			"first-token timestamp to report", e.TTFTMs)
	}
	if e.TokensPerSec != 0 {
		t.Errorf("TokensPerSec = %v, want 0 with no tokens and no generation window", e.TokensPerSec)
	}
}

// TestCacheHitTokensExcludedFromQuota pins the billing rule that the gateway's
// quota mirrors what the upstream actually charges: prompt tokens served from
// the provider's own prefix cache (DeepSeek prompt_cache_hit_tokens, Anthropic
// cache_read_input_tokens, OpenAI cached_tokens) are billed at a steep
// discount, so charging them at full quota price would exhaust a key's
// allowance up to 10x earlier than the real bill.
//
// The sniffed cache-hit count still lands in the request log — only the quota
// charge is discounted.
func TestCacheHitTokensExcludedFromQuota(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		flush()
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n")
		flush()
		// 100 prompt tokens, 80 of them served from the provider's prefix
		// cache; 20 completion tokens. Full-price charge would be 120; the
		// discounted charge is (100-80)+20 = 40.
		fmt.Fprint(w, "data: {\"model\":\"deepseek-chat\",\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"prompt_cache_hit_tokens\":80}}\n\n")
		flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flush()
	}))
	defer up.Close()

	_, rdb, audited, ts := noLogServer(t, up)
	raw := "gw-quota-cachehit"
	id := seedKey(t, rdb, raw, store.KeyInfo{})

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
	if e.CacheHitTokens != 80 {
		t.Errorf("logged cache hit = %d, want 80: the log must keep the full "+
			"cache figure for the cost dashboard even though quota discounts it", e.CacheHitTokens)
	}

	used := store.QuotaUsed(context.Background(), rdb, nil, id)
	if used != 40 {
		t.Errorf("quota_used = %d, want 40 ((100-80)+20): provider-cached input "+
			"tokens are billed at a discount upstream, so charging full price here "+
			"would exhaust the allowance ~10x earlier than the real bill", used)
	}
}
