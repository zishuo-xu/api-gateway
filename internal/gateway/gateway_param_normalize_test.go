package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Upstreams reject reasoning_effort on shape alone. Grok answers "Invalid
// reasoning effort." and DeepSeek wraps a provider error for anything they do
// not implement, and the rejection fails the whole call before the model ever
// sees it — a client that sent "HIGH" gets a 400, not a slower answer.
//
// These tests pin the contract: casing is folded to a level upstreams accept,
// levels nobody implements are dropped rather than forwarded, and the bytes we
// return are the bytes the upstream receives.

func TestNormalizeReasoningEffort(t *testing.T) {
	cases := []struct {
		name string
		// want is the expected reasoning_effort after normalisation:
		// a string = rewritten to that level, nil = the field must be gone.
		format string
		body   string
		want   interface{}
	}{
		{"uppercase level", "openai-chat", `{"model":"m","reasoning_effort":"HIGH"}`, "high"},
		{"mixed case", "openai-chat", `{"model":"m","reasoning_effort":"High"}`, "high"},
		{"padded level", "openai-chat", `{"model":"m","reasoning_effort":" high "}`, "high"},
		{"already canonical", "openai-chat", `{"model":"m","reasoning_effort":"high"}`, "high"},
		{"level not in the openai set", "openai-chat", `{"model":"m","reasoning_effort":"xhigh"}`, "xhigh"},
		{"minimal stays", "openai-chat", `{"model":"m","reasoning_effort":"minimal"}`, "minimal"},
		{"invented level dropped", "openai-chat", `{"model":"m","reasoning_effort":"auto"}`, nil},
		{"non-string dropped", "openai-chat", `{"model":"m","reasoning_effort":0}`, nil},
		{"field absent", "openai-chat", `{"model":"m"}`, nil},
		{"generic format untouched", "generic", `{"reasoning_effort":"HIGH"}`, "HIGH"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(c.body))
			out := normalizeReasoningEffort(req, &Route{APIFormat: c.format}, []byte(c.body))
			var m map[string]interface{}
			if err := json.Unmarshal(out, &m); err != nil {
				t.Fatalf("bad json out: %v", err)
			}
			got := m["reasoning_effort"]
			if got != c.want {
				t.Errorf("reasoning_effort = %v, want %v (body %s)", got, c.want, out)
			}
			// The bytes handed to the upstream must be the rewritten ones.
			b, _ := io.ReadAll(req.Body)
			if string(b) != string(out) {
				t.Errorf("r.Body = %s, want %s: rewritten body must reach upstream", b, out)
			}
			if req.ContentLength != int64(len(out)) {
				t.Errorf("ContentLength = %d, want %d: a stale length truncates or hangs the upstream read",
					req.ContentLength, len(out))
			}
		})
	}
}

// A body that already carries a canonical level must come out byte-identical.
// Re-marshalling reorders keys, and a rewrite on every request would make the
// proxy look like it is editing traffic it is only passing through.
func TestNormalizeReasoningEffortLeavesCleanBodyAlone(t *testing.T) {
	body := []byte(`{"model":"m","reasoning_effort":"high"}`)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(string(body)))
	out := normalizeReasoningEffort(req, &Route{APIFormat: "openai-chat"}, body)
	if string(out) != string(body) {
		t.Errorf("body rewritten to %s, want untouched %s", out, body)
	}
}

// An oversized body is only a truncated prefix. Re-marshalling a fragment would
// silently drop the rest of the request, so an unparseable body has to come
// back exactly as it went in — the caller guards the oversized case, this pins
// that the function itself refuses to guess.
func TestNormalizeReasoningEffortRejectsBrokenJSON(t *testing.T) {
	body := []byte(`{"model":"m","reasoning_effort":"HIGH"`)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(string(body)))
	out := normalizeReasoningEffort(req, &Route{APIFormat: "openai-chat"}, body)
	if string(out) != string(body) {
		t.Errorf("broken json rewritten to %s, want untouched %s", out, body)
	}
}

// A route whose own api_format is empty must inherit its channel's. Both
// injectStreamUsage and normalizeReasoningEffort gate on route.APIFormat, so
// leaving it empty disables them for a route that is plainly OpenAI-shaped —
// the Grok route ran for weeks with no stream_options injected because of this.
func TestAdoptChannelFormat(t *testing.T) {
	cases := []struct {
		name     string
		routeFmt string
		chanFmt  string
		withChan bool
		want     string
	}{
		{"empty route adopts channel", "", "openai-chat", true, "openai-chat"},
		{"declared route untouched", "generic", "openai-chat", true, "generic"},
		{"no channel to adopt from", "", "openai-chat", false, ""},
		{"channel also empty", "", "", true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rt := &Route{APIFormat: c.routeFmt}
			if c.withChan {
				rt.Channels = []Channel{{APIFormat: c.chanFmt}}
			}
			adoptChannelFormat(rt)
			if rt.APIFormat != c.want {
				t.Errorf("APIFormat = %q, want %q", rt.APIFormat, c.want)
			}
		})
	}
}
