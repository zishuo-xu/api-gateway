package gateway

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// The unified entry picks its upstream from the "model" in the request body, so
// a body the gateway cannot read is a request it cannot route. These tests pin
// down what "cannot read" is allowed to mean: a body cut short, or one that
// arrives compressed, must still give up its model. Both used to come back as
// `unified entry needs a "model" in the request body` for a request that
// plainly carried one — indistinguishable in the logs from a client that
// genuinely forgot the field.

func TestExtractModel(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		wantModel string
		wantSeen  bool
	}{
		{
			name:      "complete body",
			body:      `{"model":"model-a","messages":[]}`,
			wantModel: "model-a", wantSeen: true,
		},
		{
			name:      "model last",
			body:      `{"messages":[],"stream":true,"model":"model-a"}`,
			wantModel: "model-a", wantSeen: true,
		},
		{
			// The whole point: a body cut off mid-message still has a model.
			name:      "truncated after the model",
			body:      `{"model":"model-a","messages":[{"role":"user","con`,
			wantModel: "model-a", wantSeen: true,
		},
		{
			// Cut before the field: genuinely unreadable, and the caller has
			// to be told so rather than being handed a confident "no model".
			name:      "truncated before the model",
			body:      `{"messages":[{"role":"user","content":"aaaaaaaaaaa`,
			wantModel: "", wantSeen: false,
		},
		{
			// A model inside a message or a tool definition is not the
			// request's model.
			name:      "nested model is not the request model",
			body:      `{"messages":[{"role":"user","model":"decoy"}],"model":"model-a"}`,
			wantModel: "model-a", wantSeen: true,
		},
		{
			name:      "nested model only",
			body:      `{"messages":[{"role":"user","model":"decoy"}],"stream":false}`,
			wantModel: "", wantSeen: true,
		},
		{
			name:      "not json at all",
			body:      "this is not json",
			wantModel: "", wantSeen: false,
		},
		{
			name:      "json array",
			body:      `[{"model":"model-a"}]`,
			wantModel: "", wantSeen: false,
		},
		{
			name:      "empty body",
			body:      "",
			wantModel: "", wantSeen: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, seen := extractModel([]byte(c.body))
			if got != c.wantModel || seen != c.wantSeen {
				t.Errorf("extractModel(%q) = (%q,%v), want (%q,%v)",
					truncate(c.body, 40), got, seen, c.wantModel, c.wantSeen)
			}
		})
	}
}

// A request past the inspect cap is the case that hit production: the gateway
// peeked 1 MiB, tried to unmarshal the truncated result, and rejected the call.
func TestUnifiedEntryRoutesOversizedBody(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	// Well past maxInspectBody, model first - the shape every OpenAI client sends.
	body := fmt.Sprintf(`{"model":"model-a","messages":[{"role":"user","content":%q}]}`,
		strings.Repeat("x", 2<<20))

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d %s, want 200: an oversized body must still route by model", resp.StatusCode, strings.TrimSpace(string(got)))
	}
	if !strings.Contains(string(got), `"served_by":"a"`) {
		t.Errorf("body = %s, want it served by provider a", got)
	}
}

// An oversized body must still be held to the allowlist: recovering the model
// from a prefix is not licence to route it anywhere.
func TestUnifiedEntryStillRejectsUnknownModelInOversizedBody(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	body := fmt.Sprintf(`{"model":"nope","messages":[{"role":"user","content":%q}]}`,
		strings.Repeat("x", 2<<20))
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 unknown_model", resp.StatusCode)
	}
	if a.requests.Load() != 0 || b.requests.Load() != 0 {
		t.Errorf("upstream calls a=%d b=%d, want 0 0", a.requests.Load(), b.requests.Load())
	}
}

// A client that compresses its request body is speaking to the upstream, which
// agreed to it; the gateway only needs to read along well enough to route.
func TestUnifiedEntryRoutesGzippedBody(t *testing.T) {
	a := newUpstream(t, "a", http.StatusOK, "")
	b := newUpstream(t, "b", http.StatusOK, "")
	ts := newUnifiedServer(t, "/v1", a, b)
	defer ts.Close()

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(`{"model":"model-a","messages":[]}`)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d %s, want 200: a gzipped body must still route by model", resp.StatusCode, strings.TrimSpace(string(got)))
	}
	if !strings.Contains(string(got), `"served_by":"a"`) {
		t.Errorf("body = %s, want it served by provider a", got)
	}
}

func TestInspectableBodyDecompressesOnlyGzip(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte(`{"model":"gz-model","messages":[]}`))
	_ = zw.Close()

	req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(buf.Bytes()))
	got := inspectableBody(req, buf.Bytes())
	if !strings.Contains(string(got), "gz-model") {
		t.Errorf("inspectableBody(gzip) = %q, want the decompressed JSON", truncate(string(got), 60))
	}

	// Plain bytes must pass through untouched - decompressing something that
	// is not compressed would corrupt the only copy of the payload.
	plain := []byte(`{"model":"plain","messages":[]}`)
	req2, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(plain))
	if got := inspectableBody(req2, plain); string(got) != string(plain) {
		t.Errorf("inspectableBody(plain) = %q, want it unchanged", got)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
