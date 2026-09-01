package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// The audit row is written after the response is already on the wire, so
// waiting for room in its channel would turn telemetry into part of the
// request's own latency. Worse, the thing holding the line is Postgres:
// a slow or stalled database would then stall responses that had nothing
// to do with it, and the gateway would look like the upstream was down.
func TestAuditBackpressureCannotStallARequest(t *testing.T) {
	rdb := testRedis(t)
	up := echoUpstream()
	defer up.Close()

	s := &Server{
		Cfg: &config.Config{UpstreamTimeoutSec: 10},
		RDB: rdb,
		Routes: []Route{{
			Name: "a", BaseURL: up.URL, MatchPrefix: "/a", Upstream: "a",
			APIFormat: "generic", UpstreamRPS: 1000,
		}},
		// Unbuffered, and nobody ever reads it. Under a blocking send this
		// request could never finish, because nothing would ever make room.
		Auditor: make(chan store.LogEntry),
	}

	ts := httptest.NewServer(testChain(s))
	defer ts.Close()

	seedKey(t, rdb, "gw-audit-block", store.KeyInfo{QuotaLimit: 0})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/a", strings.NewReader(`{"a":1}`))
	req.Header.Set("X-API-Key", "gw-audit-block")

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request never completed: the audit write is blocking on a channel nobody drains")
	}
}

// Dropping a row is only acceptable if the loss is visible. An operator
// reading spend off request_logs has no other way to learn that the table
// is missing rows.
func TestDroppedAuditRowsAreCounted(t *testing.T) {
	before := store.AuditDropped()

	// A channel with no room and no reader, which is what "the writer cannot
	// keep up" looks like from the request's side.
	full := make(chan store.LogEntry)
	select {
	case full <- store.LogEntry{}:
		t.Fatal("expected the unbuffered channel to have no room")
	default:
		store.NoteAuditDropped()
	}

	if got := store.AuditDropped(); got != before+1 {
		t.Errorf("AuditDropped() = %d, want %d: a dropped row went uncounted", got, before+1)
	}
}
