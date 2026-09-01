package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// ----------------------------------------------------------- state machine

func TestCircuitTripsAfterRepeatedFailures(t *testing.T) {
	b := newMemBreaker(3, time.Minute, time.Minute)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if !b.allow(ctx, "up") {
			t.Fatalf("attempt %d refused before the threshold was reached", i+1)
		}
		b.failure(ctx, "up")
	}
	if got := b.state(ctx, "up"); got != stateOpen {
		t.Fatalf("after 3 failures (threshold 3): got %s, want open", got)
	}
	if b.allow(ctx, "up") {
		t.Fatal("an open circuit must refuse traffic")
	}
}

// The previous implementation deleted the open marker on any success. One good
// response anywhere — including one that never went near this upstream — wiped
// the circuit and put the full load straight back on it.
func TestCircuitSuccessCannotClearAnOpenCircuit(t *testing.T) {
	b := newMemBreaker(2, time.Minute, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	b.allow(ctx, "up")
	b.failure(ctx, "up")
	if got := b.state(ctx, "up"); got != stateOpen {
		t.Fatalf("setup: got %s, want open", got)
	}

	b.success(ctx, "up")

	if got := b.state(ctx, "up"); got != stateOpen {
		t.Fatalf("a success must not clear an open circuit: got %s", got)
	}
	if b.allow(ctx, "up") {
		t.Fatal("must still refuse after a stray success")
	}
}

// A success cancels out one failure instead of zeroing the tally. Under the old
// reset-on-success rule an upstream failing most of the time never kept enough
// failures at once to trip, so the breaker sat out exactly the partial outage
// it is there for.
func TestCircuitSuccessDecaysRatherThanResets(t *testing.T) {
	b := newMemBreaker(3, time.Minute, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	b.allow(ctx, "up")
	b.failure(ctx, "up")
	b.success(ctx, "up") // tally back to 1, not to 0

	b.allow(ctx, "up")
	b.failure(ctx, "up") // 2
	if got := b.state(ctx, "up"); got == stateOpen {
		t.Fatal("tripped too early: the success should still count for something")
	}

	b.allow(ctx, "up")
	b.failure(ctx, "up") // 3
	if got := b.state(ctx, "up"); got != stateOpen {
		t.Fatalf("two more failures after one success should reach the threshold: got %s", got)
	}
}

// Half-open is the point of the rewrite: without it the whole load lands back on
// the upstream the instant the open period ends, which usually re-opens the
// circuit immediately.
func TestCircuitProbesExactlyOnceWhenTheOpenPeriodEnds(t *testing.T) {
	b := newMemBreaker(1, 20*time.Millisecond, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	if b.allow(ctx, "up") {
		t.Fatal("must refuse while open")
	}

	time.Sleep(40 * time.Millisecond)

	if !b.allow(ctx, "up") {
		t.Fatal("the first request after the open period is the probe and must pass")
	}
	if b.allow(ctx, "up") {
		t.Fatal("only one probe may be in flight at a time")
	}
}

func TestCircuitProbeSuccessClosesItForGood(t *testing.T) {
	b := newMemBreaker(1, 20*time.Millisecond, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	time.Sleep(40 * time.Millisecond)
	if !b.allow(ctx, "up") {
		t.Fatal("probe must be allowed")
	}
	b.success(ctx, "up")

	if got := b.state(ctx, "up"); got != stateClosed {
		t.Fatalf("a healthy probe should close the circuit: got %s", got)
	}
	for i := 0; i < 4; i++ {
		if !b.allow(ctx, "up") {
			t.Fatalf("request %d refused after recovery", i+1)
		}
	}
}

func TestCircuitProbeFailureOpensItAgain(t *testing.T) {
	b := newMemBreaker(1, 20*time.Millisecond, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	time.Sleep(40 * time.Millisecond)
	if !b.allow(ctx, "up") {
		t.Fatal("probe must be allowed")
	}
	b.failure(ctx, "up")

	if got := b.state(ctx, "up"); got != stateOpen {
		t.Fatalf("a failed probe must reopen the circuit: got %s", got)
	}
	if b.allow(ctx, "up") {
		t.Fatal("must refuse again after a failed probe")
	}
}

// The console polls state on a timer. If reading it advanced the machine, the
// dashboard would spend the probe and the recovering upstream would never get
// its one test request.
func TestCircuitStateDoesNotSpendTheProbe(t *testing.T) {
	b := newMemBreaker(1, 20*time.Millisecond, time.Minute)
	ctx := context.Background()

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	time.Sleep(40 * time.Millisecond)

	for i := 0; i < 3; i++ {
		if got := b.state(ctx, "up"); got != stateHalf {
			t.Fatalf("poll %d: got %s, want half", i+1, got)
		}
	}
	if !b.allow(ctx, "up") {
		t.Fatal("polling must not consume the probe")
	}
}

func TestCircuitFailureTallyAgesOut(t *testing.T) {
	b := newMemBreaker(2, time.Minute, time.Minute)
	ctx := context.Background()
	b.window = 30 * time.Millisecond

	b.allow(ctx, "up")
	b.failure(ctx, "up")
	time.Sleep(50 * time.Millisecond)
	// The window lapsed, so the tally restarts and a lone failure must not trip.
	b.allow(ctx, "up")
	b.failure(ctx, "up")

	if got := b.state(ctx, "up"); got != stateClosed {
		t.Fatalf("a stale failure should not count: got %s", got)
	}
}

// ------------------------------------------------------------ server wiring

// newBreakerServer builds a Server whose breaker is the injected one, so these
// tests drive the state machine without a Redis server.
func newBreakerServer(b breaker, routes []Route) *Server {
	s := &Server{
		Cfg:     &config.Config{MaxAttempts: 3, UpstreamTimeoutSec: 30, RequestBudgetSec: 300},
		Routes:  routes,
		Breaker: b,
		Auditor: make(chan store.LogEntry, 64),
	}
	return s
}

func TestRouteWithoutCircuitBreakingIsNotTracked(t *testing.T) {
	b := newMemBreaker(2, time.Minute, time.Minute)
	s := newBreakerServer(b, nil)

	route := &Route{Name: "r", CBEnabled: false, Channels: []Channel{{Name: "c", Enabled: true}}}
	ch := route.Channels[0]
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		s.noteOutcome(ctx, route, ch, false)
	}
	if got := b.state(ctx, ch.upstreamKey(route.Name)); got != stateClosed {
		t.Fatalf("route opted out of the breaker: got %s, want closed", got)
	}

	// With it enabled the very same failures do trip it.
	route.CBEnabled = true
	for i := 0; i < 5; i++ {
		s.noteOutcome(ctx, route, ch, false)
	}
	if got := b.state(ctx, ch.upstreamKey(route.Name)); got != stateOpen {
		t.Fatalf("got %s, want open", got)
	}
}

func TestTrippedChannelIsOrderedLast(t *testing.T) {
	b := newMemBreaker(1, time.Minute, time.Minute)
	s := newBreakerServer(b, nil)

	route := &Route{Name: "r", CBEnabled: true, Channels: []Channel{
		{Name: "broken", Enabled: true, Priority: 0},
		{Name: "fine", Enabled: true, Priority: 1},
	}}
	ctx := context.Background()
	s.noteOutcome(ctx, route, route.Channels[0], false)

	order := s.orderedChannels(route)
	if len(order) != 2 {
		t.Fatalf("got %d channels, want 2", len(order))
	}
	if order[0].Name != "fine" || order[1].Name != "broken" {
		t.Fatalf("want [fine broken], got [%s %s]", order[0].Name, order[1].Name)
	}
}

// ------------------------------------------------------------ retry budget

// Once the caller has hung up there is nothing left to win: every further
// attempt fails instantly on a cancelled context, costs an upstream call, and
// still counts as that upstream's failure.
func TestProxyStopsRetryingOnceTheClientIsGone(t *testing.T) {
	var slowHits, fastHits int32

	// Hangs, standing in for a stalled upstream. The 2s ceiling is a test guard
	// only: whether a cancellation actually reaches the server side is the
	// upstream's business, not something this gateway can promise — which is
	// why work already in flight keeps costing quota after a hangup.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowHits, 1)
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fastHits, 1)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer fast.Close()

	s := newBreakerServer(newMemBreaker(5, time.Minute, time.Minute), []Route{{
		Name: "r", MatchPrefix: "/r", BaseURL: slow.URL, CBEnabled: true, APIFormat: "openai-chat",
		Channels: []Channel{
			{Name: "slow", BaseURL: slow.URL, Enabled: true, Priority: 0},
			{Name: "fast", BaseURL: fast.URL, Enabled: true, Priority: 1},
		},
	}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/r/v1/chat/completions",
		strings.NewReader(`{"model":"m"}`)).WithContext(ctx)
	rec := httptest.NewRecorder()

	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	s.proxy(rec, req)

	if got := atomic.LoadInt32(&slowHits); got != 1 {
		t.Fatalf("slow channel hit %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&fastHits); got != 0 {
		t.Fatalf("fast channel hit %d times after the client hung up, want 0", got)
	}
}

// A fresh attempt gets the full upstream timeout, so once the budget is spent a
// retry could only return after the caller stopped listening.
func TestProxyStopsWhenTheBudgetIsGone(t *testing.T) {
	var slowHits, fastHits int32

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowHits, 1)
		time.Sleep(1200 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError) // retryable
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fastHits, 1)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer fast.Close()

	s := newBreakerServer(newMemBreaker(5, time.Minute, time.Minute), []Route{{
		Name: "r", MatchPrefix: "/r", BaseURL: slow.URL, CBEnabled: true, APIFormat: "openai-chat",
		Channels: []Channel{
			{Name: "slow", BaseURL: slow.URL, Enabled: true, Priority: 0},
			{Name: "fast", BaseURL: fast.URL, Enabled: true, Priority: 1},
		},
	}})
	s.Cfg.RequestBudgetSec = 1 // one second for the whole chain

	req := httptest.NewRequest(http.MethodPost, "/r/v1/chat/completions",
		strings.NewReader(`{"model":"m"}`))
	rec := httptest.NewRecorder()
	s.proxy(rec, req)

	if got := atomic.LoadInt32(&slowHits); got != 1 {
		t.Fatalf("slow channel hit %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&fastHits); got != 0 {
		t.Fatalf("fast channel hit %d times after the budget was spent, want 0", got)
	}
}
