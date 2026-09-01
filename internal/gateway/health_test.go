package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zishuo-xu/api-gateway/internal/config"
	"github.com/zishuo-xu/api-gateway/internal/store"
)

// Liveness exists to answer one question: is the process running. It must
// hold even with no database and no Redis at all, because a dependency
// outage is exactly when something needs to ask.
func TestHealthzNeedsNoDependencies(t *testing.T) {
	s := &Server{Cfg: &config.Config{}}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200: liveness must not consult Postgres or Redis", rec.Code)
	}
}

// A probe that landed in the public chain would be refused for having no API
// key, and whatever is doing the probing would read that as "the gateway is
// down" instead of "the probe is unauthenticated".
func TestHealthProbesCarryNoCredential(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db}

	for _, path := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code == http.StatusUnauthorized {
			t.Errorf("GET %s = 401: the probe reached the authenticated chain", path)
		}
	}
}

// Probes run every few seconds forever. If they went through mwLogging they
// would each write an audit row, and the health of the system would be the
// main thing in the request log.
func TestHealthProbesWriteNoAuditRow(t *testing.T) {
	rdb := testRedis(t)
	db := testDB(t)
	s := &Server{Cfg: &config.Config{}, RDB: rdb, DB: db}
	audited := make(chan store.LogEntry, 8)
	s.Auditor = audited

	s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/readyz", nil))

	select {
	case e := <-audited:
		t.Errorf("probe wrote an audit row (%s %s)", e.Method, e.Path)
	default:
	}
}

// A bare 503 tells you the gateway is unhappy but not which layer to go and
// fix. The body has to name the dependency.
func TestReadyzNamesTheDependencyThatIsDown(t *testing.T) {
	// Port 1 is reserved and nothing listens on it, which is what "Redis is
	// unreachable" looks like from here.
	dead := store.NewRedis("127.0.0.1:1", "", 0)
	s := &Server{Cfg: &config.Config{}, RDB: dead}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503 with Redis unreachable", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "redis") {
		t.Errorf("body = %q, want it to name redis", body)
	}
}

// A dependency that is slow to fail must not make a healthy one look broken.
//
// The bug this pins: both checks originally shared one 2s deadline, so a
// Redis that hung spent the whole allowance and Postgres — which had not
// been asked anything yet — was reported as timing out too. Naming a
// healthy service is worse than naming none, because it sends the person
// debugging to the wrong box.
//
// 192.0.2.1 is TEST-NET-1: guaranteed unroutable, and it black-holes the
// connect rather than refusing it, which is what a hung dependency looks
// like. A refused connection would fail too fast to reproduce this.
func TestReadyzDoesNotBlameAHealthyDependency(t *testing.T) {
	s := &Server{
		Cfg: &config.Config{},
		RDB: store.NewRedis("192.0.2.1:6379", "", 0),
		DB:  testDB(t),
	}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	body := rec.Body.String()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503 (body %q)", rec.Code, body)
	}
	if !strings.Contains(body, "redis") {
		t.Errorf("body = %q, want it to name redis", body)
	}
	if strings.Contains(body, "postgres") {
		t.Errorf("body = %q, must not blame postgres: it never got asked", body)
	}
}

func TestReadyzIsOKWhenBothDependenciesAnswer(t *testing.T) {
	s := &Server{Cfg: &config.Config{}, RDB: testRedis(t), DB: testDB(t)}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /readyz = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
}
