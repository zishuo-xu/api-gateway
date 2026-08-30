package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/zishuo-xu/api-gateway/internal/config"
)

// The admin token used to be baked into the dashboard HTML: anyone who could
// fetch /admin/ owned the console. These tests pin the replacement — a login
// endpoint that mints an HttpOnly session cookie, so the page ships markup
// only and the token never crosses the wire as page content.

func newAdminTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	s := &Server{
		Cfg: &config.Config{AdminToken: "test-admin-token-0123456789"},
		RDB: testRedis(t),
		DB:  testDB(t),
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func TestAdminTokenNeverShipsInThePage(t *testing.T) {
	s, ts := newAdminTestServer(t)

	resp, err := http.Get(ts.URL + "/admin/")
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), s.Cfg.AdminToken) {
		t.Fatal("the admin token is embedded in the dashboard HTML: anyone who " +
			"can fetch the page owns the console")
	}
	if !strings.Contains(string(body), `id="login-gate"`) {
		t.Error("page has no login gate to raise on 403")
	}

	// The page alone must open nothing: the API behind it refuses an
	// unauthenticated request.
	areq, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/keys", nil)
	ares, err := http.DefaultClient.Do(areq)
	if err != nil {
		t.Fatalf("api: %v", err)
	}
	defer ares.Body.Close()
	if ares.StatusCode != http.StatusForbidden {
		t.Errorf("anonymous /admin/keys = %d, want 403", ares.StatusCode)
	}
}

func TestAdminLoginSessionFlow(t *testing.T) {
	s, ts := newAdminTestServer(t)

	login := func(token string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/login",
			strings.NewReader(`{"token":"`+token+`"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		return resp
	}

	// A wrong token is refused and mints no cookie.
	wrong := login("not-the-token")
	wrong.Body.Close()
	if wrong.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong token = %d, want 403", wrong.StatusCode)
	}
	if len(wrong.Cookies()) != 0 {
		t.Errorf("a refused login set %d cookies, want 0", len(wrong.Cookies()))
	}

	// The right token sets an HttpOnly session cookie.
	right := login(s.Cfg.AdminToken)
	right.Body.Close()
	if right.StatusCode != http.StatusOK {
		t.Fatalf("correct token = %d, want 200", right.StatusCode)
	}
	var cookie *http.Cookie
	for _, c := range right.Cookies() {
		if c.Name == sessionCookie {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatal("successful login set no session cookie")
	}
	if !cookie.HttpOnly {
		t.Error("session cookie is not HttpOnly: scripts could read it")
	}

	// The cookie alone unlocks the API, no header needed.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/keys", nil)
	req.AddCookie(cookie)
	ares, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	ares.Body.Close()
	if ares.StatusCode != http.StatusOK {
		t.Errorf("cookie-authenticated /admin/keys = %d, want 200", ares.StatusCode)
	}

	// Logout revokes the session server-side: replaying the same cookie after
	// it must fail, because the Redis key is gone, not because the client
	// dropped the cookie.
	lreq, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/logout", nil)
	lreq.AddCookie(cookie)
	lres, err := http.DefaultClient.Do(lreq)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	lres.Body.Close()

	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/keys", nil)
	req2.AddCookie(cookie)
	ares2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("keys after logout: %v", err)
	}
	ares2.Body.Close()
	if ares2.StatusCode != http.StatusForbidden {
		t.Errorf("replayed cookie after logout = %d, want 403", ares2.StatusCode)
	}
}

func TestAdminLoginBruteForceLockout(t *testing.T) {
	s, ts := newAdminTestServer(t)

	post := func(token string) int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/login",
			strings.NewReader(`{"token":"`+token+`"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// All requests come from the same test address, which is the point: the
	// budget is per IP. Exhaust it with wrong tokens.
	for i := 0; i < loginMaxFails; i++ {
		if code := post("wrong-" + strconv.Itoa(i)); code != http.StatusForbidden {
			t.Fatalf("failure %d = %d, want 403", i+1, code)
		}
	}
	// The lock is on the address, not the credential: the right token is
	// refused too, or the limit would be decorative.
	if code := post(s.Cfg.AdminToken); code != http.StatusTooManyRequests {
		t.Errorf("after %d failures the correct token = %d, want 429",
			loginMaxFails, code)
	}
}

func TestSecureEqual(t *testing.T) {
	if !secureEqual("same", "same") {
		t.Error("equal secrets compared unequal")
	}
	if secureEqual("same", "diff") {
		t.Error("different secrets compared equal")
	}
	// Different lengths must not match — and go through the same code path,
	// since ConstantTimeCompare returns early only on length, which the hash
	// step makes a non-issue.
	if secureEqual("same", "same-but-longer") {
		t.Error("prefix secrets compared equal")
	}
}

// The name field exists because owner alone cannot tell two keys of the same
// person apart: user 33 and user 99 were both "user" and the console gave no
// way to say which was which. Name is set at issue time and echoed in the list.
func TestKeyNameStoredAndListed(t *testing.T) {
	s, ts := newAdminTestServer(t)
	hdr := map[string]string{"X-Admin-Token": s.Cfg.AdminToken}

	post := func(body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/keys", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		return resp
	}

	// One key with a name, one without (legacy shape still works). The test
	// database is shared across runs, so assertions go by the returned id
	// rather than by counting rows — reruns would otherwise double the count.
	var namedID, unnamedID int64
	var err error
	decode := func(resp *http.Response) (int64, error) {
		defer resp.Body.Close()
		var out struct {
			ID int64 `json:"id"`
		}
		err := json.NewDecoder(resp.Body).Decode(&out)
		return out.ID, err
	}
	if resp := post(`{"owner":"alice","name":"我的笔记本"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("named key = %d, want 200", resp.StatusCode)
	} else if namedID, err = decode(resp); err != nil {
		t.Fatalf("decode named: %v", err)
	}
	if resp := post(`{"owner":"alice"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("unnamed key = %d, want 200", resp.StatusCode)
	} else if unnamedID, err = decode(resp); err != nil {
		t.Fatalf("decode unnamed: %v", err)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/keys", nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	var keys []struct {
		ID    int64  `json:"id"`
		Owner string `json:"owner"`
		Name  string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		t.Fatalf("decode: %v", err)
	}
	byID := map[int64]struct {
		Owner string `json:"owner"`
		Name  string `json:"name"`
	}{}
	for _, k := range keys {
		byID[k.ID] = struct {
			Owner string `json:"owner"`
			Name  string `json:"name"`
		}{k.Owner, k.Name}
	}
	if k, ok := byID[namedID]; !ok {
		t.Errorf("named key id %d missing from the list", namedID)
	} else {
		if k.Name != "我的笔记本" {
			t.Errorf("name = %q, want 我的笔记本", k.Name)
		}
		if k.Owner != "alice" {
			t.Errorf("owner = %q, want alice", k.Owner)
		}
	}
	if k, ok := byID[unnamedID]; !ok {
		t.Errorf("unnamed key id %d missing from the list", unnamedID)
	} else if k.Name != "" {
		t.Errorf("unnamed key name = %q, want empty", k.Name)
	}
}

// Deleting a route used to leave its channels enabled. The route is what pulls
// a channel into memory, so those rows could never serve again — they just sat
// there looking live while the channel list disagreed with reality. This pins
// the fix: both updates land together, in one transaction.
func TestDeleteRouteDisablesItsChannels(t *testing.T) {
	s, ts := newAdminTestServer(t)
	ctx := context.Background()

	var routeID int64
	if err := s.DB.QueryRowContext(ctx,
		`INSERT INTO routes (name, base_url, match_path, status)
		 VALUES ('zt-delete-chan','https://zt-chan.test','/zt-chan',1) RETURNING id`,
	).Scan(&routeID); err != nil {
		t.Fatalf("insert route: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = s.DB.ExecContext(bg, `DELETE FROM channels WHERE route_id=$1`, routeID)
		_, _ = s.DB.ExecContext(bg, `DELETE FROM routes WHERE id=$1`, routeID)
	})

	var chanID int64
	if err := s.DB.QueryRowContext(ctx,
		`INSERT INTO channels (route_id, name, base_url, enabled)
		 VALUES ($1,'zt-chan-a','https://zt-chan.test/v1',true) RETURNING id`,
		routeID).Scan(&chanID); err != nil {
		t.Fatalf("insert channel: %v", err)
	}

	req, err := http.NewRequest(http.MethodDelete,
		ts.URL+"/admin/routes?id="+strconv.FormatInt(routeID, 10), nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Admin-Token", s.Cfg.AdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete route: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete route = %d, want 200: %s", resp.StatusCode, body)
	}

	var routeStatus int
	if err := s.DB.QueryRowContext(ctx,
		`SELECT status FROM routes WHERE id=$1`, routeID).Scan(&routeStatus); err != nil {
		t.Fatalf("read route: %v", err)
	}
	if routeStatus != 0 {
		t.Errorf("route status = %d, want 0 (soft deleted)", routeStatus)
	}

	var enabled bool
	if err := s.DB.QueryRowContext(ctx,
		`SELECT enabled FROM channels WHERE id=$1`, chanID).Scan(&enabled); err != nil {
		t.Fatalf("read channel: %v", err)
	}
	if enabled {
		t.Error("channel is still enabled after its route was deleted: the row is " +
			"unreachable but looks live, so the channel list stops matching reality")
	}
}
