package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyInfo is the per-API-key policy the gateway enforces before forwarding.
// It is cached in Redis under "key:<hash>" so the hot path makes one Redis read
// instead of a Postgres round-trip.
//
// The value is stored as JSON, but entries written by earlier versions hold a
// bare integer id. DecodeKeyInfo accepts both, so upgrading does not lock every
// existing key out of the gateway.
type KeyInfo struct {
	ID            int64    `json:"id"`
	ExpiresAt     int64    `json:"exp"`    // unix seconds, 0 = never expires
	QuotaLimit    int64    `json:"ql"`     // <=0 means unlimited
	AllowedIPs    []string `json:"ips"`    // empty means any IP
	AllowedModels []string `json:"models"` // empty means any model
	// NoLog suppresses the log line, the audit row and the metrics for calls
	// made with this key. Intended for smoke-test keys that spend their life
	// provoking 4xx responses. Quota is unaffected: the e2e suite asserts on
	// quota exhaustion, so exempting it from billing would make those very
	// assertions vacuous.
	NoLog bool `json:"nl"`
}

// EncodeKeyInfo renders the cache value.
func EncodeKeyInfo(k KeyInfo) string {
	b, err := json.Marshal(k)
	if err != nil {
		// KeyInfo holds only scalars and string slices; marshalling cannot fail.
		return strconv.FormatInt(k.ID, 10)
	}
	return string(b)
}

// DecodeKeyInfo parses a cache value, tolerating the legacy bare-id form.
func DecodeKeyInfo(s string) (KeyInfo, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return KeyInfo{}, false
	}
	if strings.HasPrefix(s, "{") {
		var k KeyInfo
		if err := json.Unmarshal([]byte(s), &k); err == nil && k.ID > 0 {
			return k, true
		}
	}
	// Legacy: value used to be just the key id.
	if id, err := strconv.ParseInt(s, 10, 64); err == nil && id > 0 {
		return KeyInfo{ID: id}, true
	}
	return KeyInfo{}, false
}

// Expired reports whether the key has passed its expiry.
func (k KeyInfo) Expired(now time.Time) bool {
	return k.ExpiresAt > 0 && now.Unix() >= k.ExpiresAt
}

// IPAllowed checks the client IP against the allowlist. An empty list allows
// everything. Entries may be plain IPs ("1.2.3.4") or CIDRs ("10.0.0.0/8").
// An unparseable entry never matches, so a typo blocks rather than opens.
func (k KeyInfo) IPAllowed(ip string) bool {
	if len(k.AllowedIPs) == 0 {
		return true
	}
	addr := net.ParseIP(strings.TrimSpace(ip))
	if addr == nil {
		return false
	}
	for _, raw := range k.AllowedIPs {
		rule := strings.TrimSpace(raw)
		if rule == "" {
			continue
		}
		if strings.Contains(rule, "/") {
			if _, netw, err := net.ParseCIDR(rule); err == nil && netw.Contains(addr) {
				return true
			}
			continue
		}
		if net.ParseIP(rule).Equal(addr) {
			return true
		}
	}
	return false
}

// ModelAllowed checks the requested model against the per-key permission list.
func (k KeyInfo) ModelAllowed(model string) bool {
	return len(k.AllowedModels) == 0 || modelAllowed(k.AllowedModels, model)
}

// ModelInList reports whether model appears in an allowlist, case-insensitively.
// An empty model never matches: a request that omits the model cannot be
// checked against a list, so allowing it would defeat the whole check.
func ModelInList(list []string, model string) bool {
	if model == "" {
		return false
	}
	for _, m := range list {
		if strings.EqualFold(strings.TrimSpace(m), model) {
			return true
		}
	}
	return false
}

// modelAllowed is the shared membership test for model allowlists.
func modelAllowed(list []string, model string) bool {
	return ModelInList(list, model)
}

// LoadKeyInfo reads the full policy row for a key from Postgres.
func LoadKeyInfo(ctx context.Context, db *sql.DB, id int64) (KeyInfo, string, error) {
	var (
		k          KeyInfo
		hash       string
		expires    sql.NullTime
		ips        string
		modelsJSON string
	)
	err := db.QueryRowContext(ctx, `
		SELECT id, key_hash, quota_limit, expires_at, COALESCE(allowed_ips,''), COALESCE(allowed_models,'[]'), COALESCE(no_log,false)
		FROM api_keys WHERE id=$1
	`, id).Scan(&k.ID, &hash, &k.QuotaLimit, &expires, &ips, &modelsJSON, &k.NoLog)
	if err != nil {
		return KeyInfo{}, "", err
	}
	if expires.Valid {
		k.ExpiresAt = expires.Time.Unix()
	}
	k.AllowedIPs = splitList(ips)
	_ = json.Unmarshal([]byte(modelsJSON), &k.AllowedModels)
	return k, hash, nil
}

// SyncKeyCache rewrites the Redis entry for one key from the database row.
func SyncKeyCache(ctx context.Context, db *sql.DB, rdb *redis.Client, id int64) error {
	k, hash, err := LoadKeyInfo(ctx, db, id)
	if err != nil {
		return err
	}
	if rdb == nil {
		return nil
	}
	return rdb.Set(ctx, "key:"+hash, EncodeKeyInfo(k), 0).Err()
}

// SyncAllKeys refreshes every enabled key's cache entry. Disabled keys keep
// their Redis entry deleted so they stay revoked.
func SyncAllKeys(ctx context.Context, db *sql.DB, rdb *redis.Client) (int, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, key_hash, quota_limit, expires_at, COALESCE(allowed_ips,''), COALESCE(allowed_models,'[]'), COALESCE(no_log,false), status
		FROM api_keys
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var (
		n   int
		del []string
		set = map[string]string{}
		ids []int64
	)
	for rows.Next() {
		var (
			k          KeyInfo
			hash       string
			status     int
			expires    sql.NullTime
			ips        string
			modelsJSON string
		)
		if err := rows.Scan(&k.ID, &hash, &k.QuotaLimit, &expires, &ips, &modelsJSON, &k.NoLog, &status); err != nil {
			return n, err
		}
		if status != 1 {
			del = append(del, "key:"+hash)
			continue
		}
		if expires.Valid {
			k.ExpiresAt = expires.Time.Unix()
		}
		k.AllowedIPs = splitList(ips)
		_ = json.Unmarshal([]byte(modelsJSON), &k.AllowedModels)
		set["key:"+hash] = EncodeKeyInfo(k)
		ids = append(ids, k.ID)
	}
	if err := rows.Err(); err != nil {
		return n, err
	}
	if rdb == nil {
		return len(set), nil
	}
	pipe := rdb.Pipeline()
	for k, v := range set {
		pipe.Set(ctx, k, v, 0)
	}
	for _, k := range del {
		pipe.Del(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return n, err
	}
	// Warm the quota counters from Postgres so a restart does not silently
	// hand every key a fresh, empty allowance.
	for _, id := range ids {
		WarmQuota(ctx, db, rdb, id)
	}
	return len(set), nil
}

// splitList parses a comma-separated allowlist field.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
