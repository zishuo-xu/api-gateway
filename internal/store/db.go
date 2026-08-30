package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// NewDB opens a Postgres connection and pings it.
func NewDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// HashKey returns hex sha256 of the raw API key.
func HashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// SeedDemoKey inserts the demo API key into PG and registers it in Redis.
func SeedDemoKey(ctx context.Context, db *sql.DB, rdb *redis.Client, rawKey, owner string) error {
	hash := HashKey(rawKey)
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO api_keys (key_hash, owner, status)
		VALUES ($1, $2, 1)
		ON CONFLICT (key_hash) DO UPDATE SET status = EXCLUDED.status
		RETURNING id
	`, hash, owner).Scan(&id)
	if err != nil {
		return err
	}
	_ = SyncKeyCache(ctx, db, rdb, id)
	return nil
}

// generateRawKey produces a cryptographically random key.
// Prefix "gw-" makes issued keys easy to recognise in logs and configs.
func generateRawKey() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "gw-" + base64.RawURLEncoding.EncodeToString(b), nil
}

// KeySpec is the full policy for a new API key.
type KeySpec struct {
	Owner string
	// Name is an optional human label ("我的笔记本", "ci-runner"). Owner alone
	// cannot tell two keys of the same person apart; the name can.
	Name string
	// QuotaLimit is the token ceiling, and the pointer is load-bearing:
	// nil means "not specified, apply the default", while a non-nil value
	// is written as-is and <=0 means unlimited. As a plain int64 those two
	// requests both arrive as zero and cannot be told apart, which is why
	// an explicitly unlimited key could never be issued.
	QuotaLimit    *int64
	ExpiresAt     time.Time // zero value = never expires
	AllowedIPs    []string  // empty = any source IP
	AllowedModels []string  // empty = any model
	// NoLog suppresses the request log, audit row and metrics for calls made
	// with this key. For smoke-test keys, whose job is to provoke 4xx. Quota
	// is still charged.
	NoLog bool
}

// CreateAPIKey issues a brand-new key: stores only its sha256 hash in PG and
// registers the full policy in Redis for fast auth lookups.
// The raw key is returned ONCE and cannot be recovered afterwards.
func CreateAPIKey(ctx context.Context, db *sql.DB, rdb *redis.Client, spec KeySpec) (rawKey string, id int64, err error) {
	if spec.Owner == "" {
		spec.Owner = "user"
	}
	// A nil ceiling keeps the historical default so that callers which do not
	// care about quota behave exactly as they always did. A supplied one is
	// taken literally: <=0 is the "unlimited" sentinel that mwQuota, the
	// console and the Prometheus gauge already agree on. Negative values are
	// folded to 0 rather than rejected, because a caller asking for -1 wants
	// "no ceiling", not an error.
	//
	// The default is 1M tokens: a single LLM turn can easily consume 40k–100k
	// input tokens (the full context is re-sent each time), so the old 100k
	// default was exhausted in 2–3 requests — which is why every new key
	// looked broken within minutes.
	quota := int64(1000000)
	if spec.QuotaLimit != nil {
		quota = *spec.QuotaLimit
		if quota < 0 {
			quota = 0
		}
	}
	rawKey, err = generateRawKey()
	if err != nil {
		return "", 0, err
	}
	hash := HashKey(rawKey)
	var expires interface{}
	if !spec.ExpiresAt.IsZero() {
		expires = spec.ExpiresAt
	}
	err = db.QueryRowContext(ctx, `
		INSERT INTO api_keys (key_hash, owner, name, status, quota_limit, expires_at, allowed_ips, allowed_models, no_log)
		VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8) RETURNING id
	`, hash, spec.Owner, spec.Name, quota, expires,
		strings.Join(spec.AllowedIPs, ","), marshalModels(spec.AllowedModels), spec.NoLog).Scan(&id)
	if err != nil {
		return "", 0, err
	}
	if err = SyncKeyCache(ctx, db, rdb, id); err != nil {
		return "", 0, err
	}
	return rawKey, id, nil
}

// UpdateAPIKey applies a partial update. Only non-nil fields are written, so
// the console can adjust one setting without clearing the others.
type KeyPatch struct {
	QuotaLimit    *int64
	ExpiresAt     *time.Time // points at a zero time to clear expiry
	AllowedIPs    *[]string
	AllowedModels *[]string
	NoLog         *bool
}

func UpdateAPIKey(ctx context.Context, db *sql.DB, rdb *redis.Client, id int64, p KeyPatch) error {
	var expires interface{}
	if p.ExpiresAt != nil {
		if p.ExpiresAt.IsZero() {
			expires = nil
		} else {
			expires = *p.ExpiresAt
		}
	}
	_, err := db.ExecContext(ctx, `
		UPDATE api_keys SET
		  quota_limit    = COALESCE($2, quota_limit),
		  expires_at     = CASE WHEN $3::boolean THEN $4::timestamptz ELSE expires_at END,
		  allowed_ips    = COALESCE($5, allowed_ips),
		  allowed_models = COALESCE($6, allowed_models),
		  no_log         = COALESCE($7, no_log)
		WHERE id = $1
	`, id, p.QuotaLimit, p.ExpiresAt != nil, expires,
		optJoin(p.AllowedIPs), optMarshalModels(p.AllowedModels), p.NoLog)
	if err != nil {
		return err
	}
	return SyncKeyCache(ctx, db, rdb, id)
}

// DeleteAPIKey removes a key permanently and drops it from the fast auth path.
func DeleteAPIKey(ctx context.Context, db *sql.DB, rdb *redis.Client, id int64) error {
	var hash string
	if err := db.QueryRowContext(ctx, `DELETE FROM api_keys WHERE id=$1 RETURNING key_hash`, id).Scan(&hash); err != nil {
		return err
	}
	if rdb != nil {
		rdb.Del(ctx, "key:"+hash)
		rdb.Del(ctx, quotaKey(id))
		rdb.SRem(ctx, dirtySet, id)
	}
	return nil
}

// SetKeyStatus enables/disables a key. Disabling removes the Redis lookup entry
// immediately so the key stops working without waiting for any cache expiry.
func SetKeyStatus(ctx context.Context, db *sql.DB, rdb *redis.Client, id int64, enabled bool) error {
	status := 0
	if enabled {
		status = 1
	}
	var hash string
	err := db.QueryRowContext(ctx, `
		UPDATE api_keys SET status=$2 WHERE id=$1 RETURNING key_hash
	`, id, status).Scan(&hash)
	if err != nil {
		return err
	}
	if rdb == nil {
		return nil
	}
	if !enabled {
		// immediate revocation: drop it from the fast auth path
		return rdb.Del(ctx, "key:"+hash).Err()
	}
	return SyncKeyCache(ctx, db, rdb, id)
}

func marshalModels(m []string) string {
	if len(m) == 0 {
		return "[]"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func optMarshalModels(m *[]string) interface{} {
	if m == nil {
		return nil
	}
	return marshalModels(*m)
}

func optJoin(s *[]string) interface{} {
	if s == nil {
		return nil
	}
	return strings.Join(*s, ",")
}
