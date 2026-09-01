package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// auditCols is the number of persisted columns, and therefore the number
	// of bind parameters one row costs. Postgres caps a statement at 65535
	// parameters, so neither constant below can come close to it.
	auditCols = 20

	// auditBatchSize is how many rows travel in one INSERT. One round trip
	// per row is what makes a write-only sidecar the most expensive thing the
	// database does once traffic picks up; 64 rows keeps a single statement
	// around 1280 parameters while still capping unflushed data at a size
	// worth losing in a crash.
	auditBatchSize = 64

	// auditFlushInterval is how long a partially filled batch may sit. The
	// batch size alone would leave the last few rows of a quiet period
	// unwritten indefinitely — and "quiet" is exactly when you want the one
	// request you just made to show up.
	auditFlushInterval = 500 * time.Millisecond

	// auditDrainTimeout bounds the final flush at shutdown. Draining is
	// better than discarding, but a hung database must not hold the process
	// open past its stop grace period either.
	auditDrainTimeout = 3 * time.Second
)

var (
	auditDropped atomic.Int64

	// auditWarnedAt is the unix-nano timestamp of the last dropped-row
	// warning, guarding log output rather than the count itself.
	auditWarnedAt atomic.Int64
)

// AuditDropped reports how many log rows have been discarded since boot
// because the ingest channel was full. Non-zero means the writer cannot keep
// up with the gateway, and every number derived from request_logs — spend,
// error rates, the usage dashboard — is quietly missing that much.
func AuditDropped() int64 { return auditDropped.Load() }

// NoteAuditDropped records a row that never reached the ingest channel.
// Callers use it when a blocking send would be worse than a missing row.
func NoteAuditDropped() {
	n := auditDropped.Add(1)

	// At most one warning a second. A full channel under load would
	// otherwise turn the log into a second flood on top of the first, and
	// the writing itself would slow the drain it is complaining about.
	now := time.Now().UnixNano()
	if last := auditWarnedAt.Load(); now-last > int64(time.Second) {
		if auditWarnedAt.CompareAndSwap(last, now) {
			log.Printf("audit: ingest channel full, %d log rows dropped so far", n)
		}
	}
}

// LogEntry is a single audit record. Token counters stay in the log rather than
// a separate table so the usage dashboard can answer "who spent what on which
// model" with one indexed scan.
type LogEntry struct {
	APIKeyID         int64
	RouteID          int64
	Method           string
	Path             string
	Upstream         string
	StatusCode       int
	LatencyMs        int
	Cached           bool
	Model            string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	IsStream         bool
	ChannelID        int64
	ErrMsg           string
	// TTFTMs is Time To First Token in milliseconds. Only meaningful for
	// streaming responses; 0 for non-streaming, cache hits, and rejections.
	TTFTMs int64
	// TokensPerSec is the completion throughput. Computed at write time:
	//   streaming: completion_tokens / (latency - ttft)
	//   buffered:   total_tokens / latency
	// Stored as NUMERIC(10,2) for display without float drift.
	TokensPerSec float64
	// CacheHitTokens is the part of PromptTokens the provider served from its
	// own prefix cache, billed at the discounted rate. This is the only signal
	// that separates "billed for 40k input tokens" from "billed for 40k input
	// tokens at a tenth of the price", which is most of the cost difference
	// between a warm agent loop and a cold one.
	CacheHitTokens int64
	// CacheWriteTokens is the part of PromptTokens the provider charged a
	// premium to store. Only Anthropic reports it; it bills writes at ~1.25x.
	CacheWriteTokens int64
	// RejectReason names the gate that turned the request away (quota,
	// rate_ip, no_route, ...) and is empty when the request reached an
	// upstream. A status code alone cannot separate "you spent your
	// allowance" from "you are hammering us", and the two have different
	// fixes.
	RejectReason string
}

// StartAuditor starts a background worker that persists logs asynchronously.
// Returns the ingest channel and a stop function.
//
// The stop function blocks until the worker has drained what is already
// queued, up to auditDrainTimeout. Returning from stop means the rows are on
// disk; callers that ignore it (a plain `defer stopAudit()` in main is the
// easy mistake) still race the process exit and lose whatever was buffered.
func StartAuditor(db *sql.DB, bufSize int) (chan LogEntry, func()) {
	ch := make(chan LogEntry, bufSize)
	done := make(chan struct{})

	go func() {
		defer close(done)

		batch := make([]LogEntry, 0, auditBatchSize)
		tick := time.NewTicker(auditFlushInterval)
		defer tick.Stop()

		for {
			select {
			case e, ok := <-ch:
				if !ok {
					// Final flush under a deadline, so a database that has
					// stopped answering cannot hold shutdown open forever.
					ctx, cancel := context.WithTimeout(context.Background(), auditDrainTimeout)
					insertLogs(ctx, db, batch)
					cancel()
					return
				}
				batch = append(batch, e)
				if len(batch) >= auditBatchSize {
					insertLogs(context.Background(), db, batch)
					batch = batch[:0]
				}
			case <-tick.C:
				insertLogs(context.Background(), db, batch)
				batch = batch[:0]
			}
		}
	}()

	return ch, func() { close(ch); <-done }
}

// insertLogs writes rows in a single multi-valued INSERT. The context only
// bounds the statement: whatever is in the slice is lost if it expires, which
// is the intended trade-off during shutdown.
func insertLogs(ctx context.Context, db *sql.DB, rows []LogEntry) {
	if len(rows) == 0 {
		return
	}

	var sb strings.Builder
	// Sized so a full batch does not reallocate: 20 placeholders of up to 5
	// characters, plus the parens and comma that separate rows.
	sb.Grow(len(rows) * (auditCols*5 + 8))
	sb.WriteString(`INSERT INTO request_logs
		(api_key_id, route_id, method, path, upstream, status_code, latency_ms, cached,
		 model, prompt_tokens, completion_tokens, total_tokens, is_stream, channel_id, err_msg,
		 ttft_ms, tokens_per_sec, prompt_cache_hit_tokens, prompt_cache_write_tokens,
		 reject_reason) VALUES `)

	args := make([]any, 0, len(rows)*auditCols)
	for i, e := range rows {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for j := 0; j < auditCols; j++ {
			if j > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "$%d", i*auditCols+j+1)
		}
		sb.WriteString(")")

		args = append(args,
			e.APIKeyID, e.RouteID, e.Method, e.Path, e.Upstream, e.StatusCode, e.LatencyMs, e.Cached,
			e.Model, e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.IsStream, e.ChannelID, e.ErrMsg,
			e.TTFTMs, e.TokensPerSec, e.CacheHitTokens, e.CacheWriteTokens, e.RejectReason,
		)
	}

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		// One bad row fails the whole statement — an over-long err_msg, a
		// null where the schema wants a value. Retrying singly means that
		// row costs itself instead of the 63 rows it happened to share a
		// statement with, which is the difference between losing a line of
		// telemetry and losing a minute of it.
		log.Printf("audit: batch insert failed (%d rows), retrying singly: %v", len(rows), err)
		for i := range rows {
			if err := insertOne(ctx, db, rows[i]); err != nil {
				log.Printf("audit insert err: %v", err)
			}
		}
	}
}

// insertOne writes a single row. It is the fallback path for a failed batch,
// and the reason a malformed entry cannot take its neighbours down with it.
func insertOne(ctx context.Context, db *sql.DB, e LogEntry) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO request_logs
		(api_key_id, route_id, method, path, upstream, status_code, latency_ms, cached,
		 model, prompt_tokens, completion_tokens, total_tokens, is_stream, channel_id, err_msg,
		 ttft_ms, tokens_per_sec, prompt_cache_hit_tokens, prompt_cache_write_tokens,
		 reject_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
	`, e.APIKeyID, e.RouteID, e.Method, e.Path, e.Upstream, e.StatusCode, e.LatencyMs, e.Cached,
		e.Model, e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.IsStream, e.ChannelID, e.ErrMsg,
		e.TTFTMs, e.TokensPerSec, e.CacheHitTokens, e.CacheWriteTokens, e.RejectReason)
	return err
}
