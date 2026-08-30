package store

import (
	"context"
	"database/sql"
	"log"
)

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
func StartAuditor(db *sql.DB, bufSize int) (chan LogEntry, func()) {
	ch := make(chan LogEntry, bufSize)
	go func() {
		for e := range ch {
			_, err := db.ExecContext(context.Background(), `
				INSERT INTO request_logs
				(api_key_id, route_id, method, path, upstream, status_code, latency_ms, cached,
				 model, prompt_tokens, completion_tokens, total_tokens, is_stream, channel_id, err_msg,
				 ttft_ms, tokens_per_sec, prompt_cache_hit_tokens, prompt_cache_write_tokens,
				 reject_reason)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
			`, e.APIKeyID, e.RouteID, e.Method, e.Path, e.Upstream, e.StatusCode, e.LatencyMs, e.Cached,
				e.Model, e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.IsStream, e.ChannelID, e.ErrMsg,
				e.TTFTMs, e.TokensPerSec, e.CacheHitTokens, e.CacheWriteTokens, e.RejectReason)
			if err != nil {
				log.Printf("audit insert err: %v", err)
			}
		}
	}()
	return ch, func() { close(ch) }
}
