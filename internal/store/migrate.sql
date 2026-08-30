-- Incremental migration: safe to run repeatedly on an existing database.
-- init.sql only runs on first container init, so every change after that lives here.
-- All statements are idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).

-- ---------------------------------------------------------------------------
-- routes: per-route cache scope
--   'global' = one cached response shared by every API key (right for public
--              data such as weather or FX rates)
--   'key'    = each key gets its own entry. Required for anything
--              user-specific: with 'global' the first caller's answer is served
--              to whoever asks next, which is a cross-user data leak.
-- ---------------------------------------------------------------------------
ALTER TABLE routes ADD COLUMN IF NOT EXISTS cache_scope VARCHAR(16) DEFAULT 'global';

-- ---------------------------------------------------------------------------
-- api_keys: expiry, IP allowlist, per-key model permission
-- ---------------------------------------------------------------------------
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at     TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS allowed_ips    TEXT DEFAULT '';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS allowed_models TEXT DEFAULT '[]';

-- ---------------------------------------------------------------------------
-- api_keys: opt out of the request log
--   Smoke-test keys deliberately provoke 403/429/400 to prove the gateway
--   rejects them, and those rows buried the real traffic in the log. Marking
--   the key suppresses the log line, the audit row and the Prometheus
--   counters. Quota is still charged: the e2e suite asserts on quota
--   exhaustion, so waiving the charge would make its own assertions vacuous.
-- ---------------------------------------------------------------------------
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS no_log BOOLEAN DEFAULT false;

-- ---------------------------------------------------------------------------
-- request_logs: per-request LLM observability
--   ttft_ms       = Time To First Token (ms). Only meaningful for streaming:
--                   wall-clock from request start to the first SSE data chunk.
--                   0 for non-streaming and for requests that never reached an
--                   upstream (auth failures, 4xx, cache hits).
--   tokens_per_sec = completion throughput, computed at write time.
--                   For streaming:  completion_tokens / (latency - ttft).
--                   For buffered:     total_tokens / latency.
--                   0 when no tokens were reported or latency is zero.
-- ---------------------------------------------------------------------------
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS ttft_ms        INT DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS tokens_per_sec NUMERIC(10,2) DEFAULT 0;

-- ---------------------------------------------------------------------------
-- request_logs: provider-side prompt cache accounting
--   prompt_cache_hit_tokens   = input tokens the provider served from its own
--                               prefix cache instead of recomputing them. Every
--                               provider bills these at a fraction of the
--                               normal input rate, and for a chat workload they
--                               are the large majority of the bill, so without
--                               this column a cached 40k-token request and an
--                               uncached one look identical in the console.
--                               0 means "the provider reported nothing", which
--                               is not the same as a measured 0%.
--   prompt_cache_write_tokens = input tokens the provider charged a premium to
--                               store. Only Anthropic reports it; writes cost
--                               ~1.25x a normal input token, so it belongs in
--                               the ledger next to the hits rather than being
--                               folded into them.
--
-- The three provider dialects all land on these two columns:
--   DeepSeek  prompt_cache_hit_tokens / prompt_cache_miss_tokens
--   Anthropic cache_read_input_tokens / cache_creation_input_tokens
--   OpenAI    prompt_tokens_details.cached_tokens
-- DeepSeek's miss counter is dropped on purpose: hit+miss is the prompt total,
-- so the miss is derivable and storing it invites double counting.
-- ---------------------------------------------------------------------------
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS prompt_cache_hit_tokens   INT DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS prompt_cache_write_tokens INT DEFAULT 0;

-- ---------------------------------------------------------------------------
-- request_logs.reject_reason: which gate refused the request.
--
-- A 429 in this table used to be unreadable: it could mean the key spent its
-- allowance, or that a rate-limit bucket tripped. One is a billing question,
-- the other is a traffic-shaping question, and they have nothing in common
-- except the status code. Same story for 403 (expired key vs. model not on
-- the allowlist) and 404 (no route vs. unknown model).
--
-- Empty means the request reached an upstream, so a blank here is a positive
-- statement rather than missing data.
--
--   quota          allowance spent (429)
--   rate_upstream  provider-wide rate cap        (429)
--   rate_key       this key's own rate bucket    (429)
--   rate_ip        per-client-IP cap             (429)
--   no_key         no credential presented       (401)
--   bad_key        credential not recognised     (401)
--   expired        key past its expiry           (403)
--   ip_denied      source IP outside allowlist   (403)
--   model_denied   model not on key's allowlist  (403)
--   no_route       no route matched the path     (404)
--   unknown_model  unified entry, model unknown  (404)
--   no_model_allowlist  no route declares models (404)
--   no_model       unified entry without "model" (400)
--   bad_body       request body unreadable       (400)
--   method         method not allowed            (405)
--   no_channel     every channel tripped/open    (503)
--   upstream       all channels failed           (502)
-- ---------------------------------------------------------------------------
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS reject_reason VARCHAR(32) DEFAULT '';

-- ---------------------------------------------------------------------------
-- api_keys.name: a human label for the key ("我的笔记本", "ci-runner").
-- Owner alone cannot tell two keys of the same person apart; the name can.
-- Empty on legacy rows — the console falls back to showing owner.
-- ---------------------------------------------------------------------------
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS name VARCHAR(128) DEFAULT '';

-- ---------------------------------------------------------------------------
-- request_logs: token metering + channel attribution
-- ---------------------------------------------------------------------------
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS model             VARCHAR(128) DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS prompt_tokens     INT DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS completion_tokens INT DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS total_tokens      INT DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS is_stream         BOOLEAN DEFAULT false;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS channel_id        BIGINT;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS err_msg           TEXT DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_request_logs_key_created   ON request_logs(api_key_id, created_at);
CREATE INDEX IF NOT EXISTS idx_request_logs_route_created ON request_logs(route_id, created_at);

-- ---------------------------------------------------------------------------
-- channels: one route can fan out to several upstreams (multi-channel relay).
--   priority: lower values are tried first (primary vs. backup tiers).
--   weight:   relative share inside the same priority tier.
-- A route with no channel rows falls back to its own base_url/key/format, so
-- deployments created before this table keep behaving exactly as before.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channels (
  id                  BIGSERIAL PRIMARY KEY,
  route_id            BIGINT NOT NULL,
  name                VARCHAR(64) DEFAULT '',
  base_url            TEXT NOT NULL,
  downstream_auth_key TEXT DEFAULT '',
  api_format          VARCHAR(64) DEFAULT 'generic',
  weight              INT DEFAULT 1,
  priority            INT DEFAULT 0,
  enabled             BOOLEAN DEFAULT true,
  created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_channels_route ON channels(route_id);
