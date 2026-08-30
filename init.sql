CREATE TABLE IF NOT EXISTS api_keys (
  id             BIGSERIAL PRIMARY KEY,
  key_hash       VARCHAR(64) UNIQUE,
  owner          VARCHAR(64),
  name           VARCHAR(128) DEFAULT '',     -- human label to tell keys of the same owner apart
  status         SMALLINT DEFAULT 1,
  quota_limit    INT DEFAULT 100000,
  quota_used     INT DEFAULT 0,
  expires_at     TIMESTAMPTZ,                  -- NULL = never expires
  allowed_ips    TEXT DEFAULT '',              -- comma-separated IPs or CIDRs, '' = any
  allowed_models TEXT DEFAULT '[]',            -- JSON array, '[]' = any model
  no_log         BOOLEAN DEFAULT false,        -- true = skip the request log/audit/metrics for this key (smoke tests)
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS routes (
  id                   BIGSERIAL PRIMARY KEY,
  name                 VARCHAR(64),
  base_url             TEXT,
  match_path           VARCHAR(128),
  auth_type            SMALLINT DEFAULT 1,
  upstream_rps         INT DEFAULT 50,
  cache_ttl            INT DEFAULT 30,
  cb_enabled           BOOLEAN DEFAULT true,
  api_format           VARCHAR(64) DEFAULT 'generic',   -- openai-chat / openai-responses / anthropic-messages / generic
  downstream_auth_key  TEXT DEFAULT '',                  -- provider's API key (gateway injects on forward)
  models               TEXT DEFAULT '[]',                -- JSON array of allowed model IDs, e.g. ["GLM-5.3-Flash"]
  cache_scope          VARCHAR(16) DEFAULT 'global',     -- 'global' = one cached response for all keys; 'key' = per-key entry
  status               SMALLINT DEFAULT 1
);

-- One route can fan out to several upstreams. See migrate.sql for the same DDL;
-- kept here too so a fresh install starts with the full schema.
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

CREATE TABLE IF NOT EXISTS request_logs (
  id                BIGSERIAL PRIMARY KEY,
  api_key_id        BIGINT,
  route_id          BIGINT,
  method            VARCHAR(8),
  path              TEXT,
  upstream          VARCHAR(64),
  status_code       INT,
  latency_ms        INT,
  cached            BOOLEAN,
  model             VARCHAR(128) DEFAULT '',
  prompt_tokens     INT DEFAULT 0,
  completion_tokens INT DEFAULT 0,
  total_tokens      INT DEFAULT 0,
  is_stream         BOOLEAN DEFAULT false,
  channel_id        BIGINT,
  err_msg           TEXT DEFAULT '',
  ttft_ms           INT       DEFAULT 0,   -- Time To First Token (ms), streaming only
  tokens_per_sec    NUMERIC(10,2) DEFAULT 0, -- completion throughput
  prompt_cache_hit_tokens   INT DEFAULT 0, -- input tokens the provider served from its own prefix cache (billed at the discounted rate)
  prompt_cache_write_tokens INT DEFAULT 0, -- input tokens the provider charged a premium to store (Anthropic only, ~1.25x)
  reject_reason       VARCHAR(32) DEFAULT '', -- which gate refused the request: quota / rate_ip / rate_key / rate_upstream / no_route / unknown_model / no_model / model_denied / expired / ip_denied / no_key / bad_key / no_channel / upstream / bad_body / method. Empty = it reached an upstream.
  created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_request_logs_created_at    ON request_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_request_logs_key_created   ON request_logs(api_key_id, created_at);
CREATE INDEX IF NOT EXISTS idx_request_logs_route_created ON request_logs(route_id, created_at);

CREATE TABLE IF NOT EXISTS quotas (
  id          BIGSERIAL PRIMARY KEY,
  api_key_id  BIGINT,
  period      VARCHAR(16),
  used        INT DEFAULT 0,
  updated_at  TIMESTAMPTZ DEFAULT now(),
  UNIQUE (api_key_id, period)
);

-- demo route: 公开天气 API（免费、无需密钥）
-- base_url 已包含真实上游路径 /v1/forecast；MatchPrefix 为对外暴露的路径前缀。
INSERT INTO routes (name, base_url, match_path, auth_type, upstream_rps, cache_ttl, cb_enabled)
VALUES ('open-meteo', 'https://api.open-meteo.com/v1/forecast', '/v1/weather/*', 1, 50, 60, true)
ON CONFLICT DO NOTHING;
