# API 中转站 (API Gateway)

自托管 API 网关 / 转发枢纽：统一接入 → 鉴权 → 限流 → 缓存 → 转发 → 熔断 → 审计，再转发到真实公开 API。
技术栈：Go + Redis + PostgreSQL。高并发来自入站请求，上游被温柔对待。

## 目录结构

```
api-gateway/
├── docker-compose.yml      # redis + postgres + gateway
├── Dockerfile
├── go.mod
├── init.sql                # 全新安装时的完整表结构 + demo route
├── .env.example
└── internal/
    ├── config/config.go
    ├── store/              # redis(令牌桶Lua) / db(种子+审计) / 指标
    │   ├── redis.go
    │   ├── db.go           # API Key 增删改查
    │   ├── keyinfo.go      # Key 策略（过期/IP/模型）的 Redis 缓存编解码
    │   ├── quota.go        # Redis 计数 + 异步回写 Postgres
    │   ├── migrate.go      # 启动时自动执行 migrate.sql（幂等）
    │   ├── migrate.sql     # 增量迁移：新列 + channels 表
    │   ├── audit.go
    │   └── metrics.go      # 管道化 Redis 计数器 + 时序序列
    └── gateway/
        ├── gateway.go      # 中间件链 + 渠道选择 + 故障转移 + 流式转发 + 用量嗅探
        ├── admin.go        # 管理端接口（keys/routes/channels/logs/usage/playground）
        ├── stats.go        # 延迟直方图 + Prometheus 文本输出
        ├── embed.go        # 内嵌监控台 HTML
        ├── admin_ui.html   # 监控台前端（零外部依赖）
        └── (LoadRoutes / LoadChannels)
└── cmd/server/main.go      # 装配入口
```

> 已有数据库升级靠 `migrate.sql`（启动时自动执行，`AUTO_MIGRATE=false` 可关闭）。
> `init.sql` 只在首次初始化数据目录时跑一次，所以之后的所有 schema 变更都放在 `migrate.sql` 里。

## 快速开始

```bash
cp .env.example .env        # 按需修改
docker-compose up --build
```

容器起好后：Postgres 自动执行 `init.sql`（建表 + 插入 demo route `open-meteo`）；gateway 启动时种子一个 demo API Key（`DEMO_API_KEY=test-key-123`）。

## 验证

```bash
# 转发到 Open-Meteo（公开天气 API，免费）
curl -H "X-API-Key: test-key-123" \
  "http://localhost:8080/v1/weather?latitude=39.9&longitude=116.4"
```

- 首次请求走上游，写入 Redis 缓存（TTL 见 route.cache_ttl）。
- 短时间内再次相同请求命中 `X-Cache: HIT`，不再打上游。
- 无 `X-API-Key` → 401；超桶 → 429。

## 压测（证明高并发在你的层）

```bash
# 装 hey: go install github.com/rakyll/hey@latest
hey -n 5000 -c 100 -H "X-API-Key: test-key-123" \
  "http://localhost:8080/v1/weather?latitude=39.9&longitude=116.4"
```

观测：吞吐(QPS)、p99、限流拒绝率、缓存命中率；上游实际调用数远低于入站（缓存+令牌桶保护）。

## 中间件链

请求分三棵树，由 `http.ServeMux` 按「精确路径优先于 `/admin/` 前缀」路由：

```
public  : /*        -> logging -> auth -> quota -> ratelimit -> cache -> proxy
adminUI : /admin/   -> 直接返回监控台 HTML（Token 已注入页面供前端拉 JSON）
adminAPI: /admin/{metrics,keys,routes,channels,logs,usage,playground} + /metrics
                   -> adminAuth(独立 Token) -> JSON
```

- 监控台页面**无需请求头 Token**即可在普通浏览器打开；页面内嵌 `ADMIN_TOKEN`，由 JS 在拉取 JSON 时带上。
- JSON 接口（`/admin/metrics` 等）仍受 `mwAdminAuth` 保护；未设 `ADMIN_TOKEN` 时整页返回 403 硬禁用。

- **auth**：`X-API-Key` 经 sha256 后查 Redis `key:{hash}`，取回该 Key 的完整策略（过期时间、配额上限、IP 与模型白名单）。过期返回 403，来源 IP 不在允许列表返回 403。
- **quota**：配额用尽返回 429。计数走 Redis（`INCRBY`），后台每 `QUOTA_FLUSH_SEC` 秒回写 Postgres，不在热路径上开事务。
- **ratelimit**：Redis Lua 令牌桶（原子取令牌），按 `bucket:{apikey}:{upstream}` 双维度。
- **cache**：GET 按 `method+path+query` 哈希缓存到 Redis。
- **proxy**：最长前缀匹配 route → **选渠道（优先级分档 + 档内加权随机）** → 转发上游（超时由 `UPSTREAM_TIMEOUT_SEC` 控制，默认 180s）→ 失败自动换渠道重试 → 流式响应逐块 flush、非流式缓冲后回写；带熔断器（连续失败开路上游）。
- **logging/metrics**：`logging` 中间件用 **Redis Pipeline** 一次性写入累计计数器与「每秒时序」（`stats:sec:{sec}` 等），避免每个请求多次往返；监控台据此算 QPS / 缓存命中率 / 限流拒绝率 / 错误率。
- **audit**：异步落 `request_logs`（后台 worker），同时写入 model 与 token 用量。

## 监控台 (Monitoring Console)

内置一个零外部依赖的 Web 监控台，HTML 直接编译进二进制（`//go:embed`），无需挂载静态目录。

- 访问：`http://localhost:8080/admin/`（需 `ADMIN_TOKEN` 鉴权，随页面注入 Token 供前端拉取 JSON）
- 实时指标：`GET /admin/metrics`（JSON）
  - 累计：总请求 / 缓存命中 / 限流拒绝 / 错误
  - 率值：缓存命中率 / 拒绝率 / 错误率
  - 时序：最近 60 秒 QPS、缓存命中、限流拒绝（用于前端画图）
  - 熔断：各上游 `open`/`closed`
- 只读查询：`GET /admin/routes`、`GET /admin/keys`、`GET /admin/channels?route_id=`、`GET /admin/logs`
- **路由管理（可配置下游 API）**：`POST /admin/routes`（新增）、`PUT /admin/routes?id=`（更新）、`DELETE /admin/routes?id=`（软删除）
- **渠道管理**：`POST /admin/channels`、`PUT /admin/channels?id=`、`DELETE /admin/channels?id=`
- **Key 全生命周期**：`POST /admin/keys`（签发）、`PUT /admin/keys?id=`（改状态/配额/过期/白名单）、`DELETE /admin/keys?id=`（永久删除）
- **用量看板**：`GET /admin/usage?hours=24`（按 Key / 模型 / 日期聚合）
- **调试台**：`POST /admin/playground`（服务端直连上游，跳过鉴权限流，不计入统计）
- **Prometheus**：`GET /metrics`（文本格式，含请求计数与延迟直方图）

页签：总览 / 路由配置 / API Keys / 用量看板 / 调试台 / 请求日志 / 接入指南，支持 `#hash` 深链。

## URI 寻址：两种方式

### 方式一（推荐）：统一入口 `/v1`，按 model 自动路由

客户端把 `base_url` 指向 `http://网关/v1`，之后就当它是 OpenAI。**换供应商 = 换 model 名字，地址永远不变**。

```python
from openai import OpenAI
client = OpenAI(base_url="http://网关/v1", api_key="gw-你的Key")
client.chat.completions.create(model="deepseek-v4-flash", messages=[...])
```

- 网关读请求体里的 `model`，在**所有路由的 `models` 白名单**里查找，命中哪条就走那条的渠道。
- `/v1` 是**虚拟版本号**，转发前会被剥掉：客户端发 `/v1/chat/completions`，上游收到的是 `base_url + /chat/completions`。
- `GET /v1/models` 由网关自己应答，聚合返回所有路由声明过的模型。
- 未知 model → 404，并在响应体里列出可用模型；缺少 model → 400。

> 想让一条路由出现在统一入口，**必须给它配 `models` 白名单**——路由依据就是它。
> 没配白名单的路由只能通过方式二访问，否则"这个 model 归谁"会有歧义。

### 方式二：供应商专属前缀，强制指定渠道

`http://网关/{match_prefix}/...`，一条前缀绑一条路由。用于：

- 同一个模型在多家供应商都有，想锁定某一家；
- 客户端不支持自定义 base_url。

⚠️ **这里不要再自己加 `/v1`**。前缀已经指向具体的上游了，客户端若再拼 `/v1`，上游会收到重复的版本号而 404（例如 `.../v1/v1/chat/completions`）。

### 鉴权：两种写法都收

| 写法 | 适用 |
|---|---|
| `Authorization: Bearer <key>` | OpenAI 官方 SDK、绝大多数第三方客户端（它们只会发这一种） |
| `X-API-Key: <key>` | 自定义客户端、curl |

**携带凭据的那个请求头会在鉴权后被移除**，不会转发给上游——否则用户的网关 Key 会被原样送给供应商，而供应商注入逻辑看到 `Authorization` 已占用还会跳过注入真实的下游 Key。

> 通用透传路由若需要携带你自己的上游凭据，请用 `X-Provider-Key` 头；`Authorization` 已被网关用作鉴权，到不了上游。

统一入口可用 `UNIFIED_PREFIX=off` 关闭，关掉后只剩方式二，行为与之前完全一致。

## 多渠道与故障转移

一条路由可挂多条渠道（`channels` 表），每条渠道有自己的 Base URL、API Key、优先级与权重：

- **优先级（priority）**：数字越小越先尝试。主渠道挂了才轮到下一档——用于「主供应商 + 备供应商」。
- **权重（weight）**：同一优先级档内按权重比例分流——用于「同一供应商的多个账号分摊限额」。
- **故障转移**：上游网络错误、`5xx`、`429`、`408` 自动切下一条渠道重试（最多 `MAX_ATTEMPTS` 次）。**4xx 不重试**——那是调用方的问题，换渠道只是白烧配额。
- 重试会用缓冲的原始 body 重放，不会把空 body 发给下一条渠道。
- 已熔断的渠道会被跳过；全部熔断时退化为「都试一遍」，交给上游真实报错。

```bash
# 给一条路由加备渠道：主用供应商 A，A 挂了自动走 B
curl -X POST http://localhost:8080/admin/channels -H "X-Admin-Token: $T" -H 'Content-Type: application/json' \
  -d '{"route_id":1,"name":"provider-b","base_url":"https://api.provider-b.com/v1","priority":1}'
```

### 配额、过期与白名单

- **配额**：以 token 为单位计费；响应里解析不到 `usage` 时按 1 计（保证「发了请求就一定扣」）。`quota_limit <= 0` 表示不限。用尽返回 429。
- **过期**：`expires_at` 为空表示永不过期；过期返回 403 `api key expired`。
- **来源 IP**：`allowed_ips` 支持单个 IP 与 CIDR，逗号分隔。**默认不信任** `X-Forwarded-For`（需显式开启 `TRUST_PROXY=true`），否则任何人伪造该头就能绕过限制。
- **模型白名单**：`routes.models`（路由级）与 `api_keys.allowed_models`（Key 级）都会校验请求体里的 `model`，不匹配返回 403。
  - 只校验 `POST/PUT/PATCH`——`GET /v1/models` 这类发现接口不该被拦。
  - `generic` 格式的路由完全不窥探请求体（可能是文件上传）。
  - 读取 body 后会用 `io.NopCloser` 回填，超过 1 MiB 用 `MultiReader` 拼回未读部分，**流式转发不受影响**。

### 用量统计（token 计费）

- 非流式：直接解析响应 JSON 的 `usage`。
- 流式：边转发边扫描 SSE 帧，从含 `usage` 的尾帧里取（OpenAI 的尾帧同时带 `model`）。
- 自动为流式 chat 请求注入 `stream_options.include_usage`（可用 `INJECT_STREAM_USAGE=false` 关闭）——否则供应商不发用量事件，每次调用只能按 1 计。
- 兼容 OpenAI（`prompt_tokens`/`completion_tokens`）与 Anthropic（`input_tokens`/`output_tokens`）。

### 配置下游第三方 API

控制台首页有「配置下游 API」面板，可按表单增删改下游路由；也可直接用 API：

| 字段 | 含义 |
| --- | --- |
| `name` | 上游名称（同时作为 `upstream` 限流/熔断维度）；留空时自动取 base_url 的 hostname |
| `base_url` | 真实下游地址，如 `https://api.ipify.org`（SSRF 校验：仅 http/https，禁止内网/回环/元数据地址）。**唯一必填项** |
| `match_path` | 网关前缀，如 `/ip/*`，用户据此前缀统一访问；**可留空**——留空时自动按 `/<name>` 分配并避让重名，响应会返回最终前缀 |
| `upstream_rps` | 该上游令牌桶速率（默认 50） |
| `cache_ttl` | GET 缓存秒数（默认 30） |
| `cb_enabled` | 是否启用熔断（连续失败 5 次开路 10s） |

新增/更新后立即 `reloadRoutes()` 热加载（`sync.RWMutex` 保护），**无需重启**。用户侧只要带上自己的 `X-API-Key`，按 `match_prefix` 前缀访问即可，网关负责鉴权/限流/缓存/转发。

**前缀匹配规则：最长前缀优先。** 同时配了 `/v1` 和 `/v1/messages` 时，`/v1/messages` 的请求一定命中更具体的那条，与配置先后顺序无关（早期版本取首个匹配，短前缀会吞掉长前缀，使后者永远不可达）。唯一仍然冲突的情况是**两条路由前缀完全相同**——此时只有靠前的那条生效，控制台会给后面几条打上「前缀重复」标记。

**流式（SSE）**：上游返回 `text/event-stream` 或 `application/x-ndjson` 时，网关逐块 flush 转发，不等生成结束；非流式响应仍走缓冲快路径。单次上游请求的超时由 `UPSTREAM_TIMEOUT_SEC` 控制（默认 180 秒，含读完整条流的时间）——LLM 生成动辄几十秒，调小会把长回答截断成 502。

```bash
AT="X-Admin-Token: admin-secret-change-me"

# 新增：用户访问 /ip/* 即被转发到 https://api.ipify.org
curl -H "$AT" -H "Content-Type: application/json" -X POST \
  http://localhost:8080/admin/routes \
  -d '{"name":"ipify","base_url":"https://api.ipify.org","match_path":"/ip/*","upstream_rps":50,"cache_ttl":30,"cb_enabled":true}'

# 用户侧统一入口（仅自己的 API Key，网关代理到真实第三方）
curl -H "X-API-Key: test-key-123" "http://localhost:8080/ip?format=json"
# => {"ip":"203.0.113.1"}

# 删除（软删除：保留审计行，status=0）
curl -H "$AT" -X DELETE "http://localhost:8080/admin/routes?id=6"
```

### 安全：SSRF 防护

`createRoute`/`updateRoute` 调用 `validateRoute` 校验 `base_url`：
- 仅允许 `http`/`https` 协议；
- 拒绝 `localhost`、`0.0.0.0`、`::1`、云元数据 `169.254.169.254`；
- 拒绝解析后是回环 / 私网 / 链路本地 / 未指定地址的 IP。

> 注意：当前为「配置时」静态校验，DNS-rebinding 仍可能绕过。生产环境须再加**出口 IP 白名单 + 连接时 IP 校验**。

```bash
# 试图指向云元数据 -> 400
curl -H "$AT" -H "Content-Type: application/json" -X POST \
  http://localhost:8080/admin/routes \
  -d '{"name":"meta","base_url":"http://169.254.169.254/latest/","match_path":"/meta/*"}'
# => 400 cloud metadata endpoint blocked
```

> 不设置 `ADMIN_TOKEN` 时，整个 `/admin/*` 树会被**硬禁用**（返回 403），避免误暴露。

```bash
# 指标（curl 需带 Admin Token）
curl -H "X-Admin-Token: admin-secret-change-me" \
  "http://localhost:8080/admin/metrics" | head -c 400

# 浏览器打开监控台
open "http://localhost:8080/admin/"
```

## 面试可讲的点

令牌桶 vs 漏桶、Redis+Lua 原子、分布式限流一致性、缓存与请求合并降上游压力、熔断器状态机、无状态网关水平扩展、**用 Redis Pipeline 批量写指标避免热路径多次往返**、**运行时配置下游路由 + 热加载（sync.RWMutex）+ SSRF 防护**。

## 已知边界

这些不是 bug，是当前设计下的确定行为，部署前需要知道：

- **默认缓存是全局共享的**（`cache_scope=global`），缓存键只有 `method+path+query`。
  对天气、汇率这类公开接口这是特性（省上游调用）；会返回用户私有数据的 GET 路由
  **必须把「缓存归属」改成「按 Key 隔离」**。缓存命中不扣配额（没有消耗上游容量）。
- **Prometheus 的延迟直方图是进程内指标**。多实例时每个实例只上报自己的分布，
  需要在 Prometheus / Grafana 侧做聚合。计数器类指标来自 Redis，是全局的。
- **`cache_ttl` 传 0 会被当作 30 秒**，不会关闭缓存。
- **审计日志异步落库**，进程被 `SIGKILL` 时最多丢失缓冲区里的那批（默认 1024 条）。
  配额计数在 Redis 里，不受影响。
- **`IP_RPS` 默认关闭**。开启前先确认流量不是全走同一个 NAT / 出口网关，
  否则会把所有用户算成一个来源一起限流。

## 可扩展点（剩下的）

已经做完的：管理台 CRUD、Prometheus 导出、多渠道与故障转移、配额/过期/IP/模型限制、
用量看板、调试台、统一 `/v1` 入口、多实例路由同步、缓存按 Key 隔离、三层限流。

- 请求合并（Coalescing）减少重复上游调用
- K8s HPA 自动扩缩
- 协议转换（OpenAI ↔ Anthropic ↔ Gemini）——有意不做，透传由上游各自负责
- 在线充值 / 费率计费 / 多租户分组——超出网关本身的范畴

## 端到端冒烟测试

`go test` 覆盖单元与集成逻辑（用 httptest 假上游）；`scripts/e2e.sh` 覆盖整条链——
真实 HTTP、真实 Redis / Postgres、真实中间件顺序。

```bash
# 网关需要在运行，并知道它的 ADMIN_TOKEN
ADMIN_TOKEN=dev-admin-token scripts/e2e.sh
```

覆盖：管理接口鉴权、Prometheus、统一入口（模型列表 / 未知模型 / 缺 model）、
两种鉴权写法、来源 IP 限制、Key 过期、配额拦截、模型白名单、多渠道故障转移
（含请求体重放与 4xx 不重试）、用量统计。共 26 项断言。

需要外网 httpbin.org 的用例在不可达时会标 SKIP，不会伪装成通过。
脚本会清理自己创建的路由 / Key / 渠道（软删除）。网关本身的范畴
