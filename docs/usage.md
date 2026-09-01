# 使用文档

面向使用这个网关的人：怎么跑起来、怎么配路由和 Key、怎么查用量。
内部实现见 [architecture.md](architecture.md) 与 [design.md](design.md)。

## 1. 快速开始

```bash
cp .env.example .env        # 按需修改
docker-compose up --build
```

容器起好后 Postgres 自动执行 `init.sql`（建表 + 插入 demo 路由 `open-meteo`），
gateway 启动时种子一个 demo Key（`DEMO_API_KEY`，默认 `test-key-123`）。

验证：

```bash
curl -H "X-API-Key: test-key-123" \
  "http://localhost:8080/v1/weather?latitude=39.9&longitude=116.4"
```

首次请求走上游并写入缓存；短时间内重复请求命中 `X-Cache: HIT`。
不带 Key 返回 401，超桶返回 429。

压测：

```bash
# go install github.com/rakyll/hey@latest
hey -n 5000 -c 100 -H "X-API-Key: test-key-123" \
  "http://localhost:8080/v1/weather?latitude=39.9&longitude=116.4"
```

关注吞吐、p99、限流拒绝率、缓存命中率，以及上游实际调用数——
后者应远低于入站数。

## 2. 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis 地址 |
| `REDIS_PASSWORD` | 空 | Redis 密码 |
| `PG_DSN` | `postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable` | Postgres 连接串 |
| `GATEWAY_ADDR` | `:8080` | 监听地址 |
| `DEMO_API_KEY` | 空 | 启动时自动种子的 demo Key |
| `ADMIN_TOKEN` | 空 | 管理端令牌。**不设置则整个 `/admin/*` 硬禁用返回 403** |
| `UPSTREAM_TIMEOUT_SEC` | 180 | 单次上游请求超时，含读完整个流。调小会把长回答截成 502 |
| `MAX_ATTEMPTS` | 3 | 最多尝试几条渠道，1 为关闭故障转移 |
| `TRUST_PROXY` | false | 是否信任 `X-Forwarded-For`。公网直曝时打开等于任何人可伪造来源 IP |
| `INJECT_STREAM_USAGE` | true | 自动注入 `stream_options.include_usage`，关闭则流式无法统计 token |
| `QUOTA_FLUSH_SEC` | 10 | Redis 配额回写 Postgres 的间隔 |
| `AUTO_MIGRATE` | true | 启动时执行 `migrate.sql`（幂等） |
| `UNIFIED_PREFIX` | `/v1` | 统一入口前缀，设 `off` 关闭 |
| `ROUTE_RELOAD_SEC` | 10 | 多副本路由同步检查间隔，0 为关闭 |
| `IP_RPS` | 0 | 单 IP 限流，默认关闭 |

> 已有数据库升级靠 `migrate.sql`（启动时自动跑，`AUTO_MIGRATE=false` 可关）。
> `init.sql` 只在首次初始化数据目录时跑一次，之后所有 schema 变更都放 `migrate.sql`。

## 3. 寻址：两种方式

### 方式一（推荐）：统一入口 `/v1`，按 model 路由

客户端把 `base_url` 指向网关的 `/v1`，之后当它是 OpenAI。**换供应商 = 换 model 名字，地址不变。**

```python
from openai import OpenAI
client = OpenAI(base_url="http://网关/v1", api_key="gw-你的Key")
client.chat.completions.create(model="deepseek-v4-flash", messages=[...])
```

- 网关读请求体的 `model`，在所有路由的 `models` 白名单里查找，命中哪条走哪条
- `/v1` 是**虚拟版本号**，转发前剥掉：客户端发 `/v1/chat/completions`，上游收到 `base_url + /chat/completions`
- `GET /v1/models` 由网关自己应答，聚合所有路由声明过的模型
- 未知 model → 404 并列出可用模型；缺 model → 400

> 想让一条路由出现在统一入口，**必须给它配 `models` 白名单**，那就是路由依据。
> 没配的只能通过方式二访问，否则「这个 model 归谁」有歧义。

### 方式二：专属前缀，强制指定渠道

`http://网关/{match_prefix}/...`，一条前缀绑一条路由。用于：

- 同一个模型多家供应商都有，想锁定某一家
- 客户端不支持自定义 base_url

**不要再自己加 `/v1`**：前缀已指向具体上游，再拼会得到 `.../v1/v1/chat/completions` 而 404。

**前缀匹配是最长前缀优先。** 同时配了 `/v1` 和 `/v1/messages` 时，
`/v1/messages` 一定命中更具体的那条，与配置顺序无关。
唯一仍冲突的情况是两条路由前缀**完全相同**——此时只有靠前的生效，
控制台会给后面的打上「前缀重复」标记。

## 4. 鉴权

| 写法 | 适用 |
|---|---|
| `Authorization: Bearer <key>` | OpenAI 官方 SDK 及大多数第三方客户端 |
| `X-API-Key: <key>` | 自定义客户端、curl |

两种都收。携带凭据的那个头在鉴权后被移除，**不会转发给上游**——
否则用户的网关 Key 会被原样送给供应商，而凭证注入逻辑看到
`Authorization` 已占用还会跳过注入真实的下游 Key。

需要自带上游凭据时用 `X-Provider-Key`，该头原样透传。

## 5. 配置下游路由

控制台首页有表单面板，也可直接用 API：

| 字段 | 含义 |
|---|---|
| `name` | 上游名称（同时作为限流 / 熔断维度）；留空则取 base_url 的 hostname |
| `base_url` | 真实下游地址。**唯一必填项**，过 SSRF 校验 |
| `match_path` | 网关前缀，如 `/ip/*`。**可留空**——留空时按 `/<name>` 自动分配并避让重名 |
| `upstream_rps` | 该上游令牌桶速率（默认 50） |
| `cache_ttl` | GET 缓存秒数（默认 30） |
| `cb_enabled` | 是否启用熔断（连续失败 5 次开路 10 秒） |
| `models` | 模型白名单，统一入口的路由依据 |
| `cache_scope` | `global`（默认）或 `key` |
| `api_format` | `openai-chat` / `openai-responses` / `anthropic-messages` / `generic` |

```bash
AT="X-Admin-Token: admin-secret-change-me"

# 新增：用户访问 /ip/* 即转发到 https://api.ipify.org
curl -H "$AT" -H "Content-Type: application/json" -X POST \
  http://localhost:8080/admin/routes \
  -d '{"name":"ipify","base_url":"https://api.ipify.org","match_path":"/ip/*","upstream_rps":50,"cache_ttl":30,"cb_enabled":true}'

# 用户侧访问（只带自己的 Key）
curl -H "X-API-Key: test-key-123" "http://localhost:8080/ip?format=json"

# 删除（软删除：保留审计行，status=0）
curl -H "$AT" -X DELETE "http://localhost:8080/admin/routes?id=6"
```

新增 / 更新后立即热加载，无需重启。

**默认缓存是全局共享的**（`cache_scope=global`）。对天气、汇率这类公开接口是特性；
**会返回用户私有数据的路由必须把缓存归属改成「按 Key 隔离」**，
否则第一个调用方的响应会发给下一个提问的人。

## 6. 多渠道与故障转移

一条路由可挂多条渠道，每条有自己的 Base URL、Key、优先级与权重：

- **优先级 `priority`**：数字越小越先尝试。主渠道挂了才轮到下一档——「主供应商 + 备供应商」
- **权重 `weight`**：同一档内按权重比例分流——「同一供应商的多个账号分摊限额」
- **故障转移**：5xx、429、408 自动切下一条重试（最多 `MAX_ATTEMPTS` 次）。
  其他 4xx 不重试——那是调用方的问题，换渠道只是白烧配额
- 重试会用缓冲的原始 body 重放，不会把空 body 发给下一条渠道
- 已熔断的渠道被跳过；全部熔断时退化为「都试一遍」，交给上游真实报错

```bash
# 给路由加备渠道：主用 A，A 挂了自动走 B
curl -X POST http://localhost:8080/admin/channels -H "$AT" -H 'Content-Type: application/json' \
  -d '{"route_id":1,"name":"provider-b","base_url":"https://api.provider-b.com/v1","priority":1}'
```

## 7. API Key 管理

`POST /admin/keys` 签发，`PUT /admin/keys?id=` 修改，`DELETE /admin/keys?id=` 永久删除。

| 能力 | 字段 | 行为 |
|---|---|---|
| 配额 | `quota_limit` | 以 token 为单位计费；`<= 0` 表示不限；用尽 429 |
| 过期 | `expires_at` | 空表示永不过期；过期 403 `api key expired` |
| 来源 IP | `allowed_ips` | 支持单个 IP 与 CIDR，逗号分隔 |
| 模型白名单 | `allowed_models` | Key 级限制 |
| 免记录 | `no_log` | 跳过审计与指标，供冒烟测试使用 |

**来源 IP 默认不信任 `X-Forwarded-For`**，需显式开启 `TRUST_PROXY=true`——
否则任何人伪造该头就能绕过限制。

**模型白名单**在路由级（`routes.models`）与 Key 级（`api_keys.allowed_models`）都会校验：

- 只校验 `POST/PUT/PATCH`——`GET /v1/models` 这类发现接口不该被拦
- `generic` 格式的路由完全不窥探请求体（可能是文件上传）
- body 读完后用 `io.NopCloser` 回填，超过 1 MiB 用 `MultiReader` 拼回未读部分，**流式转发不受影响**

## 8. 用量统计

- **非流式**：直接解析响应 JSON 的 `usage`
- **流式**：边转发边扫描 SSE / NDJSON 帧，从含 `usage` 的尾帧里取
- 自动为流式 chat 请求注入 `stream_options.include_usage`
  （`INJECT_STREAM_USAGE=false` 可关）——否则供应商不发用量事件，每次只能按 1 计
- 兼容 OpenAI（`prompt_tokens`/`completion_tokens`）与 Anthropic（`input_tokens`/`output_tokens`）
- 响应里解析不到 `usage` 时按 1 计，保证「发了请求就一定扣」

看板：`GET /admin/usage?hours=24`，按 Key / 模型 / 日期聚合。

除 token 数外还记录 TTFT（首 token 延迟）、tokens/s，
以及 prompt cache 的命中与写入 token——
「为 4 万输入 token 付费」和「为 4 万输入 token 付费但只花十分之一」是截然不同的成本。

## 9. 监控台与指标

浏览器打开 `http://localhost:8080/admin/`，用 `ADMIN_TOKEN` 登录。
页面零外部依赖，HTML 编译进二进制。页签：总览 / 路由配置 / API Keys /
用量看板 / 调试台 / 请求日志 / 接入指南，支持 `#hash` 深链。

`GET /admin/metrics`（JSON）返回：

- 累计：总请求 / 缓存命中 / 限流拒绝 / 错误
- 率值：缓存命中率 / 拒绝率 / 错误率
- 时序：最近 60 秒的 QPS、缓存命中、限流拒绝
- 熔断：各上游 `open` / `closed`

`GET /metrics` 是 Prometheus 文本格式，含请求计数与延迟直方图。

调试台 `POST /admin/playground` 服务端直连上游，跳过鉴权限流，**不计入统计**。

### 健康检查

两个端点都不需要任何凭据，也不经过鉴权、限流、配额和日志中间件——
每几秒一次的探针若走公开链，会被 401 拒绝（读起来像"网关挂了"），
还会往审计日志里灌满探针行。

| 端点 | 语义 | 依赖不通时 |
|---|---|---|
| `GET /healthz` | 进程活着、监听器在服务 | 仍 `200` |
| `GET /readyz` | 这个副本现在能处理请求 | `503`，响应体指名是哪个依赖 |

两者的区别是刻意的：

- **`/healthz` 是 liveness，不碰任何依赖。** 依赖一抖动就判死，会让监管者
  去重启容器——重启既修不好依赖，还会丢掉内存里的路由表；Redis 短暂不通时
  更是会让所有副本同时被打掉。所以 Dockerfile 的 `HEALTHCHECK` 用它。
- **`/readyz` 是 readiness，会真去 ping Redis 和 Postgres。** 没有 Redis，
  网关查不了 key、算不了配额、限不了流，每个请求都会以一种看起来像调用方
  错误的方式失败。`503` 的响应体形如 `not ready: redis: EOF`，指名依赖。
  两个依赖各有独立的 2 秒超时，一个慢不会让另一个被误报。

`deploy/deploy.sh` 发版后轮询 `/readyz` 等就绪（最多 30 秒），不再 `sleep 3`——
容器重建后要跑迁移、同步 key 缓存、加载路由，慢的时候不止 3 秒。

## 10. 流式

上游返回 `text/event-stream` 或 `application/x-ndjson` 时，
网关逐块 flush 转发，不等生成结束；非流式仍走缓冲快路径。

单次上游请求的超时由 `UPSTREAM_TIMEOUT_SEC` 控制（默认 180 秒，含读完整条流）。
LLM 生成动辄几十秒，调小会把长回答截断成 502。

## 11. 安全

`createRoute` / `updateRoute` 会校验 `base_url`：

- 仅允许 `http` / `https`
- 拒绝 `localhost`、`0.0.0.0`、`::1`、云元数据 `169.254.169.254`
- 拒绝解析后是回环 / 私网 / 链路本地 / 未指定地址的 IP

```bash
# 试图指向云元数据
curl -H "$AT" -H "Content-Type: application/json" -X POST \
  http://localhost:8080/admin/routes \
  -d '{"name":"meta","base_url":"http://169.254.169.254/latest/","match_path":"/meta/*"}'
# => 400 cloud metadata endpoint blocked
```

> 注意：这是**配置时静态校验**，DNS-rebinding 仍可能绕过。
> 生产环境须再加出口 IP 白名单 + 连接时 IP 校验。

## 12. 端到端冒烟

```bash
go test ./...                              # 单元与集成，约 5 秒
ADMIN_TOKEN=dev-admin-token scripts/e2e.sh # 端到端，需网关在运行
```

E2E 覆盖整条真实链路（真 Redis、真 PG、真 HTTP），共 26 项断言，
含多渠道故障转移的请求体重放与 4xx 不重试。
依赖外网 httpbin.org 的用例不可达时标 SKIP，**不会伪装成通过**。
