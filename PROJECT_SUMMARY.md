# API Gateway 项目总结

> 面向复盘、简历与技术交接。使用说明见 `README.md`，初版设计见 `api-gateway-design.md`。

## 一、它是什么

一个自托管的 **OpenAI 兼容 API 网关**。客户端把 `base_url` 指向网关，网关负责鉴权、
限流、路由、多渠道故障转移、用量计费与审计，再转发到真实上游。

解决的真实问题：手里有多个 LLM 供应商（不同 key、不同额度、不同稳定性）时，
把「选哪家、挂了怎么办、谁用了多少 token」这些事收敛到一个无状态层，
对客户端只暴露一个入口和一把自己的 key。

### 规模

| 项 | 数量 |
|---|---|
| Go 源码（不含测试） | 5,316 行 / 14 个文件 |
| 测试代码 | 4,054 行 / 88 个用例 |
| 端到端冒烟 | 26 项断言（`scripts/e2e.sh`） |
| 数据表 | 5 张 |
| 管理接口 | 20+ 个端点 |
| 外部依赖 | Redis 7、PostgreSQL 15（无 ORM、无 Web 框架） |

标准库 `net/http` + `go-redis` + `lib/pq`，中间件链与路由全部自研。

---

## 二、请求链路

```
客户端
  │  统一入口 /v1（按 model 路由） 或 供应商前缀 /<route>/*
  ▼
mwLogging → mwAuth → mwQuota → mwRateLimit → mwCache → proxy
                                                          │
                          ┌───────────────────────────────┘
                          ▼
              resolveTarget（选路由 + 选渠道）
                          ▼
              orderedChannels（priority 分组 → 权重洗牌）
                          ▼
              forward → 失败? → retryableStatus? → 下一个渠道
                          ▼
              writeUpstream（流式边转边解析 usage）
                          ▼
              写缓存 / 审计异步落库 / 指标 Pipeline 批量写
```

网关进程**无状态**：路由表在内存（RWMutex 保护），限流、配额、熔断、缓存全在 Redis。
多副本靠 `ROUTE_RELOAD_SEC` 定时比对版本号同步路由，靠 Redis 共享限流与配额。

---

## 三、已实现的能力

### 接入与路由
- 统一入口 `/v1`：客户端只配一个 `base_url`，网关从请求体的 `model` 字段选上游
- 供应商专属前缀：强制指定渠道，绕过自动路由
- 路由热加载，改完立即生效，多实例自动同步

### 鉴权与限制
- 两种鉴权写法都收：`Authorization: Bearer` 与 `X-API-Key`
- Key 支持：过期时间、IP 白名单（含 CIDR）、模型白名单、配额上限
- **三层限流**：按 Key（Redis Lua 令牌桶）、按上游 RPS、按 IP（默认关闭）
- 缓存支持 `global` / `key` 两种归属，后者用于会返回用户私有数据的路由

### 多渠道与故障转移
- 一条路由可挂 N 个渠道，按 `priority` 分组、组内按 `weight` 权重洗牌
- 5xx 与网络错误自动换渠道重试，4xx 不重试（请求本身有问题，换渠道也没用）
- 重试需要重放请求体，因此入站 body 会被 peek 到内存并复位
- 熔断：按上游记录失败，开路后快速失败，半开探测恢复

### 用量与计费
- 流式响应里 provider 通常不发 usage 事件，网关用 `usageSniffer` **边转发边解析 SSE**，
  从 chunk 里还原 prompt / completion tokens
- 可选注入 `stream_options.include_usage` 让 provider 补发最终 usage
- 统计 TTFT（首 token 延迟）与 tokens/s
- 区分 prompt cache 的命中与写入 token（Anthropic 写入按 1.25 倍计费）

### 可观测
- 内置监控台（`/admin/`，单文件 HTML，无前端构建）
- Prometheus 导出、用量看板、请求日志查询、调试 Playground
- `reject_reason` 字段记录每个请求被哪道关卡拦下，便于归因

### 安全
- SSRF 防护：拒绝 `localhost`、`0.0.0.0`、私有网段、云元数据 `169.254.169.254`
- 管理端登录限流（按 IP 记失败次数，防爆破）+ Redis 会话
- `TRUST_PROXY` 默认关闭，不盲信 `X-Forwarded-For`

---

## 四、技术要点

这部分的价值在于「为什么这么做」。

**为什么用 Redis + Lua 做限流**
多副本共享计数，且「取令牌 + 扣减」必须原子。纯 Go 实现在多实例下会各算各的。

**为什么流式要边转边解析**
非流式响应直接读 JSON 就能拿到 usage；流式是 SSE，provider 通常不返回 usage。
挂一个 `io.Writer` 装饰器在转发的同时逐行扫 `data:` 帧，从 delta 里还原，
避免为了统计而缓冲整个响应（那会破坏流式的意义）。

**为什么解析 model 要增量扫描**
`model` 在 JSON body 里的位置不固定，全量反序列化大 body 太贵。
`scanModelPrefix` 边读边匹配，够用就停。

**为什么审计日志异步落库**
同步写 PG 会把上游延迟直接叠加到客户端。用带缓冲的 channel 批量写，
代价是 `SIGKILL` 时最多丢一批（默认 1024 条）——配额计数在 Redis，不受影响。

**为什么指标用 Redis Pipeline**
热路径上每个请求要写好几个计数器，逐条往返 Redis 会把延迟放大几倍，
Pipeline 合成一次往返。

**为什么 4xx 不重试**
4xx 意味着请求本身有问题（key 无效、参数错误），换渠道也会得到同样的结果，
重试只是浪费上游额度和客户端时间。

---

## 五、数据模型

| 表 | 用途 | 要点 |
|---|---|---|
| `api_keys` | API 密钥 | 存 `key_hash` 不存明文；带过期、IP 白名单、模型白名单、`no_log` |
| `routes` | 路由规则 | `match_path` 前缀匹配、上游 RPS、缓存 TTL、缓存归属、`api_format` |
| `channels` | 渠道（上游实例） | 一条路由对多个渠道，`priority` + `weight` 决定顺序 |
| `request_logs` | 请求审计 | token 用量、TTFT、tokens/s、缓存命中、`reject_reason` |
| `quotas` | 配额周期用量 | `(api_key_id, period)` 唯一，Redis 计数定时刷回 |

`request_logs` 上建了三个复合索引，覆盖按时间、按 Key、按路由三种查询模式。

---

## 六、测试

```bash
go test ./...          # 88 个用例，约 5 秒
ADMIN_TOKEN=xxx scripts/e2e.sh   # 26 项断言，需网关在运行
```

单测用 `httptest` 假上游，覆盖中间件顺序、限流、缓存、模型提取、TTFT 等逻辑。
E2E 覆盖整条真实链路（真 Redis、真 PG、真 HTTP），包括多渠道故障转移的请求体重放与
4xx 不重试。依赖外网 httpbin.org 的用例不可达时标 SKIP，**不会伪装成通过**。

---

## 七、与初版设计的偏差

对照 `api-gateway-design.md`（初版讨论稿）：

**设计里没有、实际加上的**（需求演进）
- 多渠道故障转移与权重分发——初版只有单一上游
- 统一 `/v1` 入口按 model 路由——初版是纯路径前缀匹配
- token 计费与用量看板——初版只统计请求数
- SSRF 防护、Prometheus 导出、管理台 UI、调试 Playground
- 多实例路由同步、缓存按 Key 隔离、三层限流

**设计了但没做的**
- **请求合并（Coalescing）**：相同在途请求共享结果。实现需要在 Redis 上做在途请求锁
  与结果广播，复杂度和出错面都不小，而缓存已经覆盖了大部分重复读场景，性价比不够。
- **K8s HPA**：网关已经无状态、状态全在 Redis，扩缩容只是部署形态问题，
  当前 docker-compose 单机构已经够用。
- **协议转换**（OpenAI ↔ Anthropic ↔ Gemini）：**有意不做**。透传由上游各自负责，
  做转换意味着要跟随每家协议的变更，维护成本远超收益。

---

## 八、部署

生产用 `deploy/docker-compose.prod.yml`（gateway + postgres + redis），
`deploy/deploy.sh` 一键重建网关容器，数据库与数据不动。

主要配置项（环境变量，默认值见 `internal/config/config.go`）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `UPSTREAM_TIMEOUT_SEC` | 180 | 单次上游请求超时，含流式读完 |
| `MAX_ATTEMPTS` | 3 | 最多尝试几个渠道，1 为关闭故障转移 |
| `UNIFIED_PREFIX` | `/v1` | 统一入口前缀，设 `off` 关闭 |
| `ROUTE_RELOAD_SEC` | 10 | 路由热加载间隔，0 为关闭 |
| `QUOTA_FLUSH_SEC` | 10 | Redis 配额刷回 PG 的间隔 |
| `IP_RPS` | 0 | 按 IP 限流，默认关闭 |
| `TRUST_PROXY` | false | 是否信任 `X-Forwarded-For` |

注意：部署脚本里的健康检查地址由 `GATEWAY_HOST` 环境变量传入，仓库内不硬编码服务器地址。

---

## 九、已知边界

这些不是 bug，是当前设计下的确定行为：

- 默认缓存是全局共享的，会返回用户私有数据的 GET 路由必须改成按 Key 隔离
- Prometheus 的延迟直方图是进程内指标，多实例需在 Prometheus 侧聚合
- `cache_ttl` 传 0 会被当作 30 秒，不是关闭缓存
- 审计日志异步落库，`SIGKILL` 最多丢缓冲区一批
- `IP_RPS` 开启前要确认流量不是全走同一个 NAT，否则所有用户会被算成一个来源

---

## 十、如果继续做

按性价比排序：

1. **请求合并**——上游额度是真实成本，合并能直接省钱
2. **指标聚合层**——多实例直方图需要 Prometheus recording rules
3. **审计日志分级存储**——请求日志增长快，冷热分离
4. 在线充值与费率计费——已超出网关本身范畴，属于独立计费系统
