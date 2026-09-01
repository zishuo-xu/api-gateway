# 架构文档

面向需要理解或改动这套代码的人。使用与配置见 [usage.md](usage.md)，
关键子系统的设计权衡见 [design.md](design.md)。

## 1. 它站在哪里

```
客户端（OpenAI SDK / curl / 自己的应用）
    │  只认网关的 base_url 和网关发的 key
    ▼
┌─────────────────────────────────────┐
│  gateway（无状态，可多副本）          │
│  鉴权 · 配额 · 限流 · 缓存 · 路由     │
└─────────────────────────────────────┘
    │  持有各家真实凭证，按渠道挑选
    ▼
上游供应商 A / B / C（OpenAI、Anthropic、GLM……）
```

网关**刻意不做**协议转换。上游各家接口形态不同，转换层要跟随每家的协议变更，
维护成本远超收益——透传交给上游自己负责。

技术选型上只用标准库 `net/http`，没有 Web 框架，中间件链自研；
存储侧 `go-redis` + `lib/pq`，**没有 ORM**，SQL 全部手写。

## 2. 进程内结构

入口 `Handler()` 把请求分到三条互不干扰的链：

| 链 | 匹配 | 中间件 |
|---|---|---|
| 公开链 | `/`（兜底） | `mwLogging → mwAuth → mwQuota → mwRateLimit → mwCache → proxy` |
| 管理 API | `/admin/{metrics,keys,routes,channels,logs,usage,playground}`、`/metrics` | `mwAdminAuth` |
| 管理界面 | `/admin/` | 无（页面本身不含凭据） |
| 健康检查 | `/healthz`、`/readyz` | 无（也不写审计行、不计入指标） |

健康检查刻意落在三条链之外：探针每几秒一次、永远不停，一旦走公开链就会被
`mwAuth` 拒成 401，还会往 `request_logs` 里灌满探针行。两者的语义区别见
[usage.md](usage.md) 第 9 节。

`/admin/login` 与 `/admin/logout` **刻意放在 `mwAdminAuth` 之外**——
登录是获取会话的动作，注销只吊销调用方自己持有的会话。

`/favicon.ico` 注册在根 mux 上，早于兜底规则。浏览器的图标探测不会带 API Key，
走公开链必然被 `mwAuth` 拒为 401，然后被 `mwLogging` 记成一次失败请求——
每个页面加载、每个标签页、永远如此。审计日志、错误率指标和运维的耐心
都会被一件根本不是故障的事情消耗掉。单独处理之后它不产生任何日志行、审计行或计数器。

### 后台 goroutine

进程内除请求处理外还跑三个常驻协程：

- `StartAuditor` —— 消费审计 channel，攒批写 `request_logs`
  - 满 64 条或距上次写入 500ms 触发一次批量 INSERT。单条一个 round trip
    会让这个纯旁路成为数据库最贵的写入来源
  - 批量语句失败时降级为逐条重试：一条坏数据（例如 `reject_reason` 超过
    VARCHAR(32)）只赔掉它自己，不连带同批的另外 63 条
  - 停止函数会等队列排空（上限 3s）才返回。返回即代表行已落盘；忽略它的
    返回值，就等于每次发版都静默丢掉缓冲区尾部
- `FlushQuota` 定时器 —— 把 Redis 配额计数刷回 Postgres
- `StartRouteReloader` —— 监听路由版本变化并重载路由表

请求侧投递审计行是**非阻塞**的：channel 满就丢弃并计数（
`store.AuditDropped()`）。审计是遥测，写在响应已经发出之后，为它等待
就等于让 Postgres 的写入速度变成请求延迟的一部分。丢行必须可见——
从 `request_logs` 读花费的人没有别的途径知道这张表是缺行的。

## 3. 请求全链路

```
mwLogging     记录开始时间、生成 meta（贯穿全程的请求上下文）
   ↓
mwAuth        取 key → Redis 查元数据 → 校验过期与 IP 白名单 → 摘掉凭据头
   ↓
mwQuota       已用 >= 上限？→ 429（带 X-Quota-Limit / X-Quota-Used）
   ↓
mwRateLimit   一次 Lua 调用原子扣三个令牌桶（上游 / Key / IP）
   ↓
mwCache       GET 且命中 → 直接返回（X-Cache: HIT）
   ↓
proxy         ├─ resolveTarget      选路由（统一入口按 model，否则按路径前缀）
              ├─ authorizedModel    校验模型白名单
              ├─ orderedChannels    按 priority 分层、层内按 weight 洗牌
              ├─ forward            逐个尝试，5xx / 网络错误换下一个
              └─ writeUpstream      流式边转边解析 usage
   ↓
旁路写入      缓存写回 · 审计投递 · 指标 Pipeline · 配额扣减
```

### 关于 mwAuth 里的一个细节

身份解析刻意**早于**任何策略检查。`mwLogging` 要读 `KeyInfo.NoLog` 来决定是否记录，
所以一个 key 即使在被自己的过期时间或 IP 白名单挡下时，也必须是可识别的。
先检查再解析的话，一个专门用来触发这类失败的冒烟测试 key
反而每次都会留下日志。被拒的请求到不了 proxy，`m.Billed` 保持 false，不会计费。

反过来，缺失或伪造的凭据在上面就直接返回、根本解析不出 key，
这类请求**依然会记录**——没有归属者，而这正是运维需要看见的。

## 4. 状态放在哪里

| 位置 | 内容 | 为什么 |
|---|---|---|
| **进程内存** | 路由表与渠道（`sync.RWMutex`） | 每请求都要匹配，不能有网络往返 |
| **Redis** | 限流桶、响应缓存、配额计数、熔断器、指标、管理会话、路由版本 | 高频读写，且必须多副本共享 |
| **Postgres** | 路由/渠道/Key 定义、审计日志、配额持久化值 | 需要持久、需要查询、需要事务 |

路由表是内存里唯一的可变共享状态，写操作（管理接口改完）走 `reloadRoutes()`：
先换本副本的内存表，再递增 Redis 里的版本号通知其他副本。

## 5. 旁路：不阻塞响应的部分

热路径上只做必须做完的事，其余投递给后台：

- **审计日志**：写进带缓冲的 channel（默认 1024），单条异步 INSERT
- **配额扣减**：`INCRBY` 打在 Redis，同时把 key 加进 `quota:dirty` 脏集合
- **配额落盘**：定时器每 `QUOTA_FLUSH_SEC` 秒扫脏集合写回 Postgres
- **指标**：一次 Pipeline 写完所有计数器

代价是进程被 `SIGKILL` 时，审计缓冲区里那批（最多 1024 条）会丢。
配额计数在 Redis 里，不受影响。

## 6. 多实例一致性

网关无状态，水平扩展只需要多起几个容器。三件事保证副本间一致：

**路由表**靠 Redis 里的 `routes:version` 计数器——它只是个**门铃**，
Postgres 才是事实来源。副本的 watcher 有两个触发条件：

1. 版本号动了（有同伴改了东西）→ 立即重载
2. 周期性强制重载（默认 5 分钟）→ 兜底 Redis 计数器丢失，
   以及运维直接改 SQL 的情况

版本号在启动后、第一次 tick 之前就先读一次做种。
否则同伴可能正好在我们启动读库和第一次检查之间递增了计数器，
种晚了就会漏掉那次变更。「从读不到变成读得到」也算变更。

**限流与配额**天然共享，因为都在 Redis，且用 Lua / INCRBY 保证原子。

**熔断状态**也在 Redis，一个副本观察到的上游故障对全体生效。

需要留意的是 Prometheus 的**延迟直方图是进程内指标**，
多实例时每份实例只上报自己的分布，聚合要在 Prometheus / Grafana 侧做。
计数器类指标来自 Redis，是全局的。

## 7. 部署拓扑

**本地开发**（`docker-compose.yml`）：gateway 直接 `go run`，
Redis 与 Postgres 起在容器里，端口映射到宿主机。

**生产**（`deploy/docker-compose.prod.yml`）：三个容器——gateway、postgres、redis。
`deploy/deploy.sh` 只重建网关容器，数据库与数据不动。

```
                    ┌──────────────┐
   :8080 ──────────▶│   gateway    │──┐
                    └──────────────┘  │
                                      ├──▶ redis:6379
                    ┌──────────────┐  │    （限流/缓存/配额/熔断/指标/会话）
   :8080 ──────────▶│ gateway ×N   │──┤
   （可选多副本）     └──────────────┘  │
                                      └──▶ postgres:5432
                                           （定义/审计/配额持久化）
```

## 8. 数据模型

| 表 | 作用 | 要点 |
|---|---|---|
| `api_keys` | API 密钥 | 存 `key_hash` 不存明文；过期、IP 白名单、模型白名单、`no_log` |
| `routes` | 路由规则 | `match_path` 前缀匹配、上游 RPS、缓存 TTL 与归属、`api_format` |
| `channels` | 渠道 | 一条路由对多个上游，`priority` + `weight` 决定顺序 |
| `request_logs` | 审计 | token 用量、TTFT、tokens/s、缓存命中、`reject_reason` |
| `quotas` | 配额周期用量 | `(api_key_id, period)` 唯一 |

token 计数器刻意放在 `request_logs` 里而不是单独的表，
这样用量看板回答「谁在哪个模型上花了多少」只需要一次索引扫描。
三个复合索引覆盖按时间、按 Key、按路由三种查询模式。

## 9. Redis key 总表

| Key | 用途 | TTL |
|---|---|---|
| `key:{hash}` | Key 元数据（`KeyInfo`） | 由同步逻辑维护 |
| `bucket:up:{dim}` | 上游总闸令牌桶 | 1 小时 |
| `bucket:key:{id}:{dim}` | 单 Key 令牌桶 | 1 小时 |
| `bucket:ip:{ip}` | 客户端 IP 桶（`IP_RPS` 开启时） | 1 小时 |
| `cache:{prefix}{hash}` | 响应缓存 | 路由的 `cache_ttl` |
| `quota:{id}` | 配额计数 | 无（靠 dirty 集刷盘） |
| `quota:dirty` | 待落盘的 key 集合 | 无 |
| `cb:{upstream}` | 熔断状态，值为 `open` | 10 秒 |
| `cb:{upstream}:fail` | 连续失败计数 | 30 秒 |
| `stats:{total,cached,rejected,errors}` | 累计计数器 | 无 |
| `stats:sec:{sec}` 及 `cached/rej/err` 变体 | 每秒序列 | 70 秒 |
| `adminsess:{id}` | 管理端会话 | 会话 TTL |
| `routes:version` | 路由版本号（门铃） | 无 |

每秒序列保留 60 秒窗口（图表用），TTL 设为 70 秒留一点余量。
