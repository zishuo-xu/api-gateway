# 部署运维文档

面向要在服务器上长期跑这套东西的人。软件怎么用见 [usage.md](usage.md)，
内部实现见 [architecture.md](architecture.md)。

> 本文档只描述结构与步骤。**订阅链接、OAuth token、服务器地址一律不入库**，
> 仓库是公开的。凭证只留在服务器上，以环境变量或挂载卷的形式提供。

## 1. 两种部署形态

**本地开发**（`docker-compose.yml`）：gateway 直接 `go run`，
Redis 与 Postgres 起在容器里，端口映射到宿主机。

**生产**（`deploy/docker-compose.prod.yml`）：gateway + postgres + redis 三个容器。
`deploy/deploy.sh` 只重建网关容器，数据库与数据不动。

## 2. 资源依赖清单

跑这套东西需要哪些外部资源、各自是否付费、断了会怎样。换服务器或排查故障时照这个清单核对。

### 基础设施

| 资源 | 说明 | 付费 | 断了会怎样 |
|---|---|---|---|
| 云服务器 | 2 核 / 1.6G / 40G 即可正常运行 | 是 | 全部停止 |
| 公网地址 | IP 或域名，客户端连它 | 随服务器 | 客户端连不上 |

磁盘要留余量：`request_logs` 随调用量持续增长，是最占地方的表。

### 中间件（compose 自动拉起，无需单独安装）

| 资源 | 用途 | 挂了的后果 |
|---|---|---|
| PostgreSQL | 路由 / Key / 渠道定义、审计日志、配额 | 新请求无法鉴权，审计与配额失效 |
| Redis | 限流桶、响应缓存、配额计数、熔断器、指标、会话 | **保护机制全部失效**，但转发仍能进行 |

两者都只在容器网络内，不暴露端口。注意 Redis 配了 `maxmemory` 上限与 LRU 淘汰，
目的是缓存涨起来时淘汰旧条目而不是 OOM。

### 上游 API

按性质分三类，处理方式不同：

- **付费订阅类**（各家 LLM 供应商）：受额度限制，需要盯着消耗
- **免费公开类**（天气、IP 查询等）：通常有速率限制，靠网关的缓存与上游 RPS 保护
- **测试类**（如 httpbin）：仅冒烟测试用，不服务真实流量

### Grok 订阅通道的额外资源

| 资源 | 性质 | 付费 | 断了会怎样 |
|---|---|---|---|
| SuperGrok Heavy 订阅 | xAI 账号订阅，**与网页聊天共享每周额度池** | 是 | 该通道不可用 |
| 机场订阅 | mihomo 用它拉代理节点 | 是 | 到 xAI 不通，该通道全断 |
| progrok | 开源 OAuth 桥，登录一次即可 | 否 | 该通道不可用 |
| mihomo | 开源代理客户端，**单点** | 否 | 该通道全断 |

### 外部服务

| 资源 | 用途 | 说明 |
|---|---|---|
| 代码托管 | 版本管理与灾备 | 不影响运行，影响协作与恢复能力 |
| 镜像仓库 | 拉取 postgres / redis / mihomo 等镜像 | 免费档有限流，新机器批量拉取可能失败 |

### 风险集中在哪

1. **代理出口是单点**：整个出境通道只有一个 mihomo，没有备用。
   网关的 failover 只能换上游，**换不了代理出口**——这是两套不同层次的冗余。
2. **机场订阅是不可控的外部付费依赖**，服务商出问题你无从修复。
3. **订阅额度与网页端共享**，API 调多了网页端就没额度。

要加固，方向是给代理层准备备用出口，而不是在网关层做冗余。

## 3. 生产拓扑

标准形态下网关无状态，可多副本：

```
                    ┌──────────────┐
   :8080 ──────────▶│   gateway    │──┐
                    └──────────────┘  │
                                      ├──▶ redis:6379
                    ┌──────────────┐  │
   :8080 ──────────▶│ gateway ×N   │──┤
                    └──────────────┘  │
                                      └──▶ postgres:5432
```

路由热加载靠 Redis 里的版本号广播，见 architecture.md 第 6 节。

## 4. 特殊上游：需要出境代理的通道

有些上游在国内网络下不可达。网关本身不做代理，
但可以把代理拆成独立容器，让某个渠道经由它出去——
这样代理只影响需要它的那条通道，其他上游照常直连。

下面以一条真实在跑的通道为例：**把 SuperGrok Heavy 的订阅额度
当作 OpenAI 兼容 API 使用**。

### 问题

`api.x.ai` 在国内直连不通。但订阅额度本身是有效的，
只是网络层到不了。所以需要解决两件事：

1. 网络出境——让请求能到 xAI
2. 凭据转换——订阅是 OAuth 登录态，不是 API Key，
   需要转成 OpenAI 兼容接口

### 四层链路

```
客户端
  │  统一入口 /v1，model=grok-4.6
  ▼
gateway :8080
  │  路由 grok-sub（models 白名单 ["grok-4.6"]）
  │  渠道 base_url = http://progrok:18645/v1
  ▼
progrok :18645        ← xAI 官方生态的 OAuth 桥
  │  HTTP_PROXY=http://mihomo:7890
  ▼
mihomo :7890          ← 机场订阅代理，url-test 自动选最快节点
  ▼
api.x.ai
```

**消耗的是订阅额度，不额外计费。**

### 各层职责

**mihomo**（`metacubex/mihomo`）只管出境。订阅链接拉下来若干节点，
配 `url-test` 策略组持续测速，自动挑当前最快可用的节点。
它不认识 Grok，也不认识网关。

**progrok**（`node:22-alpine` + `npm i -g progrok`）是凭据层。
通过 device-code 方式登录一次 xAI 账号（浏览器里输一个码），
拿到可自动刷新的 token，然后对外暴露一个 OpenAI 兼容的
`/v1/chat/completions`，转发到 `api.x.ai`。OAuth token 存在挂载卷里
（`/root`），容器重建不丢登录态。

**gateway** 只把它当成一条普通的 `openai-chat` 上游：
渠道 `base_url` 填 `http://progrok:18645/v1`，
路由的 `models` 填 `["grok-4.6"]`。
于是统一入口上 `model=grok-4.6` 的请求走订阅通道，
其他模型按各自的白名单走各自的渠道，互不影响。

### 网络怎么打通

这是最容易卡住的一步。**两个容器栈默认不在同一个 Docker 网络里**，
gateway 在 compose 的 `deploy_default`，而 progrok / mihomo 在 `grok-net`。
容器名 DNS 只在同一个网络内生效，跨网络 `http://progrok:18645` 解析不到。

解法是把 gateway **同时接入两个网络**：

```bash
docker network create grok-net
docker network connect grok-net deploy-gateway-1
```

连上之后 gateway 既能访问 `redis` / `postgres`（走 compose 网络），
也能解析 `progrok`（走 grok-net）。无需暴露端口到宿主机，
链路全部留在容器网络内部。

**但手工 connect 只是一次性的。** compose 重建容器时，新容器只会连接
compose 文件里声明的网络，手工加上去的那条**随之丢失**。

这个失效非常隐蔽：部署全程不报错，健康检查也返回 200——
因为它只打 `:8080/admin/`，不经过任何上游。等到有人调用 Grok 通道才炸：

```
502 upstream error: Post "http://progrok:18645/v1/chat/completions":
dial tcp: lookup progrok on 127.0.0.11:53: server misbehaving
```

所以必须写进 compose 文件，让每次重建都自动恢复：

```yaml
services:
  gateway:
    networks:
      - default      # compose 自建网络，靠它连 redis / postgres
      - grok-net     # 外部网络，靠它解析 progrok

networks:
  grok-net:
    external: true   # 由 mihomo / progrok 那套容器创建，本文件只接入
    name: grok-net
```

两条都不能少：只写 `grok-net` 会让 gateway 连不上数据库，
只写 `default`（或不写）就是上面那个 502。

### 验证顺序

分层验证，出问题能立刻定位在哪一层：

```bash
# 1. 代理层是否通
docker exec progrok curl -s -o /dev/null -w "%{http_code}" https://api.x.ai/v1/models

# 2. progrok 本身（绕过网关）
curl -s http://progrok:18645/v1/models

# 3. 走网关统一入口
curl -s http://localhost:8080/v1/chat/completions \
  -H "X-API-Key: <你的网关Key>" -H "Content-Type: application/json" \
  -d '{"model":"grok-4.6","messages":[{"role":"user","content":"hi"}]}'

# 4. 流式
curl -N http://localhost:8080/v1/chat/completions \
  -H "X-API-Key: <你的网关Key>" -H "Content-Type: application/json" \
  -d '{"model":"grok-4.6","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

第 1 步不通查 mihomo（订阅、节点、出站规则）；
第 1 通第 2 不通查 progrok（登录态、端口）；
第 2 通第 3 不通查网关（路由、渠道、模型白名单）。

### 该通道的两个配置注意点

- **关掉熔断**（`cb_enabled=false`）。订阅通道的失败往往来自代理层抖动，
  不是上游真的挂了，反复开路只会把本来能用的通道踢掉。
- **不要指望缓存**。`cache_ttl=0` 在网关里会被当作 30 秒，
  但聊天接口是 POST，缓存中间件只处理 GET，所以实际不生效。
  想省额度得靠上层自己控制。

## 5. 开机自启

生产容器用 `restart: unless-stopped`（或 compose 里等价配置），
服务器重启后 Docker 会自动拉起。

需要注意 **mihomo / progrok 这类手工 `docker run` 起来的容器**
也要带 `--restart unless-stopped`，否则重启后它们不会回来，
链路断在第二层而网关毫无察觉——表现是 Grok 通道全部超时，
其他通道正常。

## 6. 备份与回滚

- 数据库：`deploy/dump.sql` 是生产库快照的例子。定期 `pg_dump` 到异地
- 路由与 Key 定义在 Postgres，随库一起备份
- 代码回滚：`git log` 找到上一个可用提交，重新执行 `deploy/deploy.sh`
- **凭证不在库里**：`deploy/production.env` 与 OAuth token 卷需要单独备份，
  且不能进 git

## 7. 已知遗留

- mihomo / progrok 目前是手工 `docker run` 启动的，
  **尚未纳入 docker-compose 统一管理**。正式化后应与网关栈一起编排，
  并用 `depends_on` 表达启动顺序
- 订阅额度与 grok.com 官网聊天**共享同一个每周额度池**，
  API 调用会消耗网页端可用的量，需要留意消耗速度
- 代理层是单点。mihomo 挂了整个 Grok 通道不可用，
  暂未做备用出口
