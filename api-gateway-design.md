# API 中转站 · 架构设计文档（讨论稿 v1）

> **这是项目最初的架构设想，部分内容已被实际实现超越或调整。**
> 当前实现与本文的偏差见 [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) 第七章。

> 项目定位：自托管 API 网关 / 转发枢纽。客户端统一打到中转站，中转站做鉴权、限流、缓存、请求合并、熔断、审计，再转发到真实公开 API。
> 目标：练手 + 简历 + 面试。技术栈：Go + 微服务 + 容器，Redis + PostgreSQL。

---

## 1. 为什么是它

- **自包含，零爬虫式数据源包袱**：高并发来自入站请求（自带压测客户端），上游只是被转发的目标，完全可控。
- **Redis + DB 都吃满**：Redis 做限流令牌桶 / 响应缓存 / 请求去重 / 熔断状态；DB 做 API 密钥、路由规则、调用审计、配额统计。
- **面试考点密集**：限流算法、分布式限流、缓存策略、熔断降级、网关中间件链、统一鉴权、请求追踪。
- **真实公开 API 作上游**：合法 API 调用（非爬），靠缓存 + 合并 + 限流守各自额度。

---

## 2. 技术栈

- 语言：Go 1.22+
- 网关：Go `net/http` + 自研中间件链（或 gin/echo）
- 存储：Redis 7（限流 / 缓存 / 熔断 / 去重）、PostgreSQL 15（密钥 / 路由 / 审计）
- 容器：docker-compose（MVP）→ Kubernetes（完整版，gateway HPA）
- 压测：hey / wrk / 自写 Go 客户端
- 监控（完整版）：Prometheus + Grafana

---

## 3. 系统架构

```
Client (压测 / 真实调用)
   │
   ▼
[ Gateway ×N ]   无状态，中间件链：
   │   日志 → 鉴权 → 限流 → 缓存 → 合并 → 转发 → 熔断 → 审计
   ├── Redis      (令牌桶 / 缓存 / 熔断状态 / 在途请求)
   └── PostgreSQL (api_keys / routes / request_logs / quotas)
   │
   ▼
真实公开 API   (Open-Meteo · REST Countries · CoinGecko · ...)
```

管理服务（Admin）：管理 API Key、路由规则、查看统计，可并入 gateway 的管理端口或独立服务。

---

## 4. 请求生命周期（中间件链）

1. 接入日志 / CORS
2. **鉴权**：校验 API Key（Redis 查活跃）或 JWT
3. **限流**：Redis 令牌桶（按 API Key + 按上游，双维度）
4. **缓存查询**：Redis 按 `method + path + params` 哈希命中则直接返回
5. **请求合并（Coalescing）**：相同在途请求共享结果（Redis 锁 / Pub-Sub）
6. **转发上游**：带超时、注入标识
7. **熔断**：按上游失败率（Redis 状态）决定是否短路
8. **写缓存**（按 TTL）
9. **审计落库**（`request_logs`）+ 配额扣减
10. 返回

---

## 5. 数据模型（PostgreSQL）

```sql
api_keys(
  id            BIGSERIAL PK,
  key_hash      VARCHAR(64) UNIQUE,   -- 存哈希，不存明文
  owner         VARCHAR(64),
  status        SMALLINT,             -- 1=启用 0=停用
  quota_limit   INT,
  quota_used    INT,
  created_at    TIMESTAMPTZ DEFAULT now()
);

routes(
  id            BIGSERIAL PK,
  name          VARCHAR(64),
  base_url      TEXT,                 -- 上游 base，如 https://api.open-meteo.com
  match_path    VARCHAR(128),         -- 匹配规则，如 /v1/weather/*
  auth_type     SMALLINT,             -- 0=无需 1=APIKey 2=JWT
  upstream_rps  INT,                  -- 上游保护额度
  cache_ttl     INT,                  -- 秒
  cb_enabled    BOOLEAN,
  status        SMALLINT
);

request_logs(
  id            BIGSERIAL PK,
  api_key_id    BIGINT,
  route_id      BIGINT,
  method        VARCHAR(8),
  path          TEXT,
  upstream      VARCHAR(64),
  status_code   INT,
  latency_ms    INT,
  cached        BOOLEAN,
  created_at    TIMESTAMPTZ DEFAULT now()
);

quotas(
  id            BIGSERIAL PK,
  api_key_id    BIGINT,
  period        VARCHAR(16),          -- 如 '2026-08-28'
  used          INT,
  updated_at    TIMESTAMPTZ DEFAULT now()
);
```

索引：`api_keys(key_hash)`、`request_logs(created_at)`、`quotas(api_key_id, period)`。

---

## 6. Redis Key 设计

```
rate:{api_key}:{sec}        → INCR + EXPIRE       限流计数（滑动窗口，按秒）
rate:{upstream}:{sec}       → INCR + EXPIRE       上游总调用保护
bucket:{api_key}            → 令牌桶（Lua：取令牌 / refill）
cache:{method}:{reqhash}    → 缓存响应（TTL = route.cache_ttl）
inflight:{reqhash}          → 在途请求锁（SET NX，用于合并）
cb:{upstream}               → 熔断状态（closed/open/half, fail_cnt, ts）
key:{key_hash}              → 活跃密钥元数据（TTL 续期）
```

---

## 7. 高并发 & 限流设计

- **令牌桶（Lua 原子）**：保证「取令牌 + 扣减」原子，避免多副本竞态。
- **双维度限流**：对调用方限流（保护平台）+ 对上游限流（守第三方 SLA）。
- **缓存 + 合并**：相同请求命中缓存或合并在途请求 → 上游实际调用数远低于入站，温柔对待公开 API。
- **熔断**：上游错误率超阈值 → 短暂开路、快速失败，半开探测恢复。
- **无状态网关**：多副本水平扩展，状态全在 Redis，天然支持 HPA。

---

## 8. 上游（真实公开 API）接入

选**免费、无需密钥或易申请**的友好源，开局示例：

| 上游 | 说明 | 密钥 |
|---|---|---|
| Open-Meteo | 天气，免费友好 | 无 |
| REST Countries | 国家信息，免费 | 无 |
| CoinGecko | 加密货币行情，免费 | 无 |
| GitHub API | 代码仓库（进阶） | token 提限额 |

策略：每个上游配置 `upstream_rps` + `cache_ttl`，中转站据此限流与缓存，**绝不突破上游额度**。

---

## 9. 压测方案

- 工具：hey / wrk / 自写 Go 客户端，打 gateway 端点。
- 观测：吞吐(QPS)、p99 延迟、限流拒绝率、缓存命中率、**上游实际调用数（应远小于入站）**。
- 卖点：证明「高并发在我的层，上游被温柔对待」。

---

## 10. 面试逐层话术

- **背景**：为什么做中转站（统一接入 / 保护上游 / 可观测）。
- **架构**：网关无状态 + Redis / PG 分工。
- **限流**：令牌桶 vs 漏桶，为什么用 Redis + Lua 原子。
- **分布式限流一致性**：多副本共享 Redis 计数。
- **缓存与合并**：如何既降延迟又保护上游。
- **熔断**：状态机与恢复。
- **踩坑 / 权衡**：缓存一致性、限流维度选择、合并的复杂度。

---

## 11. MVP vs 完整版

- **MVP**：单 gateway 多副本 + Redis + PG + 2~3 公开上游 + API Key 鉴权 + 限流 + 缓存 + 审计；docker-compose 跑通。
- **完整版**：熔断、请求合并、多维度限流、管理台 UI、Prometheus / Grafana、K8s HPA、灰度 / 鉴权增强。

---

## 12. 下一步

确认后可选：
1. 先出 **MVP 代码骨架**（gateway + redis + pg，docker-compose 一键起）；
2. 或先**细化某一模块**（限流 Lua 脚本 / 表结构 / 中间件链）。

你定。
