# API Gateway

自托管的 OpenAI 兼容 API 网关。客户端只认一个 `base_url` 和一把网关发的 Key，
网关在背后负责鉴权、配额、限流、缓存、多渠道故障转移、用量计费与审计，
再转发到真实上游。

技术栈：Go 标准库 `net/http` + Redis + PostgreSQL。没有 Web 框架，没有 ORM。

## 它能做什么

- **统一入口**：客户端配一次 `base_url`，换供应商只需改 model 名字
- **多渠道故障转移**：一条路由挂多个上游，按优先级分档、档内按权重分流，
  5xx / 429 / 408 自动切下一条
- **三层限流**：上游总闸、单 Key、客户端 IP，一次 Lua 调用原子扣减
- **用量计费**：流式响应边转发边解析 usage，记录 token、TTFT、tokens/s
- **Key 全生命周期**：配额、过期、来源 IP 白名单、模型白名单
- **可观测**：内置监控台、Prometheus 导出、用量看板、请求日志、调试台
- **热加载**：改路由不重启，多副本自动同步

## 快速开始

```bash
cp .env.example .env
docker-compose up --build
```

```bash
curl -H "X-API-Key: test-key-123" \
  "http://localhost:8080/v1/weather?latitude=39.9&longitude=116.4"
```

首次请求走上游并写入缓存，短时间内重复请求命中 `X-Cache: HIT`。

## 文档

| 文档 | 面向 | 内容 |
|---|---|---|
| [docs/usage.md](docs/usage.md) | 使用者 | 环境变量全表、路由与 Key 配置、两种寻址方式、监控台、冒烟测试 |
| [docs/architecture.md](docs/architecture.md) | 开发者 | 进程结构、请求全链路、状态分布、多实例一致性、Redis key 总表 |
| [docs/design.md](docs/design.md) | 开发者 | 限流、缓存、熔断、故障转移、计费、配额、审计等子系统的设计权衡 |
| [docs/deployment.md](docs/deployment.md) | 运维 | 生产拓扑、需要出境代理的通道、开机自启、备份回滚 |
| [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) | 复盘 / 简历 | 规模数据、技术要点、与初版设计的偏差、已知边界 |
| [api-gateway-design.md](api-gateway-design.md) | — | v1 初版设想，**历史文档**，内容已被实现超越 |

想跑起来看 **usage**，想改代码看 **architecture** 和 **design**，
想部署看 **deployment**，想了解这个项目的取舍看 **PROJECT_SUMMARY**。

## 目录结构

```
api-gateway/
├── cmd/server/main.go      # 装配入口
├── internal/
│   ├── config/             # 环境变量与默认值
│   ├── gateway/            # 中间件链、渠道选择、故障转移、流式转发、用量嗅探
│   │   ├── gateway.go
│   │   ├── admin.go        # 管理端接口
│   │   ├── admin_ui.html   # 监控台前端（零外部依赖）
│   │   └── stats.go        # 延迟直方图 + Prometheus 输出
│   └── store/              # redis(Lua 令牌桶) / db / 配额 / 审计 / 指标
├── init.sql                # 全新安装的完整表结构 + demo route
├── migrate.sql             # 增量迁移（启动时自动执行，幂等）
└── docs/                   # 使用、架构、设计文档
```

## 已知边界

部署前需要知道的几条（完整版见 [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md)）：

- 默认缓存是**全局共享**的，会返回用户私有数据的路由必须改成按 Key 隔离
- Prometheus 的延迟直方图是**进程内指标**，多实例需在 Prometheus 侧聚合
- `cache_ttl` 传 0 会被当作 30 秒，不是关闭缓存
- 审计日志异步落库，`SIGKILL` 最多丢缓冲区里的一批（默认 1024 条）
- `IP_RPS` 开启前确认流量不是全走同一个 NAT，否则所有人会被算成一个来源

## 测试

```bash
go test ./...                              # 88 个用例，约 5 秒
ADMIN_TOKEN=dev-admin-token scripts/e2e.sh # 端到端 26 项断言
```
