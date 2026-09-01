FROM golang:1.22-alpine AS build
WORKDIR /src
# 国内服务器构建时 proxy.golang.org 不可达；goproxy.cn 对海外同样可用，无害。
ENV GOPROXY=https://goproxy.cn,direct
# 静态链接。一旦 CGO 被启用，二进制就会动态链接 musl，换到 scratch 或
# distroless 基础镜像时会起不来，而且报错信息完全不指向这里。
ENV CGO_ENABLED=0
# go.sum 必须一起复制：没有它，容器里的 go mod download 会自己生成一份，
# 依赖的哈希校验形同虚设——供应链完整性全靠这个文件。
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -trimpath 剥掉编译机上的绝对路径，panic 栈里不会泄露宿主机的目录结构。
RUN go build -trimpath -o /out/gateway ./cmd/server

FROM alpine:3.19
# 说明一下为什么这里没有装 ca-certificates：alpine 基础镜像自带
# /etc/ssl/cert.pem（指向 certs/ca-certificates.crt），访问 https 上游
# 不需要额外安装。这是 Go + Alpine 最容易踩的坑，但坑不在 alpine 而在
# scratch / distroless —— 换成那两种基础镜像必须自己把证书拷进去，
# 否则所有 TLS 上游一律 x509: certificate signed by unknown authority。
WORKDIR /app
COPY --from=build /out/gateway .

# 网关不写磁盘、不绑定 1024 以下端口，没有任何需要 root 的理由。
# 65532 是 nobody 的通用 uid/gid，不必在镜像里额外建用户。
USER 65532:65532

EXPOSE 8080

# 用 /healthz 而不是 /readyz。liveness 只回答"进程还在不在"，依赖
# （Redis / Postgres）抖动时必须保持 healthy：重启容器既修不好依赖，
# 还会丢掉内存里的路由表。而 restart: unless-stopped 不会因为 unhealthy
# 重启容器，所以探活失败在这里只会造成误报。/readyz 留给外部负载均衡、
# 发版前的就绪判断或人工排查——它会在依赖不通时返回 503，并在响应体里
# 指名是哪个依赖。
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
	CMD wget -qO- --timeout=3 http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1

CMD ["./gateway"]
