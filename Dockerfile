FROM golang:1.22-alpine AS build
WORKDIR /src
# 国内服务器构建时 proxy.golang.org 不可达；goproxy.cn 对海外同样可用，无害。
ENV GOPROXY=https://goproxy.cn,direct
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o /out/gateway ./cmd/server

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/gateway .
EXPOSE 8080
CMD ["./gateway"]
