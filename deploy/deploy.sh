#!/usr/bin/env bash
# 一键部署最新代码到阿里云（只重建网关容器，数据库与数据不动）
#
# 用法:
#   GATEWAY_HOST=<服务器地址> ./deploy/deploy.sh
#
# 环境变量:
#   GATEWAY_HOST  健康检查的目标地址，默认 localhost
#
# 注意: 本仓库是公开的，不要把服务器 IP 或域名硬编码到这里。
#       地址只在运行时通过环境变量传入。
set -e
cd "$(dirname "$0")/.."

echo "[1/3] 同步代码..."
rsync -az --exclude bin --exclude .workbuddy --exclude pgdata --exclude .git \
      -e ssh ./ aliyun:/opt/api-gateway/

echo "[2/3] 服务器重建网关容器..."
ssh aliyun 'cd /opt/api-gateway && docker compose -f deploy/docker-compose.prod.yml --env-file deploy/production.env up -d --build gateway 2>&1 | tail -2'

echo "[3/3] 健康检查..."
sleep 3
ssh aliyun 'gw status'
curl -s -o /dev/null -w "公网健康: admin=%{http_code}\n" "http://${GATEWAY_HOST:-localhost}:8080/admin/"
