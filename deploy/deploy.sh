#!/usr/bin/env bash
# 一键部署最新代码到阿里云（只重建网关容器，数据库与数据不动）
# 用法: ./deploy/deploy.sh
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
curl -s -o /dev/null -w "公网健康: admin=%{http_code}\n" http://YOUR_SERVER_IP:8080/admin/
