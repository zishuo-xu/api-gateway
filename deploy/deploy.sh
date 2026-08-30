#!/usr/bin/env bash
# 一键部署最新代码到阿里云（只重建网关容器，数据库与数据不动）
#
# 用法:
#   ./deploy/deploy.sh
#
# 健康检查在服务器上执行（打服务器自己的 8080），因此不需要知道公网地址，
# 也不需要任何环境变量——既避免了把地址写进这个公开仓库，也确保它验证的
# 确实是刚部署的那台机器，而不是跑脚本的这台。
#
# 注意: 本仓库是公开的，不要把服务器 IP 或域名硬编码到这里。
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
# 必须在服务器上执行。放在本地执行时它会打到本机的 8080——如果本地恰好
# 跑着同一套 docker-compose，会返回 200，看起来部署验证通过，实际上验证的
# 是本地容器，服务器起没起来这一步一概不知。
ssh aliyun "curl -s -o /dev/null -w '健康: admin=%{http_code}\n' http://localhost:8080/admin/"
