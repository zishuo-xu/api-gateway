#!/usr/bin/env bash
# 一键部署最新代码到阿里云（只重建网关容器，数据库与数据不动）
#
# 用法:
#   ./deploy/deploy.sh
#
# 健康检查全部在服务器上执行（打服务器自己的 8080），因此不需要知道公网地址，
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
# 这里刻意不用 `| tail -n` 之类的管道：管道的退出码取的是末端命令的，
# 永远是 0，set -e 就形同虚设——构建失败时脚本照样往下走并报告成功。
# 构建日志长一点没关系，真出问题时正好要看的就是它。
ssh aliyun 'cd /opt/api-gateway && docker compose -f deploy/docker-compose.prod.yml --env-file deploy/production.env up -d --build gateway'

echo "[3/3] 健康检查..."
sleep 3
ssh aliyun 'gw status'

# --- 第 1 步：进程活着吗 ---
# -f 让 HTTP 错误码也变成非零退出，否则 curl 抓不到 500。
# 必须在服务器上执行：放本地会打到本机的 8080——如果本地恰好跑着同一套
# compose，会返回 200，看起来验证通过，其实验证的是本地容器。
ssh aliyun "curl -sf -o /dev/null -w '进程: admin=%{http_code}\n' http://localhost:8080/admin/"

# --- 第 2 步：通道真的能用吗 ---
# 这一条是 2026-08-31 事故补上的。那次部署后 Grok 通道已经断了
# （容器重建丢了 grok-net 网络），但 /admin/ 照样返回 200，
# 脚本报告"部署成功"，实际一调用就 502。进程活着 ≠ 服务可用。
#
# 失败会重试一次。机场节点会间歇性返回 503，单次失败就判部署失败会误报
# （实测遇到过：同一分钟内前一次 503、后一次正常）。但也不能因此不验——
# 那正是事故当天的状态。
#
# 每次验证消耗极少量额度（一次 8 token 的补全），但远比"部署完以为没事、
# 过几小时才发现通道是坏的"划算。
probe_channel() {
    ssh aliyun 'set -a; . /opt/api-gateway/deploy/production.env; set +a
        curl -s -m 60 -X POST http://localhost:8080/v1/chat/completions \
            -H "Authorization: Bearer $DEMO_API_KEY" \
            -H "Content-Type: application/json" \
            -d "{\"model\":\"grok-4.6\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"max_tokens\":8}" 2>&1'
}

echo "验证真实通道（发一条最小请求）..."
if probe_channel | grep -q '"choices"'; then
    echo "通道: grok-4.6 正常"
else
    echo "首次探测未通过，3 秒后重试（区分瞬时抖动与真故障）..."
    sleep 3
    resp=$(probe_channel)
    if printf '%s' "$resp" | grep -q '"choices"'; then
        echo "通道: grok-4.6 正常（重试通过，前一次是瞬时抖动）"
    else
        echo ""
        echo "通道: grok-4.6 连续两次不通 —— 上游原文: $resp"
        echo "部署动作已完成，但通道没通过，这次部署不能算成功。"
        echo "常见原因见 docs/deployment.md 第 4 节。"
        exit 1
    fi
fi
echo "部署完成，通道已验证"
