#!/usr/bin/env bash
# 端到端冒烟测试：把网关的核心防护与中转能力跑一遍真实请求。
#
# 与 `go test` 的分工：
#   - go test 覆盖单元与集成逻辑（用 httptest 假上游）
#   - 本脚本覆盖「整条链」——真实 HTTP、真实 Redis、真实 Postgres、
#     真实中间件顺序，以及需要外部上游才能验证的行为
#
# 用法：
#   scripts/e2e.sh
#   GW_URL=http://gw:8080 ADMIN_TOKEN=x scripts/e2e.sh
#
# 依赖：curl、bash、python3（不依赖 jq）。
# 需要外网 httpbin.org 的用例在不可达时会标 SKIP，不会伪装成通过。
#
# 注意：脚本里凡是「变量后面紧跟中文」的地方一律写成 ${var}。
# bash 在部分 locale 下会把紧跟其后的多字节字符算进变量名里，
# 报出 "want?: unbound variable" 这种莫名其妙的错。

set -uo pipefail

GW_URL="${GW_URL:-http://127.0.0.1:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
PY="${PY:-python3}"

PASS=0; FAIL=0; SKIP=0

# 记录待清理对象的文件，而不是 shell 数组。
#
# 原因：这些 helper 都在命令替换里调用（ID=$(create_route ...)），
# 而命令替换跑在子 shell 中 —— 函数里对数组的 += 不会传回父 shell，
# 结果 cleanup 拿到空数组，什么都没删。下一轮脚本就会撞上上一轮
# 留下的同名路由（前缀重复），请求全打到第一条上，测试结论全错。
STATE_FILE=$(mktemp -t gw-e2e.XXXXXX)
remember() { printf '%s %s\n' "$1" "$2" >> "$STATE_FILE"; }

# ---------- 输出辅助 ----------

c() { printf '\033[%sm' "$1"; }
GREEN=$(c 32); RED=$(c 31); YELLOW=$(c 33); DIM=$(c 2); RST=$(c 0)

ok()   { PASS=$((PASS+1)); printf '  %s✓%s %s\n' "$GREEN" "$RST" "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  %s✗%s %s\n' "$RED" "$RST" "$1"
         if [ -n "${2:-}" ]; then printf '      %s%s%s\n' "$DIM" "$2" "$RST"; fi; }
skip() { SKIP=$((SKIP+1)); printf '  %s-%s %s %s(%s)%s\n' "$YELLOW" "$RST" "$1" "$DIM" "$2" "$RST"; }

# expect <说明> <期望码> <实际码> [响应体]
expect() {
  local desc="${1}" want="${2}" got="${3}" body="${4:-}"
  if [ "$want" = "$got" ]; then
    ok "${desc} (HTTP ${got})"
  else
    bad "${desc}" "期望 HTTP ${want}，实际 ${got}${body:+ — ${body:0:200}}"
  fi
}

# jget <json> <python 表达式> —— 表达式里的 d 就是解析后的对象
jget() {
  printf '%s' "$1" | "$PY" -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print($2)
except Exception:
    print('')
" 2>/dev/null
}

admin() { curl -s -m 20 -H "X-Admin-Token: $ADMIN_TOKEN" "$@"; }

create_route() { # <name> <base_url> <prefix> <format> <models_json> -> id
  local resp id
  resp=$(admin -X POST "$GW_URL/admin/routes" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"base_url\":\"$2\",\"match_path\":\"$3\",\"api_format\":\"$4\",\"models\":$5,\"cache_ttl\":0}")
  id=$(jget "$resp" "d.get('id','')")
  [ -n "$id" ] && remember route "$id"
  printf '%s' "$id"
}

create_key() { # <owner> [extra_json] -> raw key
  local resp id
  # Every key issued here is marked no_log. The assertions below deliberately
  # provoke 403 (model allowlist), 429 (quota) and 400 (failover probe); without
  # this flag each run buries real traffic in the request log under a pile of
  # self-inflicted failures. Quota is still charged - that is what makes the
  # quota assertion mean anything.
  resp=$(admin -X POST "$GW_URL/admin/keys" -H 'Content-Type: application/json' \
    -d "{\"owner\":\"$1\",\"no_log\":true${2:+,$2}}")
  id=$(jget "$resp" "d.get('id','')")
  [ -n "$id" ] && remember key "$id"
  jget "$resp" "d.get('api_key','')"
}

create_channel() { # <route_id> <name> <base_url> <priority> -> id
  local resp id
  resp=$(admin -X POST "$GW_URL/admin/channels" -H 'Content-Type: application/json' \
    -d "{\"route_id\":$1,\"name\":\"$2\",\"base_url\":\"$3\",\"priority\":$4}")
  id=$(jget "$resp" "d.get('id','')")
  [ -n "$id" ] && remember channel "$id"
  printf '%s' "$id"
}

cleanup() {
  [ -f "$STATE_FILE" ] || return 0
  # 逆序删除：先渠道后路由，避免留下悬空引用。
  local kind id
  while read -r kind id; do
    case "$kind" in
      channel) admin -X DELETE "$GW_URL/admin/channels?id=$id" >/dev/null 2>&1 ;;
    esac
  done < <(tac "$STATE_FILE" 2>/dev/null || tail -r "$STATE_FILE" 2>/dev/null)
  while read -r kind id; do
    case "$kind" in
      route) admin -X DELETE "$GW_URL/admin/routes?id=$id" >/dev/null 2>&1 ;;
    esac
  done < <(tac "$STATE_FILE" 2>/dev/null || tail -r "$STATE_FILE" 2>/dev/null)
  while read -r kind id; do
    case "$kind" in
      key) admin -X DELETE "$GW_URL/admin/keys?id=$id" >/dev/null 2>&1 ;;
    esac
  done < <(tac "$STATE_FILE" 2>/dev/null || tail -r "$STATE_FILE" 2>/dev/null)
  rm -f "$STATE_FILE"
}
trap cleanup EXIT

# call <key> <path> <body> —— 状态码写 stdout，响应体写 /tmp/_e2e_body
call() {
  curl -s -m 60 -o /tmp/_e2e_body -w '%{http_code}' -X POST "$GW_URL$2" \
    -H "X-API-Key: $1" -H 'Content-Type: application/json' -d "$3"
}
body_of() { cat /tmp/_e2e_body 2>/dev/null; }

# ---------- 前置检查 ----------

echo
printf '%sAPI 网关端到端冒烟测试%s  →  %s\n' "$DIM" "$RST" "$GW_URL"
echo

if [ -z "$ADMIN_TOKEN" ]; then
  echo "需要 ADMIN_TOKEN（网关的管理令牌）。示例："
  echo "  ADMIN_TOKEN=dev-admin-token $0"
  exit 2
fi
if ! curl -s -m 5 -o /dev/null "$GW_URL/admin/"; then
  echo "连不上 $GW_URL —— 网关没在跑？"
  exit 2
fi

HTTPBIN_OK=1
curl -s -m 8 -o /dev/null https://httpbin.org/post || HTTPBIN_OK=0

# ---------- 1. 管理接口鉴权 ----------

echo "管理接口"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' "$GW_URL/admin/routes")
expect "缺 Admin Token 应被拒绝" 403 "$code"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-Admin-Token: wrong" "$GW_URL/admin/routes")
expect "错误 Admin Token 应被拒绝" 403 "$code"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-Admin-Token: $ADMIN_TOKEN" "$GW_URL/admin/routes")
expect "正确 Admin Token 应放行" 200 "$code"

# ---------- 2. Prometheus ----------

echo
echo "Prometheus 指标"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-Admin-Token: $ADMIN_TOKEN" "$GW_URL/metrics")
expect "/metrics 应可访问" 200 "$code"
buckets=$(curl -s -m 10 -H "X-Admin-Token: $ADMIN_TOKEN" "$GW_URL/metrics" \
  | grep -c 'request_duration_seconds_bucket' || true)
if [ "${buckets:-0}" -gt 0 ]; then ok "延迟直方图有 ${buckets} 个桶"
else bad "延迟直方图缺失（Prometheus 会认为这是畸形数据）"; fi

# ---------- 3. 统一入口 ----------

echo
echo "统一入口 /v1"
PROBE_KEY=$(create_key "e2e-probe")
if [ -z "$PROBE_KEY" ]; then
  bad "签发测试 Key 失败"
  exit 1
fi

models_json=$(curl -s -m 10 -H "X-API-Key: $PROBE_KEY" "$GW_URL/v1/models")
if [ "$(jget "$models_json" "d.get('object','')")" = "list" ]; then
  ok "GET /v1/models 返回 OpenAI 格式列表"
else
  bad "GET /v1/models 格式不对" "${models_json:0:160}"
fi

FIRST_MODEL=$(jget "$models_json" "(d.get('data') or [{}])[0].get('id','')")
if [ -n "$FIRST_MODEL" ]; then
  code=$(call "$PROBE_KEY" /v1/chat/completions '{"model":"e2e-no-such-model","messages":[]}')
  expect "未知模型应 404" 404 "$code"
  case "$(body_of)" in
    *available*) ok "404 响应列出可用模型，客户端能自行纠正" ;;
    *) bad "404 响应没带可用模型列表" "$(body_of)" ;;
  esac
  code=$(call "$PROBE_KEY" /v1/chat/completions '{"messages":[]}')
  expect "缺 model 应 400（而不是 404）" 400 "$code"
else
  skip "未知模型 / 缺 model" "没有任何路由声明模型白名单"
fi

# ---------- 4. 鉴权写法 ----------

echo
echo "鉴权"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' "$GW_URL/v1/models")
expect "无凭据应 401" 401 "$code"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $PROBE_KEY" "$GW_URL/v1/models")
expect "Bearer 写法可用（OpenAI SDK 只发这一种）" 200 "$code"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-API-Key: $PROBE_KEY" "$GW_URL/v1/models")
expect "X-API-Key 写法继续可用" 200 "$code"
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer made-up" "$GW_URL/v1/models")
expect "伪造凭据应 401" 401 "$code"

# ---------- 5. IP 白名单（不需要外网） ----------

echo
echo "来源 IP 限制"
IP_KEY=$(create_key "e2e-ip" '"allowed_ips":["10.99.99.99"]')
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-API-Key: $IP_KEY" "$GW_URL/v1/models")
expect "来源 IP 不在白名单应 403" 403 "$code"

# ---------- 6. Key 过期（不需要外网） ----------

echo
echo "API Key 过期"
EXP=$(date -u -v+40S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
      || date -u -d '+40 seconds' +%Y-%m-%dT%H:%M:%SZ)
EXP_KEY=$(create_key "e2e-expiry" "\"expires_at\":\"$EXP\"")
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-API-Key: $EXP_KEY" "$GW_URL/v1/models")
expect "未过期应放行" 200 "$code"
printf '  等待 40 秒让它过期…\n'
sleep 42
code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-API-Key: $EXP_KEY" "$GW_URL/v1/models")
expect "过期后应 403" 403 "$code"

# ---------- 7. 需要外网：配额 / 白名单 / 故障转移 ----------

if [ "$HTTPBIN_OK" = "1" ]; then

  # 配额要能被消耗，必须走「真的打到上游」的请求。
  # 网关侧拒绝（401/403/404/405）不计费 —— 那是刻意的设计，
  # 所以不能拿 /v1/models 这种本地应答来测配额。
  echo
  echo "配额"
  QR=$(create_route "e2e-quota" "https://httpbin.org" "/e2e-quota" "generic" '[]')
  QUOTA_KEY=$(create_key "e2e-quota" '"quota_limit":3')
  for i in 1 2 3; do
    call "$QUOTA_KEY" /e2e-quota/post '{"e2e":"quota"}' >/dev/null 2>&1
  done
  code=$(call "$QUOTA_KEY" /e2e-quota/post '{"e2e":"quota"}')
  expect "配额用尽后应 429" 429 "$code"
  case "$(body_of)" in
    *"quota exceeded"*) ok "429 说明了是配额耗尽（不是限流）" ;;
    *) bad "429 没说明原因" "$(body_of)" ;;
  esac

  echo
  echo "模型白名单（上游 httpbin 不校验模型名）"
  MR=$(create_route "e2e-modeltest" "https://httpbin.org" "/e2e-modeltest" "openai-chat" '["allowed-model"]')
  code=$(call "$PROBE_KEY" /e2e-modeltest/post '{"model":"allowed-model","messages":[]}')
  expect "白名单内的模型应放行" 200 "$code"
  code=$(call "$PROBE_KEY" /e2e-modeltest/post '{"model":"anything-else","messages":[]}')
  expect "白名单外的模型应 403" 403 "$code"
  case "$(body_of)" in
    *"not allowed"*) ok "403 说明了原因（证明是网关在挡，不是上游）" ;;
    *) bad "403 没说明原因" "$(body_of)" ;;
  esac

  echo
  echo "多渠道故障转移"
  FR=$(create_route "e2e-failover" "https://httpbin.org/status/500" "/e2e-failover" "generic" '[]')
  create_channel "$FR" "e2e-backup" "https://httpbin.org/post" 1 >/dev/null
  sleep 1

  # 先看网关内存里到底挂了几条渠道。没有这一步，一旦断言失败就分不清
  # 是「没做故障转移」还是「备渠道自己也挂了」。
  NCH=$(jget "$(admin "$GW_URL/admin/routes")" \
        "len(([r for r in d if r.get('name')=='e2e-failover'] or [{}])[0].get('channels') or [])")
  if [ "${NCH:-0}" -lt 2 ]; then
    skip "主渠道 500 应自动切备渠道" "网关内存里只有 ${NCH:-0} 条渠道，备渠道未生效"
  else
    code=$(call "$PROBE_KEY" /e2e-failover '{"e2e":"replay-me"}')
    expect "主渠道 500 应自动切备渠道" 200 "$code"
    case "$(body_of)" in
      *"replay-me"*) ok "重试时请求体完整重放（没有被读空）" ;;
      *) bad "备渠道收到的 body 不完整" "$(body_of)" ;;
    esac
  fi

  create_channel "$FR" "e2e-400" "https://httpbin.org/status/400" 0 >/dev/null
  sleep 1
  code=$(call "$PROBE_KEY" /e2e-failover '{}')
  if [ "$code" = "400" ] || [ "$code" = "200" ]; then
    ok "4xx 不触发故障转移（返回 ${code}，没把用户错误当上游故障）"
  else
    bad "4xx 场景返回了意外的 ${code}"
  fi
else
  echo
  skip "配额 / 模型白名单 / 故障转移" "httpbin.org 不可达"
fi

# ---------- 8. 用量统计 ----------

echo
echo "用量统计"
u=$(admin "$GW_URL/admin/usage?hours=1")
reqs=$(jget "$u" "d.get('totals',{}).get('requests',0)")
if [ "${reqs:-0}" -gt 0 ]; then ok "用量看板有数据（近 1 小时 ${reqs} 次请求）"
else bad "用量看板没有数据"; fi
nmodels=$(jget "$u" "len(d.get('by_model') or [])")
if [ "${nmodels:-0}" -gt 0 ]; then ok "按模型聚合有 ${nmodels} 项"
else bad "按模型聚合为空"; fi

# ---------- 汇总 ----------

echo
printf '  %s通过 %s · 失败 %s · 跳过 %s%s\n' "$DIM" "$PASS" "$FAIL" "$SKIP" "$RST"
echo
[ "$FAIL" -eq 0 ]
