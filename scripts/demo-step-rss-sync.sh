#!/usr/bin/env bash
#
# Демо: active-region берётся из RSS mock (SSE) и периодически меняется
# active → switch → stand_in → …
#
# Подготовка:
#   ./scripts/demo-two-dc.sh etcd-start
#   ./scripts/demo-two-dc.sh rss-start
#   ./scripts/demo-two-dc.sh run-active-rss   # или start-active-rss-bg
#   ./scripts/demo-step-rss-sync.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

: "${RSS_BASE:=http://127.0.0.1:8090}"
: "${RSS_RECOVERY_GROUP:=demo-rg}"
: "${RSS_HOME_REGION:=r1}"
: "${RSS_POLL_COUNT:=6}"
: "${RSS_POLL_SEC:=3}"

demo_require_rss_healthy() {
  local body
  if ! body="$(curl -sfS --max-time 3 "${RSS_BASE}/health" 2>/dev/null)"; then
    echo "error: RSS mock не отвечает ${RSS_BASE}/health" >&2
    echo "  Поднимите: ./scripts/demo-two-dc.sh rss-start  (или docker compose up -d rss-mock)" >&2
    exit 1
  fi
  echo "RSS mock health: $body"
}

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: master data региона из RSS mock → etcd → GET /v1/active-region"
  echo "  RSS: ${RSS_BASE}  RG=${RSS_RECOVERY_GROUP}  home=${RSS_HOME_REGION}"
  echo "  Цикл mock: active → switch → stand_in (см. CYCLE_INTERVAL_SEC в compose)"
  echo ""
  demo_require_rss_healthy
  demo_require_active_healthy

  demo_step_http "1" "RSS debug snapshot" "GET" \
    "${RSS_BASE}/v1/debug/state?region=${RSS_HOME_REGION}" "" \
    "Текущий state у mock (до маппинга в LES: active/stand_in/switch)."

  demo_step_http "2" "LES GET /v1/active-region (после bootstrap из RSS)" "GET" \
    "${BASE_ACTIVE}/v1/active-region" "" \
    "Должен совпадать с RSS: region + status (active→ACTIVE, stand_in→STANDBY, switch→SWITCH)."

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 3 — Поллинг LES пока RSS крутит статусы (${RSS_POLL_COUNT}× каждые ${RSS_POLL_SEC}s)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  echo "Ожидайте смену status / region по мере CYCLE_INTERVAL_SEC у mock."
  echo ""
  local i body status region
  for i in $(seq 1 "${RSS_POLL_COUNT}"); do
    body="$(curl -sS --max-time 5 "${BASE_ACTIVE}/v1/active-region")"
    status="$(echo "$body" | jq -r '.status // .deploymentStatus // empty')"
    region="$(echo "$body" | jq -r '.region // .regionId // empty')"
    printf "  [%d/%d] region=%-4s status=%-10s  %s\n" "$i" "$RSS_POLL_COUNT" "$region" "$status" \
      "$(echo "$body" | jq -c '.')"
    if [[ "$i" -lt "$RSS_POLL_COUNT" ]]; then
      sleep "${RSS_POLL_SEC}"
    fi
  done
  echo ""
  echo "Пояснение: RssClient держит SSE к mock; каждый state пишется в etcd;"
  echo "  ActiveRegionMonitor читает etcd → GET /v1/active-region отражает RSS."
  demo_pause

  demo_step_http "4" "RSS debug snapshot (ещё раз)" "GET" \
    "${RSS_BASE}/v1/debug/state?region=${RSS_HOME_REGION}" "" \
    "Сверьте с последним LES snapshot — должны быть согласованы."

  echo "done (опционально: curl -N '${RSS_BASE}/v1/recovery-groups/${RSS_RECOVERY_GROUP}/active-region?region=${RSS_HOME_REGION}')"
}

main "$@"
