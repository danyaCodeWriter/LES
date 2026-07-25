#!/usr/bin/env bash
#
# Сценарий: cluster status=STANDBY через PUT /v1/active-region — regional API 403; global ок.
#
#   ./scripts/demo-step-standby-regional-denied.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

COORD_SRE_TOKEN="${COORD_SRE_TOKEN:-sre-stream-dev-token}"

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: status=STANDBY через PUT /v1/active-region (SRE), не через COORDINATION_DEPLOYMENT_ROLE."
  echo ""
  demo_require_active_healthy

  demo_step_http "1" "PUT status=STANDBY (SRE)" "PUT" \
    "${BASE_ACTIVE}/v1/active-region" \
    "{\"region\":\"r2\",\"status\":\"STANDBY\"}" \
    "Cluster status → stand_in; regional мутации дадут 403." \
    "" "${COORD_SRE_TOKEN}"

  sleep 1

  demo_step_http "2" "GET active-region" "GET" \
    "${BASE_ACTIVE}/v1/active-region" "" \
    "Ожидаем status=stand_in, region=r2."

  demo_step_http "3" "regional coordinator acquire → 403" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/acquire" \
    "{\"coordinatorId\":\"evil-on-standby\",\"instanceId\":\"pod-sb-1\",\"ttlSeconds\":300,\"metadata\":{\"region\":\"r2\"}}" \
    "HTTP 403 STANDBY_DEPLOYMENT."

  demo_step_http "4" "global coordinator acquire → ok" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/coordinator/acquire" \
    "{\"coordinatorId\":\"global-ok\",\"instanceId\":\"pod-sb-1\",\"ttlSeconds\":300,\"metadata\":{}}" \
    "Global разрешён на STANDBY."

  demo_step_http "5" "PUT status=ACTIVE (restore)" "PUT" \
    "${BASE_ACTIVE}/v1/active-region" \
    "{\"status\":\"ACTIVE\"}" \
    "Вернуть ACTIVE для дальнейших сценариев." \
    "" "${COORD_SRE_TOKEN}"

  echo "done"
}

main "$@"
