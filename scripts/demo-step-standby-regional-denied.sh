#!/usr/bin/env bash
#
# Сценарий: клиент ходит на STANDBY (stand-in в logical region **r2**) — regional API coordinator/workers/jobs
# отклоняются с 403; global coordinator — разрешён. В metadata тел запросов — `"region":"r2"` (как у stand-in).
# Пауза DEMO_STEP_PAUSE_SEC (по умолчанию 20).
#
#   ./scripts/demo-step-standby-regional-denied.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: STANDBY-инстанс (COORDINATION_DEPLOYMENT_ROLE=STANDBY на :8081), stand-in = logical region r2."
  echo "Вы зарегистрировали процесс как пассивный ЦОД — не слать сюда regional acquire/register/job:"
  echo "используйте BASE_ACTIVE для active-region мутаций."
  echo ""
  demo_require_active_healthy
  demo_require_standby_healthy

  demo_step_http "1" "STANDBY: GET active-region (разрешено)" "GET" \
    "${BASE_STANDBY}/v1/tenants/${TENANT}/active-region" "" \
    "Чтение конфигурации active region на пассивном инстансе разрешено (диагностика)."

  demo_step_http "2" "STANDBY: regional coordinator acquire → 403" "POST" \
    "${BASE_STANDBY}/v1/tenants/${TENANT}/coordinator/acquire" \
    "{\"coordinatorId\":\"evil-on-standby\",\"instanceId\":\"pod-sb-1\",\"ttlSeconds\":300,\"metadata\":{\"region\":\"r2\"}}" \
    "HTTP 403 и JSON с error=STANDBY_DEPLOYMENT: этот инстанс в роли STANDBY — вы вызываете active-region coordinator API, но процесс в stand-in; идите на ACTIVE (BASE_ACTIVE)."

  demo_step_http "3" "STANDBY: regional worker register → 403" "POST" \
    "${BASE_STANDBY}/v1/tenants/${TENANT}/workers/register" \
    "{\"workerId\":\"worker-on-standby\",\"instanceId\":\"pod-sb-2\",\"ttlSeconds\":300,\"metadata\":{\"region\":\"r2\"}}" \
    "То же для workers/register: regional запись только на ACTIVE."

  demo_step_http "4" "STANDBY: regional job acquire → 403" "POST" \
    "${BASE_STANDBY}/v1/tenants/${TENANT}/jobs/step-standby-deny-job:acquire" \
    "{\"holder\":\"pod-sb-3\",\"participantToken\":\"dummy\",\"ttlSeconds\":300,\"metadata\":{\"region\":\"r2\"}}" \
    "Job в active region на STANDBY не создаётся — 403 STANDBY_DEPLOYMENT."

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 5 — STANDBY: global coordinator acquire (разрешено)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local gsb_tok gsb_json
  gsb_json='{"coordinatorId":"from-standby-global","instanceId":"pod-sb-4","ttlSeconds":300,"metadata":{"note":"allowed-on-standby","region":"r2"}}'
  echo "Запрос: POST ${BASE_STANDBY}/v1/tenants/${TENANT}/global/coordinator/acquire"
  echo "Тело (JSON):" && echo "${gsb_json}" | jq .
  echo "—"
  demo_exec_post "${BASE_STANDBY}/v1/tenants/${TENANT}/global/coordinator/acquire" "${gsb_json}"
  demo_print_last_response
  gsb_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: глобальный контур не привязан к ACTIVE; STANDBY может acquire. Сохраняем token для GET."
  demo_pause

  demo_step_http "6" "STANDBY: global coordinator GET (Bearer global token)" "GET" \
    "${BASE_STANDBY}/v1/tenants/${TENANT}/global/coordinator" "" \
    "Проверка чтения глобального лидера с пассивного инстанса (тот же Bearer, что при global acquire)." \
    "" \
    "${gsb_tok}"

  demo_step_etcd_delete_tenant_prefix "${TENANT}"

  echo "Готово: сценарий STANDBY завершён. Префикс tenant в etcd очищен при наличии etcdctl (иначе см. сообщение выше)."
}

main "$@"
