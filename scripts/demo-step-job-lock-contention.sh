#!/usr/bin/env bash
#
# Сценарий: job уже удерживается — второй участник не получает lock (и отдельно: неверный TTL → 400).
# Предусловия: ACTIVE healthy (etcd + run-active). Пауза DEMO_STEP_PAUSE_SEC (по умолчанию 20).
#
#   ./scripts/demo-step-job-lock-contention.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: конкуренция за job + валидация ttlSeconds (ACTIVE)."
  echo ""
  demo_require_active_healthy

  local job="step-contention-job"
  local wa="step-contention-w-a"
  local wb="step-contention-w-b"
  local job_tok job_tok2 tok_a tok_b coord_tok

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 0a — Regional coordinator (нужен token для GET job/workers после смены API)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  coord_tok="$(curl -sS -X POST "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d '{"coordinatorId":"step-contention-leader","instanceId":"step-c-coord-pod","ttlSeconds":300,"metadata":{}}' \
    | jq -er .token)" || { echo "error: coordinator acquire failed" >&2; exit 1; }

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 0b — Два воркера (holder job = workerId, participantToken = register token)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  tok_a="$(curl -sS -X POST "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg w "$wa" '{workerId:$w,instanceId:"step-c-pa",ttlSeconds:300,metadata:{}}')" | jq -er .token)" \
    || { echo "error: register ${wa} failed" >&2; exit 1; }
  tok_b="$(curl -sS -X POST "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg w "$wb" '{workerId:$w,instanceId:"step-c-pb",ttlSeconds:300,metadata:{}}')" | jq -er .token)" \
    || { echo "error: register ${wb} failed" >&2; exit 1; }

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 1 — Первый участник захватывает job"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local jbody
  jbody="$(jq -nc --arg h "$wa" --arg pt "$tok_a" '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{who:"first"}}')"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire"
  echo "Тело (JSON):" && echo "${jbody}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire" "${jbody}"
  demo_print_last_response
  job_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: Ожидаем HTTP 200 и acquired=true — в etcd ключ лока с lease; сохраняем token для release."
  demo_pause

  demo_step_http "2" "Второй участник пытается тот же jobId" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire" \
    "$(jq -nc --arg h "$wb" --arg pt "$tok_b" '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{who:"second"}}')" \
    "Ожидаем HTTP 409 (JOB_EXISTS): lock уже у ${wa}; в теле — error/message и часто currentHolder." \
    "409"

  demo_step_http "3" "Некорректный TTL (меньше 10 сек) — отклонение валидации" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}-other:acquire" \
    "$(jq -nc --arg h "$wb" --arg pt "$tok_b" '{holder:$h,participantToken:$pt,ttlSeconds:5,metadata:{}}')" \
    "Ожидаем HTTP 400 от Bean Validation: ttlSeconds должен быть 10..300 (см. OpenAPI / LeaseTtlBounds)."

  demo_step_http "4" "Освобождение лока первым участником (по token)" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:release" \
    "$(jq -nc --arg t "$job_tok" '{token:$t}')" \
    "После released=true второй участник может захватить тот же jobId."

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 5 — Второй участник (${wb}) захватывает тот же jobId"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local j2
  j2="$(jq -nc --arg h "$wb" --arg pt "$tok_b" '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{who:"second-after-release"}}')"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire"
  echo "Тело (JSON):" && echo "${j2}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire" "${j2}"
  demo_print_last_response
  job_tok2="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  demo_pause

  demo_step_http "6" "Job GET (нужен Bearer = токен coordinator)" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}" "" \
    "Ответ без секретов renew в JSON; доступ только с токеном лидера." \
    "" \
    "${coord_tok}"

  demo_step_http "7" "Release вторым участником" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:release" \
    "$(jq -nc --arg t "$job_tok2" '{token:$t}')" \
    "Снятие лока по token из шага 5."

  echo "Очистка (тихо): deregister workers …"
  curl -sS -o /dev/null --max-time 30 -X POST "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${wa}/deregister" \
    -H 'content-type: application/json' -d "$(jq -nc --arg t "$tok_a" '{token:$t}')" || true
  curl -sS -o /dev/null --max-time 30 -X POST "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${wb}/deregister" \
    -H 'content-type: application/json' -d "$(jq -nc --arg t "$tok_b" '{token:$t}')" || true
  demo_step_etcd_delete_tenant_prefix "${TENANT}"

  echo "Готово: сценарий конкуренции завершён. Ключ ${job} снят (если release прошёл)."
}

main "$@"
