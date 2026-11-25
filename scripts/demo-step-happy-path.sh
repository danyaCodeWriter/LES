#!/usr/bin/env bash
#
# Пошаговый happy path по REST ACTIVE (пауза DEMO_STEP_PAUSE_SEC между шагами, по умолчанию 20).
# Предусловия: etcd, run-active (см. scripts/demo-two-dc.sh).
#
#   export DEMO_STEP_PAUSE_SEC=15   # опционально
#   ./scripts/demo-step-happy-path.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: happy path (regional + global leases). Рекомендуется чистый etcd для tenant"
  echo "или отдельный TENANT=..., иначе шаги acquire/register могут вернуть HTTP 409, если lease ещё жив."
  echo ""
  demo_require_active_healthy

  local cid="step-happy-coord"
  local iid="step-happy-pod-1"
  local w1="step-happy-w-a"
  local w2="step-happy-w-b"
  local job="step-happy-job"
  local gcoord="step-happy-global"
  local ginst="step-happy-global-pod"

  demo_step_http "1" "Health LES" "GET" \
    "${BASE_ACTIVE}/actuator/health" "" \
    "LES готов принимать regional/global lease API (сегмент etcd = coordination.region)."

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 2 — Coordinator acquire (regional short-path / coordination.region)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local coord_json coord_tok
  coord_json="{\"coordinatorId\":\"${cid}\",\"instanceId\":\"${iid}\",\"ttlSeconds\":300,\"metadata\":{\"logicalRegion\":\"${COORD_DEMO_REGION_INITIAL}\"}}"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/acquire"
  echo "Тело (JSON):" && echo "${coord_json}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/acquire" "${coord_json}"
  demo_print_last_response
  coord_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: При acquired=true в ответе есть token — его сохраняем и передаём в renew (как у воркера)."
  demo_pause

  demo_step_http "3" "Coordinator renew (по token)" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/renew" \
    "$(jq -nc --arg t "$coord_tok" '{token:$t}')" \
    "Продление lease лидера при совпадении token с записью в etcd (instanceId в пути не нужен)."

  demo_step_http "4" "Coordinator GET" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator" "" \
    "Чтение записи лидера и метаданных (fenceEpoch и т.д.); заголовок Authorization: Bearer = token acquire)." \
    "" \
    "${coord_tok}"

  # --- workers + tokens ---
  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  printf "║ Шаг 5 — Регистрация воркера %s\n" "${w1}"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register"
  local json1
  json1="$(jq -nc --arg w "$w1" --arg i "$iid" '{workerId:$w, instanceId:($i + "-w1"), ttlSeconds:300, metadata:{range:"a-m"}}')"
  echo "Тело (JSON):" && echo "${json1}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register" "${json1}"
  demo_print_last_response
  local tok1
  tok1="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: Регистрация создаёт ключ воркера с lease; token нужен для heartbeat/deregister."
  demo_pause

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  printf "║ Шаг 6 — Регистрация воркера %s\n" "${w2}"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local json2 iid2
  iid2="${iid}-w2"
  json2="$(jq -nc --arg w "$w2" --arg i "$iid2" '{workerId:$w, instanceId:$i, ttlSeconds:300, metadata:{range:"n-z"}}')"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register"
  echo "Тело (JSON):" && echo "${json2}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register" "${json2}"
  demo_print_last_response
  local tok2
  tok2="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: Второй воркер — для consistent hash по двум узлам."
  demo_pause

  demo_step_http "7" "Heartbeat воркера ${w1}" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${w1}/heartbeat" \
    "$(jq -nc --arg t "$tok1" '{token:$t}')" \
    "Продление lease по token (процесс должен слать heartbeat чаще TTL)."

  demo_step_http "8" "Heartbeat воркера ${w2}" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${w2}/heartbeat" \
    "$(jq -nc --arg t "$tok2" '{token:$t}')" \
    "То же для второго воркера."

  demo_step_http "9" "Список workerId (active region)" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/ids" "" \
    "Короткий список id для rings/assign." \
    "" \
    "${coord_tok}"

  demo_step_http "10" "Полный список воркеров (payload)" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers" "" \
    "Все записи воркеров в текущем регионе." \
    "" \
    "${coord_tok}"

  demo_step_http "11" "Consistent hash: rings/assign (только ответ, etcd не пишет)" "POST" \
    "${BASE_ACTIVE}/v1/rings/assign" \
    "{\"workerIds\":[\"${w1}\",\"${w2}\"],\"virtualNodes\":128}" \
    "Вычисление диапазонов кольца на сервере; в etcd ключей не создаёт. Путь без tenant: POST /v1/rings/assign. Массив points с pos — только при withPos:true в теле."

  demo_step_grpc "12" "gRPC ListWorkers (нужен coordinatorReadToken)" "${GRPC_ACTIVE_ADDR}" \
    "coordination.v1.Coordination/ListWorkers" \
    "$(jq -nc --arg t "$TENANT" --arg ct "$coord_tok" '{tenantId:$t,coordinatorReadToken:$ct}')" \
    "Список воркеров active region; тот же секрет, что для GET /workers. Если grpcurl нет — шаг пропускается."

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 13 — Job acquire (эксклюзивная блокировка по jobId)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local job_json job_tok
  job_json="$(jq -nc --arg h "$w1" --arg pt "$tok1" \
    '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{kind:"step-demo"}}')"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire"
  echo "Тело (JSON):" && echo "${job_json}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire" "${job_json}"
  demo_print_last_response
  job_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  echo "Пояснение: holder — id зарегистрированного воркера (или coordinatorId); participantToken — токен register/acquire."
  echo "В ответе при acquired=true есть отдельный token лока — его сохраняем для renew/release (как у coordinator)."
  demo_pause

  demo_step_http "14" "Job renew (по token)" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:renew" \
    "$(jq -nc --arg t "$job_tok" '{token:$t}')" \
    "Продление etcd lease лока при совпадении token с записью в etcd."

  demo_step_http "15" "Job GET" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}" "" \
    "Текущее состояние лока в active region." \
    "" \
    "${coord_tok}"

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 16 — Global coordinator acquire (токен нужен для GET global ниже)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  local gcoord_json global_coord_tok
  gcoord_json="{\"coordinatorId\":\"${gcoord}\",\"instanceId\":\"${ginst}\",\"ttlSeconds\":300,\"metadata\":{\"via\":\"happy-step\"}}"
  echo "Запрос: POST ${BASE_ACTIVE}/v1/tenants/${TENANT}/global/coordinator/acquire"
  echo "Тело (JSON):" && echo "${gcoord_json}" | jq .
  echo "—"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/coordinator/acquire" "${gcoord_json}"
  demo_print_last_response
  global_coord_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  echo ""
  demo_pause

  demo_step_http "17" "Global coordinator GET" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/coordinator" "" \
    "Чтение глобального лидера (Bearer = token global acquire)." \
    "" \
    "${global_coord_tok}"

  demo_step_http "18" "Job release (по token)" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:release" \
    "$(jq -nc --arg t "$job_tok" '{token:$t}')" \
    "Снятие лока с проверкой token; ключ в etcd удаляется."

  demo_step_http "19" "Deregister воркера ${w1}" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${w1}/deregister" \
    "$(jq -nc --arg t "$tok1" '{token:$t}')" \
    "Корректное снятие регистрации с проверкой token (освобождает lease-ключ воркера)."

  demo_step_http "20" "Deregister воркера ${w2}" "POST" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${w2}/deregister" \
    "$(jq -nc --arg t "$tok2" '{token:$t}')" \
    "То же для второго воркера."

  demo_step_etcd_delete_tenant_prefix "${TENANT}"

  echo "Готово: happy path завершён. После очистки префикса etcd для tenant ключи coordinator/workers/jobs сняты;"
  echo "при DEMO_STEP_ETCD_DELETE_TENANT=0 старые лидеры остаются до TTL — см. README."
}

main "$@"