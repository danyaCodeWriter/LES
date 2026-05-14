#!/usr/bin/env bash
#
# Только два фокуса API (остальное — тихая подготовка):
#   1) GET …/workers/{workerId}/jobs — все job-lock'и воркера (X-Worker-Token)
#   2) PUT / GET / DELETE …/global/kv/{keyId} — KV ticket
# В конце: тихий release job + deregister worker, затем etcdctl del префикса tenant (если DEMO_STEP_ETCD_DELETE_TENANT=1 и etcdctl в PATH).
#
#   DEMO_STEP_ETCD_DELETE_TENANT=0 ./scripts/demo-step-workers-and-keys.sh  — не трогать etcd
#   chmod +x scripts/demo-step-workers-and-keys.sh && ./scripts/demo-step-workers-and-keys.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

# Возвращает путь к префиксу state-файлов: ${pfx}.coord ${pfx}.wt ${pfx}.jt
_prep_for_jobs_and_kv() {
  local tenant="$1" base="$2" wid="$3" jid="$4"
  local pfx code coord_tok wt jbody jcode job_tok
  pfx="$(mktemp)"

  demo_exec_post "${base}/v1/tenants/${tenant}/coordinator/acquire" \
    "$(jq -nc '{coordinatorId:"wk-demo-coord",instanceId:"wk-demo-pod",ttlSeconds:300,metadata:{prep:true}}')"
  code="${DEMO_LAST_HTTP_CODE}"
  if [[ "${code}" == "200" ]]; then
    coord_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  elif [[ "${code}" == "409" ]]; then
    coord_tok="$(jq -r '.currentHolder.token // empty' "${DEMO_LAST_BODY_FILE}")"
  fi
  demo_cleanup_last_response
  if [[ -z "${coord_tok}" ]]; then
    echo "error: не удалось получить regional coordinator token (HTTP ${code})" >&2
    exit 1
  fi

  demo_exec_post "${base}/v1/tenants/${tenant}/workers/register" \
    "$(jq -nc --arg w "$wid" --arg i "wk-inst-${wid}" '{workerId:$w,instanceId:$i,ttlSeconds:300,metadata:{prep:true}}')"
  if [[ "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: worker register HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_print_last_response
    exit 1
  fi
  wt="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response

  jbody="$(jq -nc --arg h "$wid" --arg pt "$wt" '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{prep:true}}')"
  demo_exec_post "${base}/v1/tenants/${tenant}/jobs/${jid}:acquire" "${jbody}"
  jcode="${DEMO_LAST_HTTP_CODE}"
  if [[ "${jcode}" != "200" ]]; then
    echo "error: job acquire HTTP ${jcode}" >&2
    demo_print_last_response
    exit 1
  fi
  job_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  if [[ -z "${job_tok}" ]]; then
    echo "error: пустой job token" >&2
    exit 1
  fi

  printf '%s' "${coord_tok}" >"${pfx}.coord"
  printf '%s' "${wt}" >"${pfx}.wt"
  printf '%s' "${job_tok}" >"${pfx}.jt"
  echo "${pfx}"
}

_quiet_teardown_rest() {
  local base="$1" tenant="$2" wid="$3" jid="$4" job_tok="$5" wt="$6"
  curl -sS -o /dev/null --max-time 30 -X POST "${base}/v1/tenants/${tenant}/jobs/${jid}:release" \
    -H 'content-type: application/json' -d "$(jq -nc --arg t "$job_tok" '{token:$t}')" || true
  curl -sS -o /dev/null --max-time 30 -X POST "${base}/v1/tenants/${tenant}/workers/${wid}/deregister" \
    -H 'content-type: application/json' -d "$(jq -nc --arg t "$wt" '{token:$t}')" || true
}

main() {
  demo_need_tools
  demo_print_usage_pause
  echo ""
  echo "Сценарий: только GET …/workers/{id}/jobs и global KV (PUT → GET → DELETE)."
  echo "Подготовка без пошагового вывода; затем очистка lease через REST и префикса tenant в etcd."
  echo ""
  demo_require_active_healthy

  local rid wid jid kv_key kv_put kv_tok pfx wt job_tok worker_hdr
  rid="$(date +%s)"
  wid="wk-jobs-${rid}"
  jid="wk-job-${rid}"
  kv_key="wk-kv-${rid}"
  kv_put="$(jq -nc '{value:{demo:"kv-ticket"},ttlSeconds:180}')"

  echo "Подготовка (тихо): coordinator + worker «${wid}» + job «${jid}» …"
  pfx="$(_prep_for_jobs_and_kv "${TENANT}" "${BASE_ACTIVE}" "${wid}" "${jid}")"
  wt="$(cat "${pfx}.wt")"
  job_tok="$(cat "${pfx}.jt")"
  rm -f "${pfx}" "${pfx}.coord" "${pfx}.wt" "${pfx}.jt"
  echo ""

  worker_hdr="X-Worker-Token: ${wt}"
  demo_step_http "1" "GET …/workers/{workerId}/jobs (все job-lock'и воркера)" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${wid}/jobs" "" \
    "Массив JobState, где holder = этот worker; нужен только **X-Worker-Token** из POST …/workers/register." \
    "" "" "${worker_hdr}"

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  echo "║ Шаг 2 — PUT …/global/kv/{keyId} (создание ticket, без Authorization)"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  echo "Запрос: PUT ${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}"
  echo "Тело (JSON):" && echo "${kv_put}" | jq .
  echo "—"
  demo_exec_put "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}" "${kv_put}"
  demo_print_last_response
  if [[ "${DEMO_LAST_HTTP_CODE}" != "201" && "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: PUT KV HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_cleanup_last_response
    _quiet_teardown_rest "${BASE_ACTIVE}" "${TENANT}" "${wid}" "${jid}" "${job_tok}" "${wt}"
    demo_step_etcd_delete_tenant_prefix "${TENANT}"
    exit 1
  fi
  kv_tok="$(jq -r '.accessToken // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  if [[ -z "${kv_tok}" ]]; then
    echo "error: нет accessToken в ответе PUT KV" >&2
    _quiet_teardown_rest "${BASE_ACTIVE}" "${TENANT}" "${wid}" "${jid}" "${job_tok}" "${wt}"
    demo_step_etcd_delete_tenant_prefix "${TENANT}"
    exit 1
  fi
  echo ""
  echo "Пояснение: сохраните accessToken для GET и DELETE этого ключа."
  demo_pause

  demo_step_http "3" "GET …/global/kv/{keyId} (Bearer = accessToken)" "GET" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}" "" \
    "Чтение значения по секрету, выданному при создании." \
    "" "${kv_tok}"

  demo_step_http "4" "DELETE …/global/kv/{keyId}" "DELETE" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}" "" \
    "Удаление ticket; заголовок Authorization = accessToken." \
    "" "${kv_tok}"

  echo "Очистка (тихо): release job, deregister worker …"
  _quiet_teardown_rest "${BASE_ACTIVE}" "${TENANT}" "${wid}" "${jid}" "${job_tok}" "${wt}"
  demo_step_etcd_delete_tenant_prefix "${TENANT}"
  echo "Готово."
}

main "$@"
