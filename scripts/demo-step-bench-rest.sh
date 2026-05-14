#!/usr/bin/env bash
#
# Измерение времени REST (curl %{time_total} → ms): coordinator GET/renew, workers/ids, workers,
# job GET, global KV GET. Без пауз между итерациями.
#
# Переменные:
#   BENCH_ITERATIONS  — число замеров после разогрева (по умолчанию 15)
#   BENCH_WARMUP      — разогрев, ответы не печатаем (по умолчанию 3)
#   TENANT, BASE_ACTIVE — как в demo-two-dc.sh
#
#   BENCH_ITERATIONS=30 BENCH_WARMUP=5 ./scripts/demo-step-bench-rest.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

: "${BENCH_ITERATIONS:=15}"
: "${BENCH_WARMUP:=3}"

sec_to_ms() {
  awk -v s="$1" 'BEGIN { printf "%d\n", s * 1000 + 0.5 }'
}

# Печатает только time_total (секунды) в stdout.
_tt_get_bearer() {
  local url="$1" bearer="$2"
  curl -sS -o /dev/null -w '%{time_total}' --max-time 60 -H "Authorization: Bearer ${bearer}" "${url}" 2>/dev/null || echo "nan"
}

_tt_post_json() {
  local url="$1" json="$2"
  curl -sS -o /dev/null -w '%{time_total}' --max-time 60 -X POST "${url}" \
    -H 'content-type: application/json' -d "${json}" 2>/dev/null || echo "nan"
}

_tt_put_json() {
  local url="$1" json="$2"
  curl -sS -o /dev/null -w '%{time_total}' --max-time 60 -X PUT "${url}" \
    -H 'content-type: application/json' -d "${json}" 2>/dev/null || echo "nan"
}

bench_one_round() {
  local base="$1" tenant="$2" coord_tok="$3" job_id="$4" job_tok="$5" kv_key="$6" kv_tok="$7"
  local s ms_cget ms_crenew ms_wids ms_ws ms_jget ms_kv

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/coordinator" "${coord_tok}")"
  ms_cget="$(sec_to_ms "${s}")"

  s="$(_tt_post_json "${base}/v1/tenants/${tenant}/coordinator/renew" "$(jq -nc --arg t "${coord_tok}" '{token:$t}')")"
  ms_crenew="$(sec_to_ms "${s}")"

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/workers/ids" "${coord_tok}")"
  ms_wids="$(sec_to_ms "${s}")"

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/workers" "${coord_tok}")"
  ms_ws="$(sec_to_ms "${s}")"

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/jobs/${job_id}" "${coord_tok}")"
  ms_jget="$(sec_to_ms "${s}")"

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/global/kv/${kv_key}" "${kv_tok}")"
  ms_kv="$(sec_to_ms "${s}")"

  s="$(_tt_post_json "${base}/v1/tenants/${tenant}/jobs/${job_id}:renew" "$(jq -nc --arg t "${job_tok}" '{token:$t}')")"
  local ms_jrenew
  ms_jrenew="$(sec_to_ms "${s}")"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${ms_cget}" "${ms_crenew}" "${ms_wids}" "${ms_ws}" "${ms_jget}" "${ms_jrenew}" "${ms_kv}"
}

main() {
  demo_need_tools
  echo "Benchmark REST (ACTIVE). BASE_ACTIVE=${BASE_ACTIVE}  TENANT=${TENANT}"
  echo "Итераций: ${BENCH_ITERATIONS}, разогрев: ${BENCH_WARMUP} (BENCH_ITERATIONS / BENCH_WARMUP)."
  echo ""
  demo_require_active_healthy

  local cid="bench-rest-coord" iid="bench-rest-pod" w1="bench-rest-worker" job="bench-rest-job"
  local kv_key="bench-kv-$(date +%s)"
  local coord_json coord_tok reg_json wt job_json job_tok kv_put kv_tok

  coord_json="$(jq -nc --arg c "$cid" --arg i "$iid" '{coordinatorId:$c, instanceId:$i, ttlSeconds:300, metadata:{bench:true}}')"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator/acquire" "${coord_json}"
  if [[ "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: coordinator acquire HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_print_last_response
    exit 1
  fi
  coord_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response
  if [[ -z "${coord_tok}" ]]; then
    echo "error: пустой coordinator token" >&2
    exit 1
  fi

  reg_json="$(jq -nc --arg w "$w1" --arg i "${iid}-w" '{workerId:$w, instanceId:$i, ttlSeconds:300, metadata:{}}')"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/register" "${reg_json}"
  if [[ "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: worker register HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_print_last_response
    exit 1
  fi
  wt="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response

  job_json="$(jq -nc --arg h "$w1" --arg pt "$wt" '{holder:$h, participantToken:$pt, ttlSeconds:300, metadata:{}}')"
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:acquire" "${job_json}"
  if [[ "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: job acquire HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_print_last_response
    exit 1
  fi
  job_tok="$(jq -r '.token // empty' "${DEMO_LAST_BODY_FILE}")"
  demo_cleanup_last_response

  kv_put="$(jq -nc '{value:{k:1}, ttlSeconds:300}')"
  DEMO_LAST_BODY_FILE="$(mktemp)"
  DEMO_LAST_HTTP_CODE="$(curl -sS -o "${DEMO_LAST_BODY_FILE}" -w '%{http_code}' --max-time 60 -X PUT \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}" \
    -H 'content-type: application/json' -d "${kv_put}" 2>/dev/null || echo "000")"
  if [[ "${DEMO_LAST_HTTP_CODE}" != "201" && "${DEMO_LAST_HTTP_CODE}" != "200" ]]; then
    echo "error: PUT KV HTTP ${DEMO_LAST_HTTP_CODE}" >&2
    demo_print_last_response
    exit 1
  fi
  kv_tok="$(jq -r '.accessToken // empty' "${DEMO_LAST_BODY_FILE}")"
  rm -f "${DEMO_LAST_BODY_FILE}"
  if [[ -z "${kv_tok}" ]]; then
    echo "error: пустой KV accessToken" >&2
    exit 1
  fi

  echo "Подготовка: coordinator + worker + job + KV созданы (tenant=${TENANT})."
  echo ""

  local w
  for ((w = 1; w <= BENCH_WARMUP; w++)); do
    bench_one_round "${BASE_ACTIVE}" "${TENANT}" "${coord_tok}" "${job}" "${job_tok}" "${kv_key}" "${kv_tok}" >/dev/null
  done

  echo "Колонки (все значения в ms):"
  echo "  coord_GET  coordinator_renew  workers_ids  workers_list  job_GET  job_renew  kv_GET"
  echo "------------------------------------------------------------------------------------------"

  local tsv
  tsv="$(mktemp)"
  local i line
  for ((i = 1; i <= BENCH_ITERATIONS; i++)); do
    line="$(bench_one_round "${BASE_ACTIVE}" "${TENANT}" "${coord_tok}" "${job}" "${job_tok}" "${kv_key}" "${kv_tok}")"
    echo "${line}" | tr '\t' ' ' | awk -v n="$i" '{ printf "%3d  %6s  %6s  %6s  %6s  %6s  %6s  %6s\n", n, $1, $2, $3, $4, $5, $6, $7 }'
    echo "${line}" >>"${tsv}"
  done

  echo "------------------------------------------------------------------------------------------"
  echo "Сводка (среднее / min / max по ${BENCH_ITERATIONS} итерациям, ms):"
  awk -F'\t' '
    {
      n++
      for (i = 1; i <= 7; i++) {
        v = $i + 0
        sum[i] += v
        if (n == 1 || v < min[i]) min[i] = v
        if (n == 1 || v > max[i]) max[i] = v
      }
    }
    END {
      split("coord_GET coord_renew workers_ids workers_list job_GET job_renew kv_GET", lbl, " ")
      for (i = 1; i <= 7; i++)
        printf "  %-14s avg=%.1f  min=%d  max=%d\n", lbl[i] ":", sum[i] / n, min[i], max[i]
    }
  ' "${tsv}"
  rm -f "${tsv}"

  echo ""
  echo "Очистка: release job, deregister worker, DELETE KV (coordinator остаётся до TTL)."
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/${job}:release" "$(jq -nc --arg t "${job_tok}" '{token:$t}')"
  demo_cleanup_last_response
  demo_exec_post "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/${w1}/deregister" "$(jq -nc --arg t "${wt}" '{token:$t}')"
  demo_cleanup_last_response
  curl -sS -o /dev/null --max-time 60 -X DELETE \
    -H "Authorization: Bearer ${kv_tok}" \
    "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/kv/${kv_key}" || true
  demo_step_etcd_delete_tenant_prefix "${TENANT}"
  echo "Готово."
}

main "$@"
