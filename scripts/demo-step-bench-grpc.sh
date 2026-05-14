#!/usr/bin/env bash
#
# Измерение времени gRPC (grpcurl + встроенный bash time, TIMEFORMAT → сек → ms): GetCoordinator, RenewCoordinator,
# два ListWorkers подряд (аналог «лёгкий + полный» список; отдельного RPC только id, как REST workers/ids, в proto нет),
# GetJob, RenewJob, затем REST GET global KV (в proto нет gRPC для ticket-KV — та же колонка kv_GET_REST, что в bench-rest).
#
# Переменные:
#   BENCH_ITERATIONS, BENCH_WARMUP — как в demo-step-bench-rest.sh
#   TENANT, BASE_ACTIVE, GRPC_ACTIVE_ADDR — как в demo-two-dc.sh / demo-step-helpers.sh
#   BENCH_PROTO_ROOT — каталог с coordination/v1/coordination.proto (по умолчанию ../proto от этого скрипта)
#
#   BENCH_ITERATIONS=30 BENCH_WARMUP=5 ./scripts/demo-step-bench-grpc.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
: "${BENCH_PROTO_ROOT:=${REPO_ROOT}/proto}"
: "${BENCH_ITERATIONS:=15}"
: "${BENCH_WARMUP:=3}"

sec_to_ms() {
  awk -v s="$1" 'BEGIN { printf "%d\n", s * 1000 + 0.5 }'
}

# stdout: целое ms или nan. Без отдельного python на каждый вызов — иначе в цифру попадает старт интерпретатора (~15–40 ms на macOS).
_grpc_ms() {
  local method_full="$1" json_body="$2"
  local tfile elapsed rc
  tfile="$(mktemp)"
  TIMEFORMAT='%R'
  {
    time grpcurl -plaintext \
      -import-path "${BENCH_PROTO_ROOT}" \
      -proto coordination/v1/coordination.proto \
      -d "${json_body}" \
      "${GRPC_ACTIVE_ADDR}" \
      "${method_full}" >/dev/null 2>&1
  } 2>"${tfile}"
  rc=$?
  elapsed="$(tr -d ' \r\n' <"${tfile}")"
  rm -f "${tfile}"
  if [[ "${rc}" != 0 ]]; then
    echo "nan"
    return 0
  fi
  sec_to_ms "${elapsed}"
}

_tt_get_bearer() {
  local url="$1" bearer="$2"
  curl -sS -o /dev/null -w '%{time_total}' --max-time 60 -H "Authorization: Bearer ${bearer}" "${url}" 2>/dev/null || echo "nan"
}

bench_one_round() {
  local base="$1" tenant="$2" coord_tok="$3" job_id="$4" job_tok="$5" kv_key="$6" kv_tok="$7" holder="$8"
  local ms_cget ms_crenew ms_l1 ms_l2 ms_jget ms_jrenew ms_kv s
  local j_gc j_rc j_lw j_gj j_rj

  j_gc="$(jq -nc --arg t "${tenant}" --arg ct "${coord_tok}" '{tenantId:$t,coordinatorReadToken:$ct}')"
  ms_cget="$(_grpc_ms coordination.v1.Coordination/GetCoordinator "${j_gc}")"

  j_rc="$(jq -nc --arg t "${tenant}" --arg tok "${coord_tok}" '{tenantId:$t,token:$tok}')"
  ms_crenew="$(_grpc_ms coordination.v1.Coordination/RenewCoordinator "${j_rc}")"

  j_lw="$(jq -nc --arg t "${tenant}" --arg ct "${coord_tok}" '{tenantId:$t,coordinatorReadToken:$ct}')"
  ms_l1="$(_grpc_ms coordination.v1.Coordination/ListWorkers "${j_lw}")"
  ms_l2="$(_grpc_ms coordination.v1.Coordination/ListWorkers "${j_lw}")"

  j_gj="$(jq -nc --arg t "${tenant}" --arg j "${job_id}" --arg ct "${coord_tok}" '{tenantId:$t,jobId:$j,coordinatorReadToken:$ct}')"
  ms_jget="$(_grpc_ms coordination.v1.Coordination/GetJob "${j_gj}")"

  j_rj="$(jq -nc --arg t "${tenant}" --arg j "${job_id}" --arg h "${holder}" --arg tok "${job_tok}" '{tenantId:$t,jobId:$j,holder:$h,token:$tok}')"
  ms_jrenew="$(_grpc_ms coordination.v1.Coordination/RenewJob "${j_rj}")"

  s="$(_tt_get_bearer "${base}/v1/tenants/${tenant}/global/kv/${kv_key}" "${kv_tok}")"
  ms_kv="$(sec_to_ms "${s}")"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${ms_cget}" "${ms_crenew}" "${ms_l1}" "${ms_l2}" "${ms_jget}" "${ms_jrenew}" "${ms_kv}"
}

main() {
  demo_need_tools
  if ! command -v grpcurl >/dev/null 2>&1; then
    echo "error: нужен grpcurl в PATH (brew install grpcurl / см. README)." >&2
    exit 1
  fi
  if [[ ! -f "${BENCH_PROTO_ROOT}/coordination/v1/coordination.proto" ]]; then
    echo "error: не найден proto: ${BENCH_PROTO_ROOT}/coordination/v1/coordination.proto (задайте BENCH_PROTO_ROOT)." >&2
    exit 1
  fi

  echo "Benchmark gRPC (ACTIVE). GRPC_ACTIVE_ADDR=${GRPC_ACTIVE_ADDR}  BASE_ACTIVE=${BASE_ACTIVE}  TENANT=${TENANT}"
  echo "Proto: BENCH_PROTO_ROOT=${BENCH_PROTO_ROOT}"
  echo "Итераций: ${BENCH_ITERATIONS}, разогрев: ${BENCH_WARMUP} (BENCH_ITERATIONS / BENCH_WARMUP)."
  echo ""
  demo_require_active_healthy

  local cid="bench-grpc-coord" iid="bench-grpc-pod" w1="bench-grpc-worker" job="bench-grpc-job"
  local kv_key="bench-kv-grpc-$(date +%s)"
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

  job_json="$(jq -nc --arg h "$w1" --arg pt "$wt" '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{}}')"
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

  echo "Подготовка: coordinator + worker + job + KV созданы через REST (tenant=${TENANT})."
  echo ""

  local w
  for ((w = 1; w <= BENCH_WARMUP; w++)); do
    bench_one_round "${BASE_ACTIVE}" "${TENANT}" "${coord_tok}" "${job}" "${job_tok}" "${kv_key}" "${kv_tok}" "${w1}" >/dev/null
  done

  echo "Колонки (все значения в ms):"
  echo "  coord_GET  coordinator_renew  listWorkers_1  listWorkers_2  job_GET  job_renew  kv_GET_REST"
  echo "------------------------------------------------------------------------------------------"

  local tsv
  tsv="$(mktemp)"
  local i line
  for ((i = 1; i <= BENCH_ITERATIONS; i++)); do
    line="$(bench_one_round "${BASE_ACTIVE}" "${TENANT}" "${coord_tok}" "${job}" "${job_tok}" "${kv_key}" "${kv_tok}" "${w1}")"
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
      split("coord_GET coord_renew listW_1 listW_2 job_GET job_renew kv_GET_REST", lbl, " ")
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
