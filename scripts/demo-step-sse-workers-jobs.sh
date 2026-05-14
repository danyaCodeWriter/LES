#!/usr/bin/env bash
#
# Отдельный терминал: два SSE-потока одновременно — stream/workers и stream/jobs (active region или global).
# Строки помечаются префиксами [workers] и [jobs]. Остановка: Ctrl+C.
#
# Active region (по умолчанию):
#   export COORD_SSE_BEARER='<token из POST …/coordinator/acquire>'
#   ./scripts/demo-step-sse-workers-jobs.sh
#
# Global:
#   export DEMO_SSE_SCOPE=global
#   export GLOBAL_SSE_BEARER='<token из POST …/global/coordinator/acquire>'
#   ./scripts/demo-step-sse-workers-jobs.sh
#
# Переменные: TENANT, BASE_ACTIVE (как в demo-two-dc.sh). Опционально stdbuf для построчного вывода: наличие stdbuf в PATH.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/demo-step-helpers.sh
source "${SCRIPT_DIR}/lib/demo-step-helpers.sh"

: "${DEMO_SSE_SCOPE:=active}"

_sse_prefix_lines() {
  local prefix="$1"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    printf '%s %s\n' "${prefix}" "${line}"
  done
}

_curl_sse() {
  if command -v stdbuf >/dev/null 2>&1; then
    stdbuf -oL curl -sS -N \
      -H 'Accept: text/event-stream' \
      -H "Authorization: Bearer $2" \
      "$1" 2>&1
  else
    curl -sS -N \
      -H 'Accept: text/event-stream' \
      -H "Authorization: Bearer $2" \
      "$1" 2>&1
  fi
}

main() {
  demo_need_tools
  demo_require_active_healthy

  local base workers_url jobs_url token
  base="${BASE_ACTIVE%/}"

  if [[ "${DEMO_SSE_SCOPE}" == "global" ]]; then
    token="${GLOBAL_SSE_BEARER:-}"
    if [[ -z "${token}" ]]; then
      echo "error: для DEMO_SSE_SCOPE=global задайте GLOBAL_SSE_BEARER (token из POST …/global/coordinator/acquire)." >&2
      exit 1
    fi
    workers_url="${base}/v1/tenants/${TENANT}/global/stream/workers"
    jobs_url="${base}/v1/tenants/${TENANT}/global/stream/jobs"
    echo "SSE (global, Ctrl+C): workers + jobs, tenant=${TENANT}"
  else
    token="${COORD_SSE_BEARER:-}"
    if [[ -z "${token}" ]]; then
      echo "error: задайте COORD_SSE_BEARER — токен из POST …/coordinator/acquire (active region), тот же что для GET …/workers." >&2
      echo "  Пример: export COORD_SSE_BEARER=\$(curl -sS -X POST .../coordinator/acquire -d '...' | jq -r .token)" >&2
      exit 1
    fi
    workers_url="${base}/v1/tenants/${TENANT}/stream/workers"
    jobs_url="${base}/v1/tenants/${TENANT}/stream/jobs"
    echo "SSE (active region, Ctrl+C): workers + jobs, tenant=${TENANT}"
  fi

  echo "URLs:"
  echo "  [workers] ${workers_url}"
  echo "  [jobs]    ${jobs_url}"
  echo ""

  local pw="" pj=""
  trap 'kill ${pw:-0} ${pj:-0} 2>/dev/null; echo ""; echo "SSE остановлен (Ctrl+C)."; exit 130' INT TERM

  _curl_sse "${workers_url}" "${token}" | _sse_prefix_lines "[workers]" &
  pw=$!
  _curl_sse "${jobs_url}" "${token}" | _sse_prefix_lines "[jobs]" &
  pj=$!

  wait "${pw}" "${pj}" || true
}

main "$@"
