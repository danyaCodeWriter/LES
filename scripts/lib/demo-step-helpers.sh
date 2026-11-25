#!/usr/bin/env bash
# Общие функции для пошаговых демо-скриптов (source из demo-step-*.sh).
# Переменные (можно переопределить в окружении):
#   DEMO_STEP_PAUSE_SEC — пауза между шагами (по умолчанию 20)
#   TENANT, BASE_ACTIVE, BASE_STANDBY — как в demo-two-dc.sh

: "${DEMO_STEP_PAUSE_SEC:=20}"
: "${TENANT:=demo-team}"
: "${BASE_ACTIVE:=http://127.0.0.1:8080}"
: "${BASE_STANDBY:=http://127.0.0.1:8081}"
: "${GRPC_ACTIVE_ADDR:=127.0.0.1:9090}"
: "${COORD_DEMO_REGION_INITIAL:=r1}"
: "${COORD_ETCD_ROOT_PREFIX:=/coord/v1/tenants}"
: "${DEMO_STEP_ETCD_DELETE_TENANT:=1}"
: "${ETCDCTL_ENDPOINTS:=127.0.0.1:2379}"
# Имя контейнера из scripts/demo-two-dc.sh (etcd-start); для очистки без локального etcdctl: docker exec … etcdctl
: "${ETCD_CONTAINER_NAME:=etcd-coord-demo}"

demo_need_tools() {
  command -v curl >/dev/null 2>&1 || {
    echo "error: нужен curl" >&2
    exit 1
  }
  command -v jq >/dev/null 2>&1 || {
    echo "error: нужен jq" >&2
    exit 1
  }
}

demo_require_active_healthy() {
  demo_need_tools
  local body st
  if ! body="$(curl -sfS --max-time 8 "${BASE_ACTIVE}/actuator/health" 2>/dev/null)"; then
    echo "error: ACTIVE не отвечает GET ${BASE_ACTIVE}/actuator/health" >&2
    echo "  Поднимите по порядку: scripts/demo-two-dc.sh etcd-start && init-file && run-active (JDK 21+; скрипт сам cd в репо и выставит JAVA_HOME при необходимости)." >&2
    exit 1
  fi
  st="$(echo "$body" | jq -r '.status // empty')"
  if [[ "$st" != "UP" ]]; then
    echo "error: ACTIVE health status='$st' (нужен UP). Ответ: $body" >&2
    exit 1
  fi
}

demo_require_standby_healthy() {
  demo_need_tools
  local body st
  if ! body="$(curl -sfS --max-time 8 "${BASE_STANDBY}/actuator/health" 2>/dev/null)"; then
    echo "error: STANDBY не отвечает GET ${BASE_STANDBY}/actuator/health" >&2
    echo "  Поднимите: scripts/demo-two-dc.sh run-standby (или start-standby-bg), после etcd-start и init-file." >&2
    exit 1
  fi
  st="$(echo "$body" | jq -r '.status // empty')"
  if [[ "$st" != "UP" ]]; then
    echo "error: STANDBY health status='$st'. Ответ: $body" >&2
    exit 1
  fi
}

demo_pause() {
  echo ""
  echo "──────── пауза ${DEMO_STEP_PAUSE_SEC}s (переменная DEMO_STEP_PAUSE_SEC) ────────"
  sleep "${DEMO_STEP_PAUSE_SEC}"
  echo ""
}

# Печать шага: номер, заголовок, описание запроса, выполнение HTTP, пояснение, пауза.
# Аргументы: step_no title method url json_body explain [expected_http_code] [bearer_for_get_or_auth_header] [extra_header]
# json_body: пустая строка для GET или DELETE без тела
# expected_http_code: если задан (например 409), при несовпадении с фактическим кодом — exit 1
# bearer: для GET — Authorization: Bearer; для POST/PUT/DELETE — тоже добавляется, если непусто
# extra_header: одна дополнительная строка заголовка curl, напр. "X-Worker-Token: secret" (для GET …/workers/{id}/jobs)
demo_step_http() {
  local step_no="$1" title="$2" method="$3" url="$4" json_body="$5" explain="$6" expect="${7:-}" bearer="${8:-}" extra_header="${9:-}"
  local tmp code
  tmp="$(mktemp)"

  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  printf "║ Шаг %s — %s\n" "$step_no" "$title"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  echo "Запрос: ${method} ${url}"
  if [[ -n "${json_body}" ]]; then
    echo "Тело (JSON):"
    echo "${json_body}" | jq . 2>/dev/null || echo "${json_body}"
  fi
  if [[ "${method}" == "GET" ]]; then
    demo_log_get_coordinator_auth "${url}" "${bearer}" "${extra_header}"
  elif [[ -n "${bearer}" || -n "${extra_header}" ]]; then
    if [[ -n "${bearer}" ]]; then
      echo "Заголовок: Authorization: Bearer …"
    fi
    if [[ -n "${extra_header}" ]]; then
      echo "Доп. заголовок: ${extra_header}"
    fi
  fi
  echo "—"

  set +e
  if [[ "${method}" == "GET" ]]; then
    if [[ -n "${bearer}" ]]; then
      if [[ -n "${extra_header}" ]]; then
        code="$(curl -sS -o "${tmp}" -w '%{http_code}' --max-time 60 \
          -H "Authorization: Bearer ${bearer}" -H "${extra_header}" "${url}" 2>/dev/null || echo "000")"
      else
        code="$(curl -sS -o "${tmp}" -w '%{http_code}' --max-time 60 -H "Authorization: Bearer ${bearer}" "${url}" 2>/dev/null || echo "000")"
      fi
    else
      if [[ -n "${extra_header}" ]]; then
        code="$(curl -sS -o "${tmp}" -w '%{http_code}' --max-time 60 -H "${extra_header}" "${url}" 2>/dev/null || echo "000")"
      else
        code="$(curl -sS -o "${tmp}" -w '%{http_code}' --max-time 60 "${url}" 2>/dev/null || echo "000")"
      fi
    fi
  else
    local -a curl_args=(-sS -o "${tmp}" -w '%{http_code}' --max-time 60 -X "${method}" "${url}")
    if [[ -n "${json_body}" ]]; then
      curl_args+=(-H 'content-type: application/json' -d "${json_body}")
    fi
    if [[ -n "${bearer}" ]]; then
      curl_args+=(-H "Authorization: Bearer ${bearer}")
    fi
    if [[ -n "${extra_header}" ]]; then
      curl_args+=(-H "${extra_header}")
    fi
    code="$(curl "${curl_args[@]}" 2>/dev/null || echo "000")"
  fi
  set +e

  echo "Ответ: HTTP ${code}"
  if jq -e . <"${tmp}" >/dev/null 2>&1; then
    jq . <"${tmp}"
  else
    cat "${tmp}"
  fi
  if [[ -n "${expect}" && "${code}" != "${expect}" ]]; then
    rm -f "${tmp}"
    echo "error: ожидали HTTP ${expect}, получили ${code}" >&2
    exit 1
  fi
  rm -f "${tmp}"
  echo ""
  echo "Пояснение: ${explain}"
  demo_pause
}

# Лог для GET: нужен ли токен координатора (Authorization: Bearer) и что реально уходит в curl.
demo_log_get_coordinator_auth() {
  local url="$1"
  local bearer="$2"
  local extra_header="$3"
  local path="${url%%\?*}"
  if [[ "$path" =~ /v1/tenants/[^/]+/workers/[^/]+/jobs$ ]]; then
    echo "Правило: GET job-lock'ов воркера — заголовок **X-Worker-Token** (= token из POST …/workers/register в этом контуре). Coordinator Bearer не нужен."
    if [[ -n "${extra_header}" ]]; then
      echo "Заголовок запроса: ${extra_header}"
    else
      echo "Заголовок запроса: (нет X-Worker-Token) — ожидайте 401."
    fi
    return 0
  fi
  if [[ "$path" =~ /v1/tenants/[^/]+/global/jobs/[^/]+$ ]]; then
    echo "Правило: GET …/global/jobs/{jobId} — токен **лидера global coordinator** (\`Authorization: Bearer\` / \`X-Coordinator-Token\`) **или** токен **регистрации воркера-holder** этого job (тот же Bearer или \`X-Worker-Token\`)."
    if [[ -n "${bearer}" ]]; then
      echo "Заголовок запроса: Authorization: Bearer ${bearer}"
    else
      echo "Заголовок запроса: (нет Authorization) — нужен coordinator или worker токен; иначе 401."
    fi
    if [[ -n "${extra_header}" ]]; then
      echo "Доп. заголовок: ${extra_header}"
    fi
    return 0
  fi
  if [[ "$path" =~ /v1/tenants/[^/]+/jobs/[^/:]+$ ]]; then
    echo "Правило: GET …/jobs/{jobId} (active region) — токен **лидера regional coordinator** **или** токен **регистрации воркера-holder** этого job (Bearer / \`X-Worker-Token\` / \`X-Coordinator-Token\`)."
    if [[ -n "${bearer}" ]]; then
      echo "Заголовок запроса: Authorization: Bearer ${bearer}"
    else
      echo "Заголовок запроса: (нет Authorization) — нужен coordinator или worker токен; иначе 401."
    fi
    if [[ -n "${extra_header}" ]]; then
      echo "Доп. заголовок: ${extra_header}"
    fi
    return 0
  fi
  if [[ "$path" =~ /v1/tenants/[^/]+/global/kv/[^/]+$ ]]; then
    echo "Правило: GET …/global/kv/{keyId} — **ticket-доступ**: \`Authorization: Bearer\` = accessToken из ответа PUT …/global/kv/{key} (или \`X-Kv-Access-Token\`). Без токена → **401** KV_ACCESS_TOKEN_REQUIRED."
    if [[ -n "${bearer}" ]]; then
      echo "Заголовок запроса: Authorization: Bearer ${bearer}"
    else
      echo "Заголовок запроса: (нет Authorization) — ожидайте HTTP 401."
    fi
    if [[ -n "${extra_header}" ]]; then
      echo "Доп. заголовок: ${extra_header}"
    fi
    return 0
  fi
  echo "Правило сервера: GET coordinator, workers, workers/ids, SSE stream/workers|jobs, global/* (кроме отдельно описанных GET …/jobs/{id}, …/global/jobs/{id}, …/global/kv/{id}) — только с токеном **лидера** того же контура (\`Authorization: Bearer\` = token из POST …/coordinator/acquire или …/global/coordinator/acquire). Без заголовка → **401** COORDINATOR_READ_TOKEN_REQUIRED. Альтернатива: \`X-Coordinator-Token\`."
  if [[ -n "${bearer}" ]]; then
    echo "Заголовок запроса: Authorization: Bearer ${bearer}"
  else
    echo "Заголовок запроса: (нет Authorization) — для перечисленных путей ожидайте HTTP 401."
  fi
  if [[ -n "${extra_header}" ]]; then
    echo "Доп. заголовок: ${extra_header}"
  fi
}

# grpcurl: step_no title grpc_addr full_method json_body explain
demo_step_grpc() {
  local step_no="$1" title="$2" addr="$3" full_method="$4" json_body="$5" explain="$6"
  echo "╔══════════════════════════════════════════════════════════════════════════════╗"
  printf "║ Шаг %s — %s\n" "$step_no" "$title"
  echo "╚══════════════════════════════════════════════════════════════════════════════╝"
  echo "Запрос: grpcurl -plaintext -d '<json>' ${addr} ${full_method}"
  echo "Тело (JSON):"
  echo "${json_body}" | jq . 2>/dev/null || echo "${json_body}"
  if echo "${json_body}" | jq -e '.coordinatorReadToken // empty' >/dev/null 2>&1; then
    local _crt
    _crt="$(echo "${json_body}" | jq -r '.coordinatorReadToken // empty')"
    if [[ -n "${_crt}" ]]; then
      echo "Аналог REST GET: в gRPC поле coordinatorReadToken = тот же секрет, что в заголовке Authorization: Bearer для HTTP."
    fi
  fi
  echo "—"
  if ! command -v grpcurl >/dev/null 2>&1; then
    echo "Ответ: (grpcurl не найден в PATH — шаг пропущен; установите grpcurl для gRPC-демо)"
  else
    grpcurl -plaintext -d "${json_body}" "${addr}" "${full_method}" 2>&1 || true
  fi
  echo ""
  echo "Пояснение: ${explain}"
  demo_pause
}

demo_print_usage_pause() {
  cat <<EOF
Пауза между шагами: DEMO_STEP_PAUSE_SEC=${DEMO_STEP_PAUSE_SEC} (секунды, можно export перед запуском).
Базы: BASE_ACTIVE=${BASE_ACTIVE}  BASE_STANDBY=${BASE_STANDBY}  TENANT=${TENANT}
Сервер(а): scripts/demo-two-dc.sh (etcd-start → init-file → run-active / run-standby); для Spring нужен JDK 21+.
Очистка etcd в конце demo-step: DEMO_STEP_ETCD_DELETE_TENANT=${DEMO_STEP_ETCD_DELETE_TENANT} (1 = etcdctl del префикс \${root}/\${TENANT}/), ETCDCTL_ENDPOINTS=${ETCDCTL_ENDPOINTS}
Если etcdctl нет на хосте, но запущен Docker-контейнер ETCD_CONTAINER_NAME=${ETCD_CONTAINER_NAME}, используется: docker exec … etcdctl (тот же кластер, что etcdkeeper).
EOF
}

# Нормализация корня ключей (как в Keyspace.normalizeRoot): без лишнего «/» в конце.
_demo_coord_etcd_root_trimmed() {
  local r="${COORD_ETCD_ROOT_PREFIX:-/coord/v1/tenants}"
  while [[ "${r}" == */ ]]; do r="${r%/}"; done
  [[ -z "${r}" ]] && r="/coord/v1/tenants"
  [[ "${r}" == /* ]] || r="/${r}"
  printf '%s' "${r}"
}

# etcdctl на хосте (к coordination.etcd.endpoints) или внутри контейнера демо-etcd.
_demo_etcdctl() {
  export ETCDCTL_API="${ETCDCTL_API:-3}"
  local eps="${ETCDCTL_ENDPOINTS}"
  if [[ "${eps}" != http://* && "${eps}" != https://* ]]; then
    eps="http://${eps}"
  fi
  if command -v etcdctl >/dev/null 2>&1; then
    etcdctl --endpoints="${eps}" "$@"
  elif command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${ETCD_CONTAINER_NAME}"; then
    docker exec -e ETCDCTL_API="${ETCDCTL_API:-3}" "${ETCD_CONTAINER_NAME}" etcdctl "$@"
  else
    return 127
  fi
}

# Удаление всех ключей coordination под префиксом tenant (демо-сброс).
# Отключить: DEMO_STEP_ETCD_DELETE_TENANT=0. Корень: COORD_ETCD_ROOT_PREFIX (как coordination.etcd.rootPrefix, по умолчанию /coord/v1/tenants).
demo_step_etcd_delete_tenant_prefix() {
  local tenant="$1"
  if [[ -z "${tenant}" ]]; then
    return 0
  fi
  if [[ "${DEMO_STEP_ETCD_DELETE_TENANT}" != "1" ]]; then
    echo "Очистка etcd: пропуск (DEMO_STEP_ETCD_DELETE_TENANT != 1)."
    return 0
  fi
  if ! command -v etcdctl >/dev/null 2>&1 && ! {
    command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${ETCD_CONTAINER_NAME}"
  }; then
    echo "Очистка etcd: нет etcdctl в PATH и не найден контейнер Docker «${ETCD_CONTAINER_NAME}» — префикс не трогаем." >&2
    echo "  Установите etcdctl, либо поднимите etcd: scripts/demo-two-dc.sh etcd-start (контейнер ${ETCD_CONTAINER_NAME}), либо задайте ETCD_CONTAINER_NAME." >&2
    return 0
  fi
  local root prefix out remaining vf
  root="$(_demo_coord_etcd_root_trimmed)"
  prefix="${root}/${tenant}/"
  echo "Очистка etcd: del --prefix \"${prefix}\" (etcdctl на хосте или docker exec ${ETCD_CONTAINER_NAME})"
  if out="$(_demo_etcdctl del "${prefix}" --prefix 2>&1)"; then
    echo "${out}"
  else
    echo "Очистка etcd: ошибка del: ${out}" >&2
    return 0
  fi
  vf="$(mktemp)"
  if _demo_etcdctl get "${prefix}" --prefix --keys-only >"${vf}" 2>/dev/null; then
    remaining="$(wc -l <"${vf}" | tr -d '[:space:]')"
  else
    remaining="?"
  fi
  rm -f "${vf}"
  echo "Проверка префикса «${prefix}»: осталось ключей (get --prefix --keys-only): ${remaining}"
  if [[ "${remaining}" != "0" ]]; then
    echo "Предупреждение: ключи под этим tenant в etcd ещё есть или проверка не удалась — сравните ETCDCTL_ENDPOINTS/host с coordination.etcd.endpoints и TENANT=${tenant}." >&2
  fi
}

# --- Выполнение с сохранением тела ответа (для извлечения token и т.д.) ---
DEMO_LAST_HTTP_CODE=""
DEMO_LAST_BODY_FILE=""

demo_exec_get() {
  local url="$1"
  DEMO_LAST_BODY_FILE="$(mktemp)"
  set +e
  DEMO_LAST_HTTP_CODE="$(curl -sS -o "${DEMO_LAST_BODY_FILE}" -w '%{http_code}' --max-time 60 "${url}" 2>/dev/null || echo "000")"
  set +e
}

demo_exec_post() {
  local url="$1" json="$2"
  DEMO_LAST_BODY_FILE="$(mktemp)"
  set +e
  DEMO_LAST_HTTP_CODE="$(curl -sS -o "${DEMO_LAST_BODY_FILE}" -w '%{http_code}' --max-time 60 -X POST "${url}" \
    -H 'content-type: application/json' -d "${json}" 2>/dev/null || echo "000")"
  set +e
}

demo_exec_put() {
  local url="$1" json="$2"
  DEMO_LAST_BODY_FILE="$(mktemp)"
  set +e
  DEMO_LAST_HTTP_CODE="$(curl -sS -o "${DEMO_LAST_BODY_FILE}" -w '%{http_code}' --max-time 60 -X PUT "${url}" \
    -H 'content-type: application/json' -d "${json}" 2>/dev/null || echo "000")"
  set +e
}

demo_print_last_response() {
  echo "Ответ: HTTP ${DEMO_LAST_HTTP_CODE}"
  if [[ -n "${DEMO_LAST_BODY_FILE}" && -f "${DEMO_LAST_BODY_FILE}" ]]; then
    if jq -e . <"${DEMO_LAST_BODY_FILE}" >/dev/null 2>&1; then
      jq . <"${DEMO_LAST_BODY_FILE}"
    else
      cat "${DEMO_LAST_BODY_FILE}"
    fi
  fi
}

demo_cleanup_last_response() {
  rm -f "${DEMO_LAST_BODY_FILE}"
  DEMO_LAST_BODY_FILE=""
}
