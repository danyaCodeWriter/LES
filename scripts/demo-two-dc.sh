#!/usr/bin/env bash
#
# Демо: два «ЦОД» — ACTIVE (8080) и STANDBY (8081), один etcd + etcdkeeper (sidecar, общий netns),
# переключение active region через файл без рестарта JVM. См. ./scripts/demo-two-dc.sh help
#
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT

export TENANT="${TENANT:-demo-team}"
export BASE_ACTIVE="${BASE_ACTIVE:-http://127.0.0.1:8080}"
export BASE_STANDBY="${BASE_STANDBY:-http://127.0.0.1:8081}"
# gRPC coordination.v1 (для grpcurl в active-flow / standby-checks)
export GRPC_ACTIVE_ADDR="${GRPC_ACTIVE_ADDR:-127.0.0.1:9090}"
export GRPC_STANDBY_ADDR="${GRPC_STANDBY_ADDR:-127.0.0.1:9091}"

# Логирование GET + Authorization (см. demo_log_get_coordinator_auth в lib).
DEMO_HELPERS="${ROOT}/scripts/lib/demo-step-helpers.sh"
# shellcheck disable=SC1090
[[ -f "${DEMO_HELPERS}" ]] && source "${DEMO_HELPERS}"

COORD_ACTIVE_REGION_FILE="${COORD_ACTIVE_REGION_FILE:-/tmp/coord-demo-active-region}"
readonly COORD_ACTIVE_REGION_FILE

# Два logical региона в демо; в файле в каждый момент времени активен ровно один.
COORD_DEMO_REGION_INITIAL="${COORD_DEMO_REGION_INITIAL:-r1}"
COORD_DEMO_REGION_SWITCH="${COORD_DEMO_REGION_SWITCH:-r2}"
# После switch-region: ждём regionalWritesBlocked=false (истечение TTL старых ключей), до таймаута.
COORD_DEMO_DRAIN_POLL_SEC="${COORD_DEMO_DRAIN_POLL_SEC:-2}"
COORD_DEMO_DRAIN_TIMEOUT_SEC="${COORD_DEMO_DRAIN_TIMEOUT_SEC:-150}"

ETCD_CONTAINER_NAME="${ETCD_CONTAINER_NAME:-etcd-coord-demo}"
# UI — sidecar к etcd (см. etcd-start); в форме подключения: http://127.0.0.1:2379
ETCD_UI_CONTAINER_NAME="${ETCD_UI_CONTAINER_NAME:-etcdkeeper-coord-demo}"
ETCD_UI_HOST_PORT="${ETCD_UI_HOST_PORT:-8099}"
ETCD_UI_IMAGE="${ETCD_UI_IMAGE:-evildecay/etcdkeeper:latest}"
# Таймаут gRPC-клиента etcdkeeper к etcd (сек); дефолт в бинаре 5 — на медленном Docker часто «context deadline exceeded».
ETCD_UI_CLIENT_TIMEOUT="${ETCD_UI_CLIENT_TIMEOUT:-30}"
COORD_DEMO_DOCKER_NETWORK="${COORD_DEMO_DOCKER_NETWORK:-coord-demo-net}"

SPRING_ACTIVE_PID_FILE="${SPRING_ACTIVE_PID_FILE:-/tmp/coord-demo-spring-active.pid}"
SPRING_STANDBY_PID_FILE="${SPRING_STANDBY_PID_FILE:-/tmp/coord-demo-spring-standby.pid}"
SPRING_ACTIVE_LOG="${SPRING_ACTIVE_LOG:-/tmp/coord-demo-spring-active.log}"
SPRING_STANDBY_LOG="${SPRING_STANDBY_LOG:-/tmp/coord-demo-spring-standby.log}"

die() {
  echo "error: $*" >&2
  exit 1
}

need_curl() {
  command -v curl >/dev/null 2>&1 || die "curl not found"
}

need_jq() {
  command -v jq >/dev/null 2>&1 || die "jq not found (install for readable JSON)"
}

need_docker() {
  command -v docker >/dev/null 2>&1 || die "docker not found"
}

need_mvn() {
  command -v mvn >/dev/null 2>&1 || die "mvn not found (install Apache Maven or use SDKMAN)"
}

# Выставляет JAVA_HOME, если пустой: macOS — /usr/libexec/java_home; иначе — java -XshowSettings.
ensure_java_home() {
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
    echo "JAVA_HOME=$JAVA_HOME (уже задан)"
    return 0
  fi
  local os jh props_home
  os="$(uname -s 2>/dev/null || echo unknown)"
  if [[ "$os" == "Darwin" ]] && [[ -x "/usr/libexec/java_home" ]]; then
    for v in 21 22 23 24; do
      if jh="$(/usr/libexec/java_home -v "$v" 2>/dev/null)"; then
        export JAVA_HOME="$jh"
        echo "JAVA_HOME=$JAVA_HOME (macOS, Java $v)"
        return 0
      fi
    done
    if jh="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
      export JAVA_HOME="$jh"
      echo "JAVA_HOME=$JAVA_HOME (macOS, Java 17 — для этого репо нужен 21+, см. ошибку при mvn)"
      return 0
    fi
    jh="$(/usr/libexec/java_home 2>/dev/null)" || true
    if [[ -n "$jh" ]]; then
      export JAVA_HOME="$jh"
      echo "JAVA_HOME=$JAVA_HOME (macOS, default java_home)"
      return 0
    fi
  fi
  if command -v java >/dev/null 2>&1; then
    props_home="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java\.home = //p' | head -1)"
    if [[ -n "$props_home" && -x "$props_home/bin/java" ]]; then
      export JAVA_HOME="$props_home"
      echo "JAVA_HOME=$JAVA_HOME (из java -XshowSettings)"
      return 0
    fi
    if [[ -n "$props_home" ]]; then
      export JAVA_HOME="$props_home"
      echo "JAVA_HOME=$JAVA_HOME (из java -XshowSettings, без bin/java в PATH layout)"
      return 0
    fi
  fi
  die "JAVA_HOME не задан и не удалось определить JDK; установите JDK 21 и выполните: export JAVA_HOME=/path/to/jdk"
}

# Перед запуском Spring: mvn на PATH, JDK 21+, репозиторий в ROOT.
prepare_maven_spring_run() {
  need_mvn
  ensure_java_home
  export PATH="$JAVA_HOME/bin:$PATH"
  local jline tmp jmajor=0
  # Первая строка иногда не та (Picked up _JAVA_OPTIONS); ищем строку с version "…".
  jline="$("$JAVA_HOME/bin/java" -version 2>&1 | grep -E ' version "' | head -n 1 || true)"
  if [[ -z "$jline" ]]; then
    jline="$("$JAVA_HOME/bin/java" -version 2>&1 | head -n 1 || true)"
  fi
  if [[ -n "$jline" ]]; then
    tmp="${jline#*version \"}"
    tmp="${tmp%%\"*}"
    jmajor="${tmp%%.*}"
  fi
  if [[ -z "$jmajor" || "$jmajor" == *[!0-9]* ]]; then
    jmajor=0
  fi
  if [[ "$jmajor" -lt 21 ]]; then
    die "coordination-server собирается с Java 21+ (release 21). Сейчас JAVA_HOME=$JAVA_HOME, первая строка java -version: $jline — установите JDK 21+ или: export JAVA_HOME=\$(/usr/libexec/java_home -v 21)"
  fi
  cd "$ROOT" || die "cannot cd to $ROOT"
}

# coordination-server зависит от coordination-hash; при запуске по server/pom.xml sibling не в реакторе — ставим jar в ~/.m2.
install_coordination_hash_local() {
  mvn -q -f "$ROOT/pom.xml" -pl coordination-hash install -DskipTests
}

# Spring Boot accepts comma- or space-separated args, but Maven splits -D values on comma,
# so use spaces here or server.port becomes "8081,--grpc..." and the app fails to start.
spring_run_args_active() {
  echo "--coordination.active-region.file-path=${COORD_ACTIVE_REGION_FILE} --coordination.active-region.poll-interval-ms=2000"
}

spring_run_args_standby() {
  echo "--server.port=8081 --grpc.server.port=9091 --coordination.active-region.file-path=${COORD_ACTIVE_REGION_FILE} --coordination.active-region.poll-interval-ms=2000"
}

# Spring + etcd: агрегатный /actuator/health = UP (иначе coordinator/worker/job вернут 5xx и jq в active-flow «сыпется»).
require_active_healthy() {
  need_curl
  need_jq
  local body st
  if ! body="$(curl -sfS --max-time 8 "$BASE_ACTIVE/actuator/health" 2>/dev/null)"; then
    die "Нет ответа GET $BASE_ACTIVE/actuator/health (часто: coordination-server не слушает 8080, или HTTP 503 — etcd недоступен для сервера).

Сделайте из корня репозитория по порядку:
  $(basename "$0") etcd-start
  $(basename "$0") init-file
  $(basename "$0") run-active
(или фон: $(basename "$0") start-active-bg && tail -f $SPRING_ACTIVE_LOG)

Проверка etcd с хоста: curl -sS http://127.0.0.1:2379/version"
  fi
  st="$(echo "$body" | jq -r '.status // empty')"
  if [[ "$st" != "UP" ]]; then
    die "ACTIVE: actuator health status='$st' (нужен UP). Ответ: $body

Чаще всего не запущен Docker-etcd (etcd-start) или порт 2379 занят другим процессом."
  fi
}

require_standby_healthy() {
  need_curl
  need_jq
  local body st
  if ! body="$(curl -sfS --max-time 8 "$BASE_STANDBY/actuator/health" 2>/dev/null)"; then
    die "Нет ответа GET $BASE_STANDBY/actuator/health — инстанс STANDBY не поднят на 8081 или health не UP.

Сначала: $(basename "$0") etcd-start && $(basename "$0") init-file, затем в отдельном терминале $(basename "$0") run-standby (или start-standby-bg)."
  fi
  st="$(echo "$body" | jq -r '.status // empty')"
  if [[ "$st" != "UP" ]]; then
    die "STANDBY: actuator health status='$st'. Ответ: $body"
  fi
}

cmd_help() {
  cat <<EOF
Использование: $(basename "$0") <команда>

Команды:
  help              Эта справка + как поднять два сервера
  (отдельно)        Пошаговые демо с паузой DEMO_STEP_PAUSE_SEC: demo-step-happy-path.sh,
                    demo-step-workers-and-keys.sh (GET worker jobs + global KV + очистка etcd),
                    demo-step-sse-workers-jobs.sh (два SSE: workers + jobs, отдельный терминал),
                    demo-step-bench-rest.sh (замер ms основных REST), demo-step-bench-grpc.sh (gRPC + kv GET по REST),
                    demo-step-job-lock-contention.sh, demo-step-standby-regional-denied.sh
                    (см. docs/DEMO_PLAN_RU.md)
  init-file         Записать в файл активный регион (env COORD_DEMO_REGION_INITIAL, по умолчанию r1)
  etcd-start        Запустить etcd + etcdkeeper UI в Docker (etcd :2379, UI см. ниже)
  etcd-stop         Остановить UI, etcd и docker-сеть демо
  etcd-verify       Проверить: etcd на localhost:2379 и доступ из контейнера UI к http://<etcd>:2379
  active-flow       Полный REST на ACTIVE + опционально grpcurl ComputeRanges (**сначала** etcd-start, init-file, run-active)
  switch-region     Переключить активный регион (env COORD_DEMO_REGION_SWITCH, по умолчанию r2)
  active-after-switch  Короткая реакция клиента после switch (ACTIVE)
  standby-checks    GET + 403 на active-region мутации + global на STANDBY (нужны etcd, init-file, ACTIVE+STANDBY)
  sse-active-region Поток SSE с ACTIVE (блокирует терминал)
  sse-workers-active Поток workers с ACTIVE (блокирует)
  java-home         Показать/выставить JAVA_HOME и выйти (для отладки)
  run-active        Запуск coordination-server в ACTIVE (foreground; JDK 21+; hash install + mvn -f coordination-server/pom.xml spring-boot:run)
  run-standby       Запуск в STANDBY на :8081 / gRPC :9091 (foreground)
  start-active-bg   То же ACTIVE в фоне (лог $SPRING_ACTIVE_LOG, pid $SPRING_ACTIVE_PID_FILE)
  start-standby-bg  То же STANDBY в фоне (лог $SPRING_STANDBY_LOG)
  stop-apps         Остановить фоновые Spring-процессы (pid-файлы + порты 8080/8081)

Переменные окружения:
  TENANT, BASE_ACTIVE, BASE_STANDBY, GRPC_ACTIVE_ADDR, GRPC_STANDBY_ADDR, COORD_ACTIVE_REGION_FILE
  ETCD_CONTAINER_NAME, ETCD_UI_CONTAINER_NAME, ETCD_UI_HOST_PORT, ETCD_UI_IMAGE
  COORD_DEMO_DOCKER_NETWORK
  ETCD_UI_OPEN_BROWSER=1 — после etcd-start открыть UI в браузере (macOS open)
  ETCD_UI_CLIENT_TIMEOUT — таймаут etcd-клиента внутри etcdkeeper (сек, по умолчанию 30)
  SPRING_ACTIVE_PID_FILE, SPRING_STANDBY_PID_FILE, SPRING_ACTIVE_LOG, SPRING_STANDBY_LOG
  COORD_DEMO_REGION_INITIAL (по умолчанию r1), COORD_DEMO_REGION_SWITCH (по умолчанию r2)

Файл active region (по умолчанию $COORD_ACTIVE_REGION_FILE):
  Оба инстанса сервера должны быть запущены с одинаковым
  --coordination.active-region.file-path=... и без выставленного COORDINATION_ACTIVE_REGION,
  чтобы переключение делалось только записью в файл.

Поднять два сервера:

  Вариант A — два терминала (удобно для логов на экране):
    Терминал 1: $(basename "$0") run-active
    Терминал 2: $(basename "$0") run-standby
    (скрипт cd в $ROOT, JAVA_HOME 21+, install coordination-hash, затем spring-boot:run по coordination-server/pom.xml)

  Вариант B — оба в фоне из одного терминала:
    $(basename "$0") start-active-bg && $(basename "$0") start-standby-bg
    логи: tail -f $SPRING_ACTIVE_LOG   и   tail -f $SPRING_STANDBY_LOG
    остановка: $(basename "$0") stop-apps

Порядок демо:
  1) $(basename "$0") etcd-start && $(basename "$0") init-file
     После etcd-start: Etcd UI → etcd v3 → адрес http://127.0.0.1:2379 (sidecar с etcd)
  2) Поднять ACTIVE и STANDBY (run-active / run-standby или start-*-bg)
  3) $(basename "$0") active-flow
  4) В другом терминале: $(basename "$0") sse-active-region
  5) $(basename "$0") switch-region && $(basename "$0") active-after-switch
  6) $(basename "$0") standby-checks
EOF
}

cmd_init_file() {
  printf '%s\n' "$COORD_DEMO_REGION_INITIAL" > "$COORD_ACTIVE_REGION_FILE"
  echo "wrote active region $COORD_DEMO_REGION_INITIAL -> $COORD_ACTIVE_REGION_FILE"
}

cmd_etcd_verify() {
  need_docker
  echo "=== etcd с хоста (http://127.0.0.1:2379/version) ==="
  if command -v curl >/dev/null 2>&1; then
    curl -sS --max-time 3 "http://127.0.0.1:2379/version" && echo "" || echo "(curl failed)"
  else
    echo "(install curl for this check)"
  fi
  echo ""
  if ! docker ps --format '{{.Names}}' | grep -qx "$ETCD_UI_CONTAINER_NAME"; then
    echo "контейнер $ETCD_UI_CONTAINER_NAME не запущен — сначала: $0 etcd-start"
    return 1
  fi
  echo "=== из контейнера $ETCD_UI_CONTAINER_NAME → http://127.0.0.1:2379/version (sidecar = тот же netns, что etcd) ==="
  if docker exec "$ETCD_UI_CONTAINER_NAME" wget -qO- -T 5 "http://127.0.0.1:2379/version" 2>/dev/null; then
    echo ""
    echo "OK: etcdkeeper в одном network namespace с etcd; в UI укажите http://127.0.0.1:2379"
  else
    echo ""
    echo "FAIL: из UI-контейнера не виден etcd на 127.0.0.1:2379. Перезапустите: $0 etcd-stop && $0 etcd-start"
    return 1
  fi
}

cmd_etcd_start() {
  need_docker
  local c
  for c in "$ETCD_CONTAINER_NAME" "$ETCD_UI_CONTAINER_NAME"; do
    if docker ps -a --format '{{.Names}}' | grep -qx "$c"; then
      echo "container $c already exists; remove with: $0 etcd-stop" >&2
      exit 1
    fi
  done

  if ! docker network inspect "$COORD_DEMO_DOCKER_NETWORK" >/dev/null 2>&1; then
    docker network create "$COORD_DEMO_DOCKER_NETWORK"
    echo "created docker network: $COORD_DEMO_DOCKER_NETWORK"
  fi

  # Порты UI (8099→8080) пробрасываем на контейнер etcd: etcdkeeper крутится sidecar'ом в том же network namespace.
  docker run -d \
    --name "$ETCD_CONTAINER_NAME" \
    --network "$COORD_DEMO_DOCKER_NETWORK" \
    -p 2379:2379 \
    -p "${ETCD_UI_HOST_PORT}:8080" \
    quay.io/coreos/etcd:v3.5.15 \
    /usr/local/bin/etcd \
    --name etcd0 \
    --advertise-client-urls http://127.0.0.1:2379 \
    --listen-client-urls http://0.0.0.0:2379 \
    --initial-advertise-peer-urls http://0.0.0.0:2380 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --initial-cluster etcd0=http://0.0.0.0:2380 \
    --initial-cluster-state new \
    --initial-cluster-token coord-demo

  # Sidecar: общий netns с etcd → в UI адрес http://127.0.0.1:2379 (иначе gRPC после member list часто «deadline exceeded»).
  docker run -d \
    --name "$ETCD_UI_CONTAINER_NAME" \
    --network "container:$ETCD_CONTAINER_NAME" \
    -w /opt/etcdkeeper \
    --entrypoint /bin/sh \
    "$ETCD_UI_IMAGE" \
    -c "exec ./etcdkeeper.bin -h 0.0.0.0 -p 8080 -timeout ${ETCD_UI_CLIENT_TIMEOUT}"

  echo ""
  echo "etcd (для Spring на хосте): http://127.0.0.1:2379  (контейнер $ETCD_CONTAINER_NAME)"
  echo "Etcd UI (etcdkeeper):        http://127.0.0.1:${ETCD_UI_HOST_PORT}/etcdkeeper"
  echo ""
  echo "В UI: версия etcd v3, адрес (sidecar — localhost это тот же процесс netns, что и etcd):"
  echo "  http://127.0.0.1:2379"
  echo ""
  echo "Префикс ключей сервиса в дереве: /coord/v1/tenants"
  echo ""
  sleep 1
  cmd_etcd_verify || true
  echo ""

  if [[ "${ETCD_UI_OPEN_BROWSER:-}" == "1" ]] && command -v open >/dev/null 2>&1; then
    open "http://127.0.0.1:${ETCD_UI_HOST_PORT}/etcdkeeper" 2>/dev/null || true
  fi
}

cmd_etcd_stop() {
  need_docker
  docker rm -f "$ETCD_UI_CONTAINER_NAME" "$ETCD_CONTAINER_NAME" 2>/dev/null || true
  docker network rm "$COORD_DEMO_DOCKER_NETWORK" 2>/dev/null || true
  echo "removed containers $ETCD_UI_CONTAINER_NAME, $ETCD_CONTAINER_NAME (if existed)"
  echo "removed docker network $COORD_DEMO_DOCKER_NETWORK (if existed and empty)"
}

cmd_active_flow() {
  need_curl
  need_jq
  require_active_healthy
  echo "Подсказка: в Etcd UI префикс /coord/v1/tenants появится после первых успешных записей; до этого дерево может быть пустым — это нормально."
  echo ""

  echo "=== active-region snapshot (ACTIVE) ==="
  curl -sS "$BASE_ACTIVE/v1/tenants/$TENANT/active-region" | jq .

  echo "=== coordinator acquire ==="
  local COORD_TOKEN ACQ_JSON
  ACQ_JSON="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d "{\"coordinatorId\":\"payments\",\"instanceId\":\"pod-active-1\",\"ttlSeconds\":300,\"metadata\":{\"logicalRegion\":\"${COORD_DEMO_REGION_INITIAL}\"}}")"
  echo "$ACQ_JSON" | jq .
  COORD_TOKEN="$(echo "$ACQ_JSON" | jq -er .token)" || die "coordinator acquire: нет token в ответе (нужен для renew)"

  echo "=== coordinator renew ==="
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator/renew" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg t "$COORD_TOKEN" '{token:$t}')" | jq .

  echo "=== coordinator GET (Authorization: Bearer coordinator token) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator" "${COORD_TOKEN}"
  fi
  curl -sS -H "Authorization: Bearer $COORD_TOKEN" "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator" | jq .

  echo "=== workers register + heartbeat ==="
  local TOKEN1 TOKEN2
  TOKEN1="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/workers/register" \
    -H 'content-type: application/json' \
    -d '{"workerId":"worker-a","instanceId":"pod-w1","ttlSeconds":300,"metadata":{"range":"a-m"}}' | jq -er .token)" \
    || die "register worker-a failed (см. JSON выше; часто 5xx если etcd не поднят)"
  TOKEN2="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/workers/register" \
    -H 'content-type: application/json' \
    -d '{"workerId":"worker-b","instanceId":"pod-w2","ttlSeconds":300,"metadata":{"range":"n-z"}}' | jq -er .token)" \
    || die "register worker-b failed"
  echo "TOKEN1=$TOKEN1"
  echo "TOKEN2=$TOKEN2"
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/workers/worker-a/heartbeat" \
    -H 'content-type: application/json' -d "{\"token\":\"$TOKEN1\"}" | jq .
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/workers/worker-b/heartbeat" \
    -H 'content-type: application/json' -d "{\"token\":\"$TOKEN2\"}" | jq .

  echo "=== workers/ids + workers list (Bearer coordinator) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/ids" "${COORD_TOKEN}"
  fi
  curl -sS -H "Authorization: Bearer $COORD_TOKEN" "$BASE_ACTIVE/v1/tenants/$TENANT/workers/ids" | jq .
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers" "${COORD_TOKEN}"
  fi
  curl -sS -H "Authorization: Bearer $COORD_TOKEN" "$BASE_ACTIVE/v1/tenants/$TENANT/workers" | jq .

  echo "=== rings/assign ==="
  curl -sS -X POST "$BASE_ACTIVE/v1/rings/assign" \
    -H 'content-type: application/json' \
    -d '{"workerIds":["worker-a","worker-b"],"virtualNodes":128}' | jq .

  if command -v grpcurl >/dev/null 2>&1; then
    echo "=== grpcurl ComputeRanges на ACTIVE $GRPC_ACTIVE_ADDR (те же worker-a, worker-b; без записи в etcd) ==="
    grpcurl -plaintext -d '{"workerIds":["worker-a","worker-b"],"virtualNodes":128}' \
      "$GRPC_ACTIVE_ADDR" coordination.v1.Coordination/ComputeRanges 2>&1 || true
    echo "=== grpcurl ListWorkers (нужен coordinator_read_token = тот же, что COORD_TOKEN) ==="
    echo "Аналог REST GET: поле coordinatorReadToken в JSON = заголовок Authorization: Bearer для HTTP."
    grpcurl -plaintext -d "$(jq -nc --arg t "$TENANT" --arg ct "$COORD_TOKEN" '{tenantId:$t,coordinatorReadToken:$ct}')" \
      "$GRPC_ACTIVE_ADDR" coordination.v1.Coordination/ListWorkers 2>&1 || true
  else
    echo "(пропуск: grpcurl не в PATH — установите для демо gRPC consistent hash; REST rings/assign уже выше)"
  fi

  echo "=== job acquire (holder=worker-a, participantToken from register) ==="
  local demo_job_tok
  demo_job_tok="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/jobs/demo-job-1:acquire" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg h worker-a --arg pt "$TOKEN1" \
      '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{kind:"demo"}}')" | jq -er .token)" \
    || die "job acquire failed (см. JSON выше)"

  echo "=== job renew (token from acquire, expect renewed true) ==="
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/jobs/demo-job-1:renew" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg t "$demo_job_tok" '{token:$t}')" | jq .

  echo "=== job acquire second holder (expect HTTP 409 JOB_EXISTS) ==="
  local _tmp _code
  _tmp="$(mktemp)"
  _code="$(curl -sS -o "$_tmp" -w '%{http_code}' -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/jobs/demo-job-1:acquire" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg h worker-b --arg pt "$TOKEN2" \
      '{holder:$h,participantToken:$pt,ttlSeconds:300,metadata:{}}')")"
  echo "HTTP ${_code}"
  jq . <"$_tmp" 2>/dev/null || cat "$_tmp"
  rm -f "$_tmp"
  [[ "$_code" == "409" ]] || die "expected HTTP 409 for second job acquire, got ${_code}"

  echo "=== job GET (Bearer coordinator) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/jobs/demo-job-1" "${COORD_TOKEN}"
  fi
  curl -sS -H "Authorization: Bearer $COORD_TOKEN" "$BASE_ACTIVE/v1/tenants/$TENANT/jobs/demo-job-1" | jq .

  echo "=== coordinator competitor (expect HTTP 409 COORDINATOR_EXISTS) ==="
  _tmp="$(mktemp)"
  _code="$(curl -sS -o "$_tmp" -w '%{http_code}' -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d '{"coordinatorId":"payments","instanceId":"pod-intruder","ttlSeconds":300,"metadata":{}}')"
  echo "HTTP ${_code}"
  jq . <"$_tmp" 2>/dev/null || cat "$_tmp"
  rm -f "$_tmp"
  [[ "$_code" == "409" ]] || die "expected HTTP 409 for competing coordinator acquire, got ${_code}"

  echo "=== global coordinator acquire (ACTIVE) ==="
  local GLOBAL_COORD_TOK
  GLOBAL_COORD_TOK="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/global/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d '{"coordinatorId":"global-demo","instanceId":"pod-active-1","ttlSeconds":300,"metadata":{}}' | jq -er .token)" \
    || die "global coordinator acquire: no token"
  echo "(global coordinator token acquired)"
  echo "=== global coordinator GET (Bearer global coordinator) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/global/coordinator" "${GLOBAL_COORD_TOK}"
  fi
  curl -sS -H "Authorization: Bearer $GLOBAL_COORD_TOK" "$BASE_ACTIVE/v1/tenants/$TENANT/global/coordinator" | jq .

  echo "=== job release ==="
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/jobs/demo-job-1:release" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg t "$demo_job_tok" '{token:$t}')" | jq .

  echo "done active-flow"
}

wait_regional_writes_unblocked() {
  need_curl
  need_jq
  local deadline=$((SECONDS + COORD_DEMO_DRAIN_TIMEOUT_SEC))
  echo "waiting for GET active-region: regionalWritesBlocked=false (TTL drain of previous region; timeout ${COORD_DEMO_DRAIN_TIMEOUT_SEC}s, poll ${COORD_DEMO_DRAIN_POLL_SEC}s)"
  while true; do
    local blocked
    blocked="$(curl -sS "$BASE_ACTIVE/v1/tenants/$TENANT/active-region" | jq -r '.regionalWritesBlocked // false')"
    if [[ "$blocked" != "true" ]]; then
      echo "regionalWritesBlocked is not true — regional lease mutations allowed on ACTIVE (or field absent)"
      return 0
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      die "timeout: regionalWritesBlocked still true after ${COORD_DEMO_DRAIN_TIMEOUT_SEC}s (release coordinator/workers/jobs in old region or wait for lease TTL, max 300s per entity)"
    fi
    sleep "$COORD_DEMO_DRAIN_POLL_SEC"
  done
}

cmd_switch_region() {
  printf '%s\n' "$COORD_DEMO_REGION_SWITCH" > "$COORD_ACTIVE_REGION_FILE"
  echo "wrote active region $COORD_DEMO_REGION_SWITCH -> $COORD_ACTIVE_REGION_FILE (server picks up via poll-interval)"
  sleep 3
  need_curl
  need_jq
  echo "=== active-region snapshot right after file write (may show regionalWritesBlocked=true) ==="
  curl -sS "$BASE_ACTIVE/v1/tenants/$TENANT/active-region" | jq .
  wait_regional_writes_unblocked
  echo "=== active-region after drain cleared ==="
  curl -sS "$BASE_ACTIVE/v1/tenants/$TENANT/active-region" | jq .
}

cmd_active_after_switch() {
  need_curl
  need_jq
  require_active_healthy
  echo "=== re-acquire coordinator in ${COORD_DEMO_REGION_SWITCH} segment (token for GETs) ==="
  local SWITCH_COORD_TOK
  SWITCH_COORD_TOK="$(curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d "{\"coordinatorId\":\"payments\",\"instanceId\":\"pod-active-1\",\"ttlSeconds\":300,\"metadata\":{\"logicalRegion\":\"${COORD_DEMO_REGION_SWITCH}\"}}" | jq -er .token)" \
    || die "coordinator acquire after switch: no token"

  echo "=== workers/ids in NEW segment (often empty right after switch) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/workers/ids" "${SWITCH_COORD_TOK}"
  fi
  curl -sS -H "Authorization: Bearer $SWITCH_COORD_TOK" "$BASE_ACTIVE/v1/tenants/$TENANT/workers/ids" | jq .

  echo "=== coordinator in new segment ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/coordinator" "${SWITCH_COORD_TOK}"
  fi
  curl -sS -H "Authorization: Bearer $SWITCH_COORD_TOK" "$BASE_ACTIVE/v1/tenants/$TENANT/coordinator" | jq .

  echo "=== register worker in ${COORD_DEMO_REGION_SWITCH} segment ==="
  curl -sS -X POST "$BASE_ACTIVE/v1/tenants/$TENANT/workers/register" \
    -H 'content-type: application/json' \
    -d "{\"workerId\":\"worker-${COORD_DEMO_REGION_SWITCH}-1\",\"instanceId\":\"pod-${COORD_DEMO_REGION_SWITCH}-1\",\"ttlSeconds\":300,\"metadata\":{}}" | jq .
}

http_post_code_body() {
  local url="$1"
  local json="$2"
  local tmp
  tmp="$(mktemp)"
  local code
  code="$(curl -sS -o "$tmp" -w '%{http_code}' -X POST "$url" \
    -H 'content-type: application/json' -d "$json")"
  echo "HTTP $code"
  if command -v jq >/dev/null 2>&1; then
    jq . <"$tmp" 2>/dev/null || cat "$tmp"
  else
    cat "$tmp"
  fi
  rm -f "$tmp"
}

cmd_standby_checks() {
  need_curl
  need_jq
  require_standby_healthy
  require_active_healthy

  echo "=== STANDBY: GET active-region (still public) ==="
  curl -sS "$BASE_STANDBY/v1/tenants/$TENANT/active-region" | jq . 2>/dev/null || curl -sS "$BASE_STANDBY/v1/tenants/$TENANT/active-region"
  echo "=== STANDBY: GET regional coordinator without Bearer (expect 401) ==="
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_STANDBY}/v1/tenants/${TENANT}/coordinator" ""
  fi
  local _sb401 _tmp401
  _tmp401="$(mktemp)"
  _sb401="$(curl -sS -o "$_tmp401" -w '%{http_code}' "$BASE_STANDBY/v1/tenants/$TENANT/coordinator" 2>/dev/null || echo "000")"
  echo "HTTP ${_sb401}"
  cat "$_tmp401" 2>/dev/null | jq . 2>/dev/null || cat "$_tmp401"
  rm -f "$_tmp401"

  echo "=== STANDBY: active-region coordinator acquire (expect 403) ==="
  http_post_code_body "$BASE_STANDBY/v1/tenants/$TENANT/coordinator/acquire" \
    '{"coordinatorId":"evil","instanceId":"pod-standby","ttlSeconds":300,"metadata":{}}'

  echo "=== STANDBY: coordinator renew (expect 403) ==="
  http_post_code_body "$BASE_STANDBY/v1/tenants/$TENANT/coordinator/renew" \
    '{"token":"standby-dummy-token"}'

  echo "=== STANDBY: worker register (expect 403) ==="
  http_post_code_body "$BASE_STANDBY/v1/tenants/$TENANT/workers/register" \
    '{"workerId":"x","instanceId":"y","ttlSeconds":300,"metadata":{}}'

  echo "=== STANDBY: job acquire (expect 403) ==="
  http_post_code_body "$BASE_STANDBY/v1/tenants/$TENANT/jobs/demo-job-2:acquire" \
    '{"holder":"worker-a","participantToken":"dummy","ttlSeconds":300,"metadata":{}}'

  echo "=== STANDBY: job renew (expect 403) ==="
  http_post_code_body "$BASE_STANDBY/v1/tenants/$TENANT/jobs/demo-job-1:renew" \
    '{"token":"standby-dummy-job-token"}'

  echo "=== STANDBY: global coordinator acquire (allowed) ==="
  local SB_GLOBAL_TOK
  SB_GLOBAL_TOK="$(curl -sS -X POST "$BASE_STANDBY/v1/tenants/$TENANT/global/coordinator/acquire" \
    -H 'content-type: application/json' \
    -d '{"coordinatorId":"global-from-standby","instanceId":"pod-standby","ttlSeconds":300,"metadata":{"via":"standby"}}' | jq -er .token)" \
    || { echo "standby global acquire failed" >&2; SB_GLOBAL_TOK=""; }

  echo "=== STANDBY: global coordinator GET (Bearer global token) ==="
  if [[ -n "$SB_GLOBAL_TOK" ]]; then
    if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
      demo_log_get_coordinator_auth "${BASE_STANDBY}/v1/tenants/${TENANT}/global/coordinator" "${SB_GLOBAL_TOK}"
    fi
    curl -sS -H "Authorization: Bearer $SB_GLOBAL_TOK" "$BASE_STANDBY/v1/tenants/$TENANT/global/coordinator" | jq . 2>/dev/null \
      || curl -sS -H "Authorization: Bearer $SB_GLOBAL_TOK" "$BASE_STANDBY/v1/tenants/$TENANT/global/coordinator"
  else
    echo "(skip GET: no token from acquire)"
  fi

  if command -v grpcurl >/dev/null 2>&1; then
    echo "=== grpcurl STANDBY :9091 AcquireCoordinator (expect FAILED_PRECONDITION) ==="
    grpcurl -plaintext -d "{\"tenantId\":\"$TENANT\",\"coordinatorId\":\"grpc-evil\",\"instanceId\":\"sb\",\"ttlSeconds\":300,\"metadata\":{}}" \
      "$GRPC_STANDBY_ADDR" coordination.v1.Coordination/AcquireCoordinator 2>&1 || true
  else
    echo "(skip grpcurl: not installed)"
  fi

  echo "=== STANDBY: stream/workers (expect 403) ==="
  code="$(curl -sS -o /dev/null -w '%{http_code}' -N -H 'Accept: text/event-stream' \
    "$BASE_STANDBY/v1/tenants/$TENANT/stream/workers")"
  echo "HTTP $code"
}

cmd_sse_active_region() {
  need_curl
  echo "SSE (Ctrl+C to stop): $BASE_ACTIVE/v1/tenants/$TENANT/stream/active-region"
  curl -N -H 'Accept: text/event-stream' "$BASE_ACTIVE/v1/tenants/$TENANT/stream/active-region"
}

cmd_sse_workers_active() {
  need_curl
  echo "SSE (Ctrl+C to stop): $BASE_ACTIVE/v1/tenants/$TENANT/stream/workers"
  echo "Нужен coordinator token: export COORD_SSE_BEARER='<token из coordinator acquire>' (иначе 401)."
  if declare -F demo_log_get_coordinator_auth >/dev/null 2>&1; then
    demo_log_get_coordinator_auth "${BASE_ACTIVE}/v1/tenants/${TENANT}/stream/workers" "${COORD_SSE_BEARER:-}"
  fi
  if [[ -z "${COORD_SSE_BEARER:-}" ]]; then
    echo "warning: COORD_SSE_BEARER пуст — запрос вернёт 401 без Bearer" >&2
  fi
  local _h=()
  if [[ -n "${COORD_SSE_BEARER:-}" ]]; then
    _h=(-H "Authorization: Bearer ${COORD_SSE_BEARER}")
  fi
  curl -N -H 'Accept: text/event-stream' "${_h[@]}" "$BASE_ACTIVE/v1/tenants/$TENANT/stream/workers"
}

cmd_java_home() {
  ensure_java_home
  echo "export JAVA_HOME=$(printf %q "$JAVA_HOME")"
}

cmd_run_active() {
  prepare_maven_spring_run
  install_coordination_hash_local
  unset COORDINATION_ACTIVE_REGION || true
  export COORDINATION_DEPLOYMENT_ROLE=ACTIVE
  echo "Starting ACTIVE (Ctrl+C to stop). Repo: $ROOT"
  exec mvn -f "$ROOT/coordination-server/pom.xml" spring-boot:run \
    -Dspring-boot.run.arguments="$(spring_run_args_active)"
}

cmd_run_standby() {
  prepare_maven_spring_run
  install_coordination_hash_local
  unset COORDINATION_ACTIVE_REGION || true
  export COORDINATION_DEPLOYMENT_ROLE=STANDBY
  echo "Starting STANDBY :8081 gRPC :9091 (Ctrl+C to stop). Repo: $ROOT"
  exec mvn -f "$ROOT/coordination-server/pom.xml" spring-boot:run \
    -Dspring-boot.run.arguments="$(spring_run_args_standby)"
}

cmd_start_active_bg() {
  prepare_maven_spring_run
  if [[ -f "$SPRING_ACTIVE_PID_FILE" ]] && kill -0 "$(cat "$SPRING_ACTIVE_PID_FILE")" 2>/dev/null; then
    die "ACTIVE уже запущен (pid $(cat "$SPRING_ACTIVE_PID_FILE")); сначала: $0 stop-apps"
  fi
  nohup env \
    JAVA_HOME="$JAVA_HOME" \
    PATH="$PATH" \
    ROOT="$ROOT" \
    COORD_ACTIVE_REGION_FILE="$COORD_ACTIVE_REGION_FILE" \
    bash -c '
      set -euo pipefail
      cd "$ROOT"
      mvn -q -f "$ROOT/pom.xml" -pl coordination-hash install -DskipTests
      unset COORDINATION_ACTIVE_REGION || true
      export COORDINATION_DEPLOYMENT_ROLE=ACTIVE
      exec mvn -f "$ROOT/coordination-server/pom.xml" spring-boot:run \
        -Dspring-boot.run.arguments="--coordination.active-region.file-path=${COORD_ACTIVE_REGION_FILE} --coordination.active-region.poll-interval-ms=2000"
    ' >"$SPRING_ACTIVE_LOG" 2>&1 &
  echo $! >"$SPRING_ACTIVE_PID_FILE"
  echo "ACTIVE Spring Boot в фоне, pid=$(cat "$SPRING_ACTIVE_PID_FILE"), лог: $SPRING_ACTIVE_LOG"
}

cmd_start_standby_bg() {
  prepare_maven_spring_run
  if [[ -f "$SPRING_STANDBY_PID_FILE" ]] && kill -0 "$(cat "$SPRING_STANDBY_PID_FILE")" 2>/dev/null; then
    die "STANDBY уже запущен (pid $(cat "$SPRING_STANDBY_PID_FILE")); сначала: $0 stop-apps"
  fi
  nohup env \
    JAVA_HOME="$JAVA_HOME" \
    PATH="$PATH" \
    ROOT="$ROOT" \
    COORD_ACTIVE_REGION_FILE="$COORD_ACTIVE_REGION_FILE" \
    bash -c '
      set -euo pipefail
      cd "$ROOT"
      mvn -q -f "$ROOT/pom.xml" -pl coordination-hash install -DskipTests
      unset COORDINATION_ACTIVE_REGION || true
      export COORDINATION_DEPLOYMENT_ROLE=STANDBY
      exec mvn -f "$ROOT/coordination-server/pom.xml" spring-boot:run \
        -Dspring-boot.run.arguments="--server.port=8081 --grpc.server.port=9091 --coordination.active-region.file-path=${COORD_ACTIVE_REGION_FILE} --coordination.active-region.poll-interval-ms=2000"
    ' >"$SPRING_STANDBY_LOG" 2>&1 &
  echo $! >"$SPRING_STANDBY_PID_FILE"
  echo "STANDBY Spring Boot в фоне, pid=$(cat "$SPRING_STANDBY_PID_FILE"), лог: $SPRING_STANDBY_LOG"
}

cmd_stop_apps() {
  local f pid pids
  for f in "$SPRING_ACTIVE_PID_FILE" "$SPRING_STANDBY_PID_FILE"; do
    if [[ -f "$f" ]]; then
      pid="$(cat "$f" 2>/dev/null)" || true
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 1
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
      fi
      rm -f "$f"
    fi
  done
  if command -v lsof >/dev/null 2>&1; then
    for p in 8080 8081; do
      pids="$(lsof -ti ":$p" 2>/dev/null || true)"
      if [[ -n "$pids" ]]; then
        # shellcheck disable=SC2086
        kill $pids 2>/dev/null || true
      fi
    done
  fi
  echo "stop-apps: остановлены pid-файлы и процессы на портах 8080/8081 (если были)"
}

main() {
  local cmd="${1:-help}"
  shift || true
  case "$cmd" in
    help|-h|--help) cmd_help ;;
    init-file) cmd_init_file ;;
    etcd-start) cmd_etcd_start ;;
    etcd-stop) cmd_etcd_stop ;;
    etcd-verify) cmd_etcd_verify ;;
    active-flow) cmd_active_flow ;;
    switch-region) cmd_switch_region ;;
    active-after-switch) cmd_active_after_switch ;;
    standby-checks) cmd_standby_checks ;;
    sse-active-region) cmd_sse_active_region ;;
    sse-workers-active) cmd_sse_workers_active ;;
    java-home) cmd_java_home ;;
    run-active) cmd_run_active ;;
    run-standby) cmd_run_standby ;;
    start-active-bg) cmd_start_active_bg ;;
    start-standby-bg) cmd_start_standby_bg ;;
    stop-apps) cmd_stop_apps ;;
    *) die "unknown command: $cmd (try: help)" ;;
  esac
}

main "$@"
