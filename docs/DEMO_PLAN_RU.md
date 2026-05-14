# Демо coordination (план по терминалам)

Корень репо: каталог с `pom.xml`. Tenant по умолчанию: **`demo-team`**. Инфраструктура: [`scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh). Префикс в etcd: **`/coord/v1/tenants`**.

Порты: **2379** etcd, **8099** Etcd UI, **8080** / **9090** ACTIVE, **8081** / **9091** STANDBY. Нужны **curl** и **jq**; для шага gRPC в happy path — **grpcurl** (опционально).

### GET и токен координатора (что видно в логах)

Чтение **regional** данных лидера (`GET …/coordinator`, `…/workers`, `…/workers/ids`, `…/jobs/{id}`, SSE `stream/workers` и `stream/jobs`, а также **global** coordinator) **без** заголовка **`Authorization: Bearer`** (или **`X-Coordinator-Token`**) с секретом из **POST …/coordinator/acquire** / **global/coordinator/acquire** в том же контуре сервер не отдаёт: ответ **401** и код **`COORDINATOR_READ_TOKEN_REQUIRED`**. Исключение: **`GET …/v1/tenants/{tenantId}/active-region`** — без Bearer (это снимок конфигурации active region, не tenant pool в etcd).

В пошаговых скриптах (`demo-step-*.sh`) и в **`active-flow` / `standby-checks` / `sse-workers-active` / `demo-step-sse-workers-jobs.sh`** в консоль перед соответствующим запросом выводится правило и строка **`Заголовок запроса: Authorization: Bearer …`** (если токен передан) либо явное **«нет Authorization»** для демонстрации 401.

---

## Терминалы: подготовка

| Терминал | Что делать |
|----------|------------|
| **T0** | `cd <корень-репо>` → `./scripts/demo-two-dc.sh etcd-start` → `./scripts/demo-two-dc.sh init-file` |
| **T1** | Из **корня репо**: `./scripts/demo-two-dc.sh run-active` — оставить до конца демо (**Ctrl+C** остановит процесс). Нужен **JDK 21+**; скрипт ставит `coordination-hash` в `~/.m2` и запускает `spring-boot:run` по `coordination-server/pom.xml` (не через корневой `pom.xml` + `-pl`, иначе Maven сначала бьёт в aggregator). |
| **T2** | `./scripts/demo-two-dc.sh run-standby` — так же (второй терминал) |
| **T3** | Пошаговые сценарии (happy path, workers+KV, bench, job lock, standby — см. ниже) |
| **T4** | (опционально) **SSE** workers + jobs параллельно с T3: `scripts/demo-step-sse-workers-jobs.sh` (нужен `COORD_SSE_BEARER`, см. §1d) |

**Вместо T1+T2 в foreground:** `./scripts/demo-two-dc.sh start-active-bg` и `start-standby-bg`, логи `tail -f /tmp/coord-demo-spring-active.log`. Остановка: `./scripts/demo-two-dc.sh stop-apps`.

**Etcd UI:** `http://127.0.0.1:8099/etcdkeeper` → **v3** → адрес **`http://127.0.0.1:2379`**.

**Конец:** T1/T2 **Ctrl+C** (или `stop-apps`) → T0: `./scripts/demo-two-dc.sh etcd-stop`.

---

## Терминал T3: пошаговые сценарии (пауза между шагами)

Пауза между шагами задаётся переменной **`DEMO_STEP_PAUSE_SEC`** (по умолчанию **20** секунд). Перед любым скриптом можно выставить базы и tenant:

```bash
export DEMO_STEP_PAUSE_SEC=20
export TENANT=demo-team
export BASE_ACTIVE=http://127.0.0.1:8080
export BASE_STANDBY=http://127.0.0.1:8081
```

Сделайте скрипты исполняемыми один раз: `chmod +x scripts/demo-step-*.sh`

### 1) Happy path (весь спектр REST на ACTIVE)

```bash
./scripts/demo-step-happy-path.sh
```

Для каждого шага в консоль выводятся: **какой метод/URL и тело запроса**; для **GET** к защищённым путям — **правило про Bearer** и строка **`Заголовок запроса: Authorization: Bearer <token>`** (или пометка, что заголовка нет, для публичного `active-region`); затем **HTTP-код и JSON ответа** → **краткое пояснение** → пауза `DEMO_STEP_PAUSE_SEC` → следующий шаг.

Охват: `active-region`, coordinator acquire/renew/get (GET с `Authorization: Bearer` = token acquire), два воркера (register → heartbeat → ids/list с тем же Bearer), `rings/assign`, опционально **grpcurl ListWorkers** с `coordinatorReadToken`, job acquire (holder = workerId, `participantToken` из register) / renew / get, global coordinator acquire/get, job release, deregister обоих воркеров.

Рекомендация: для предсказуемого `acquired=true` у coordinator используйте **новый `TENANT=...`** или чистый префикс в etcd; иначе предыдущий лидер может ещё держать lease.

### 1b) Job'ы воркера (GET …/jobs) + global KV ticket

```bash
./scripts/demo-step-workers-and-keys.sh
```

В консоли **только** два блока API: **GET …/workers/{workerId}/jobs** (заголовок `X-Worker-Token`) и **PUT / GET / DELETE …/global/kv/{keyId}**. Подготовка (coordinator, register worker, job acquire) выполняется **без** пошагового вывода. В конце — тихий release/deregister и удаление префикса `${COORD_ETCD_ROOT_PREFIX}/${TENANT}/` через **etcdctl** (если `DEMO_STEP_ETCD_DELETE_TENANT=1` и `etcdctl` в `PATH`; отключить: `DEMO_STEP_ETCD_DELETE_TENANT=0`).

### 1c) Замер латентности REST (ms)

Пауз между итерациями нет. Переменные: **`BENCH_ITERATIONS`** (по умолчанию 15), **`BENCH_WARMUP`** (разогрев, по умолчанию 3).

```bash
BENCH_ITERATIONS=25 BENCH_WARMUP=5 ./scripts/demo-step-bench-rest.sh
```

Печатается строка на итерацию: **coord GET**, **coordinator renew**, **workers/ids**, **workers**, **job GET**, **job renew**, **kv GET** — всё в миллисекундах (`curl -w '%{time_total}'`). В конце — среднее / min / max по каждому API. После замера скрипт делает release job, deregister worker, DELETE KV.

**Замер gRPC** (рядом, для сравнения): `chmod +x scripts/demo-step-bench-grpc.sh`, в `PATH` нужен **grpcurl**. Подготовка тех же сущностей через REST; замер — `grpcurl` + bash **`time`** / **`TIMEFORMAT`** (wall-clock одного вызова; без отдельного **python3** на каждый RPC — иначе в цифру попадал бы старт интерпретатора). RPC к `coordination.v1.Coordination` (флаги `-import-path …/proto -proto coordination/v1/coordination.proto`). Колонки 3–4 — два подряд **`ListWorkers`** (отдельного RPC «только id», как у REST `workers/ids`, в proto нет). Колонка **`kv_GET_REST`** — тот же REST GET global KV (в текущем proto нет gRPC для ticket-KV).

```bash
./scripts/demo-step-bench-grpc.sh
```

### 1d) SSE: потоки `stream/workers` и `stream/jobs` (отдельный терминал)

Пока в **T3** идут happy path / другие сценарии, в **четвёртом** терминале можно смотреть **два** SSE-потока сразу (строки с префиксами `[workers]` и `[jobs]`). Нужен тот же **`Authorization: Bearer`**, что для GET списков — токен **POST …/coordinator/acquire** (active region) или **global** acquire.

```bash
export TENANT=demo-team
export BASE_ACTIVE=http://127.0.0.1:8080
export COORD_SSE_BEARER='<token regional coordinator acquire>'
chmod +x scripts/demo-step-sse-workers-jobs.sh
./scripts/demo-step-sse-workers-jobs.sh
```

Глобальные потоки (`…/global/stream/workers` и `…/global/stream/jobs`):

```bash
export DEMO_SSE_SCOPE=global
export GLOBAL_SSE_BEARER='<token global coordinator acquire>'
./scripts/demo-step-sse-workers-jobs.sh
```

Остановка: **Ctrl+C** (обрывает оба `curl`). Аналог одной команды: `./scripts/demo-two-dc.sh sse-workers-active` (только workers).

### 2) Конкуренция за job + неверный TTL

```bash
./scripts/demo-step-job-lock-contention.sh
```

- Сначала регистрируются два воркера; первый вызов **захватывает** job с их токенами.
- Второй — тот же `jobId`, другой валидный участник: ожидается **HTTP 409** (`JOB_EXISTS`, в теле — сообщение и часто `currentHolder`).
- Затем запрос с **`ttlSeconds: 5`** (ниже минимума 10): ожидается **HTTP 400** (валидация).
- В конце — **release** первым участником; затем **второй** с тем же `jobId` снова **acquire** (успех), **GET job** с Bearer coordinator, **release** вторым токеном лока.

### 3) STANDBY (stand-in): regional запрещён, global разрешён

```bash
./scripts/demo-step-standby-regional-denied.sh
```

Проверяется, что на **8081** regional **coordinator / workers / jobs** дают **403** с телом **`STANDBY_DEPLOYMENT`** (смысл: вы вызываете active-region API, но процесс поднят как пассивный ЦОД в **r2** — мутации regional нужно слать на **ACTIVE**), а **global coordinator** — успешно. В JSON сценария в `metadata` указано **`region: "r2"`** (stand-in), не r1.

---

## Дополнительно (без пошаговой паузы)

В [`scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh): `active-flow` (всё подряд), `switch-region`, `active-after-switch`, `standby-checks`, SSE и т.д. Перед защищёнными GET в лог печатается тот же блок про **Authorization: Bearer** и полный токен, что и в пошаговых скриптах. См. `./scripts/demo-two-dc.sh help`.

**TTL** для coordinator / worker register / job acquire: **10–300** секунд. После **смены active region** смотрите **`regionalWritesBlocked`** в `GET …/active-region` (drain по TTL старого региона).

---

## Сбои (коротко)

| Симптом | Действие |
|---------|----------|
| Скрипт не стартует | `chmod +x scripts/demo-step-*.sh` |
| Нет ответа от ACTIVE | `etcd-start`, `init-file`, `run-active`, проверка `curl -sS http://127.0.0.1:8080/actuator/health` |
| Шаг STANDBY падает на health | Запустите `run-standby` на **8081** |
| **HTTP 409** на первом шаге happy path (лидер / job / worker уже есть) | Другой tenant или дождаться TTL старого lease |
| **HTTP 401** на `GET …/coordinator`, workers, jobs, SSE workers/jobs | Сначала **POST …/coordinator/acquire** в этом контуре, затем **`Authorization: Bearer <token>`** из ответа (в логах шага это печатается явно перед `curl`) |

Подробности: [`README.md`](../README.md).
