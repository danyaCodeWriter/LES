# Демо coordination (план по терминалам)

Корень репо: каталог с `pom.xml`. Tenant по умолчанию: **`demo-team`**. Инфраструктура: [`scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh). Префикс в etcd: **`/coord/v1/tenants`**.

Порты: **2379** etcd, **8099** Etcd UI, **8090** RSS mock, **8080** / **9090** ACTIVE (home **r1**), **8081** / **9091** stand_in (home **r2**). Нужны **curl** и **jq**; для шага gRPC в happy path — **grpcurl** (опционально). Инфра: [`docker-compose.yml`](../docker-compose.yml) (`etcd` + `etcdkeeper` + `rss-mock`).

### Модель 2 ЦОД (что показать ведущему)

| | ACTIVE `:8080` | stand_in `:8081` |
|--|----------------|------------------|
| Home LES (`GET …/active-region` → `region`) | обычно **r1** | обычно **r2** |
| Regional coordinator / workers / **jobs** | ок при `status=active` | **403** `STANDBY_DEPLOYMENT` |
| **Global** coordinator / workers / jobs | ок | ок |
| etcd POV ключ | `/coord/v1/cluster/regions/{home}/activeRegion` | свой `{home}`, не общий |

**Job dual-DC (сказать вслух):** regional job = один владелец в активном сегменте; на stand_in — 403; после switch — новый acquire. Нужна exclusivity на оба ЦОД → **`…/global/jobs/...`** (drain не рвёт). Полная таблица: [`PLATFORM_OVERVIEW_RU.md`](PLATFORM_OVERVIEW_RU.md) §5.3, [`README.md`](../README.md) §«Задачи / блокировки».

**Playbook клиента:** `GET …/active-region` → если `status != active` не шлём regional mutate; если `regionalWritesBlocked` ждём drain; выбор regional vs global job.

### GET и токен координатора (что видно в логах)

Чтение **regional** данных лидера (`GET …/coordinator`, `…/workers`, `…/workers/ids`, `…/jobs/{id}`, SSE `stream/workers` и `stream/jobs`, а также **global** coordinator) **без** заголовка **`Authorization: Bearer`** (или **`X-Coordinator-Token`**) с секретом из **POST …/coordinator/acquire** / **global/coordinator/acquire** в том же контуре сервер не отдаёт: ответ **401** и код **`COORDINATOR_READ_TOKEN_REQUIRED`**. Исключение: **`GET …/v1/tenants/{tenantId}/active-region`** — без Bearer (это снимок конфигурации active region, не tenant pool в etcd).

В пошаговых скриптах (`demo-step-*.sh`) и в **`active-flow` / `standby-checks` / `sse-workers-active` / `demo-step-sse-workers-jobs.sh`** в консоль перед соответствующим запросом выводится правило и строка **`Заголовок запроса: Authorization: Bearer …`** (если токен передан) либо явное **«нет Authorization»** для демонстрации 401.

---

## Терминалы: подготовка

| Терминал | Что делать |
|----------|------------|
| **T0** | `cd <корень-репо>` → `./scripts/demo-two-dc.sh etcd-start` → `./scripts/demo-two-dc.sh init-file` |
| **T0 (RSS)** | Для шага RSS: ещё `./scripts/demo-two-dc.sh rss-start` (или сразу `infra-up`) |
| **T1** | Из **корня репо**: `./scripts/demo-two-dc.sh run-active` — оставить до конца демо (**Ctrl+C** остановит процесс). Нужен **JDK 21+**; скрипт ставит `coordination-hash` в `~/.m2` и запускает `spring-boot:run` по `coordination-server/pom.xml` (не через корневой `pom.xml` + `-pl`, иначе Maven сначала бьёт в aggregator). Для RSS-демо: **`run-active-rss`**. |
| **T2** | `./scripts/demo-two-dc.sh run-standby` — так же (второй терминал) |
| **T3** | Пошаговые сценарии (happy path, workers+KV, bench, job lock, standby, **RSS** — см. ниже) |
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

Охват: `active-region` (`region`=home, `activeRegion`, `status`), coordinator acquire/renew/get (GET с `Authorization: Bearer` = token acquire), два воркера (register → heartbeat → ids/list с тем же Bearer), `rings/assign`, опционально **grpcurl ListWorkers** с `coordinatorReadToken`, **regional** job acquire (holder = workerId, `participantToken` из register) / renew / get, global coordinator acquire/get, job release, deregister обоих воркеров.

Рекомендация: для предсказуемого `acquired=true` у coordinator используйте **новый `TENANT=...`** или чистый префикс в etcd; иначе предыдущий лидер может ещё держать lease. Для демо «один job на оба ЦОД» добавьте отдельно `…/global/jobs/…:acquire` (см. §«Модель 2 ЦОД» выше).

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

### 2) Конкуренция за regional job + неверный TTL

```bash
./scripts/demo-step-job-lock-contention.sh
```

- Сначала регистрируются два воркера; первый вызов **захватывает** **regional** job с их токенами.
- Второй — тот же `jobId`, другой валидный участник: ожидается **HTTP 409** (`JOB_EXISTS`, в теле — сообщение и часто `currentHolder`).
- Затем запрос с **`ttlSeconds: 5`** (ниже минимума 10): ожидается **HTTP 400** (валидация).
- В конце — **release** первым участником; затем **второй** с тем же `jobId` снова **acquire** (успех), **GET job** с Bearer coordinator, **release** вторым токеном лока.

Смысл для 2 ЦОД: этот сценарий — exclusivity **внутри активного сегмента**. На stand_in тот же regional acquire даст **403** (шаг 3); для exclusivity между ЦОД — global jobs (playbook в README / PLATFORM §5.3).

### 3) stand_in: regional запрещён, global разрешён

```bash
./scripts/demo-step-standby-regional-denied.sh
```

Проверяется, что на **8081** regional **coordinator / workers / jobs** дают **403** с телом **`STANDBY_DEPLOYMENT`** (пассивный ЦОД, home обычно **r2**, `status=stand_in` — regional мутации шлите на **ACTIVE :8080**), а **global coordinator** — успешно. В JSON сценария в `metadata` часто указано **`region: "r2"`** (home stand_in), не r1.

Ведущему: «regional job здесь мёртв намеренно; если нужен один владелец на оба сайта — `…/global/jobs`».

### 4) RSS mock: master data (active → switch → stand_in)

Mock реализует контракт [`openapi/rss-api.yaml`](../openapi/rss-api.yaml) и крутит статусы каждые **`RSS_CYCLE_INTERVAL_SEC`** (по умолчанию **8** с).

```bash
# T0
./scripts/demo-two-dc.sh etcd-start
./scripts/demo-two-dc.sh rss-start
# либо: ./scripts/demo-two-dc.sh infra-up

# T1 — сервер с coordination.rss.required=true
./scripts/demo-two-dc.sh run-active-rss
# или фон: ./scripts/demo-two-dc.sh start-active-rss-bg

# T3
./scripts/demo-step-rss-sync.sh
```

Скрипт показывает snapshot RSS (`/v1/debug/state`), затем `GET /v1/active-region` у LES и несколько раз поллит статус, пока mock проходит цикл **active → switch → stand_in**. Маппинг wire→внутренний: `active`→ACTIVE, `stand_in`→STANDBY, `switch`→SWITCH. В etcd пишется per-home ключ: RSS `activeRegion` → логический serves-id; RSS `region` → **home** LES; `status` — POV home. В ответе LES: `region`=home, `activeRegion`=serves, `status`=wire.

Прямой SSE к mock (опционально):

```bash
curl -N "http://127.0.0.1:8090/v1/recovery-groups/demo-rg/active-region?region=r1"
```

Принудительно сменить статус mock: `curl -X POST 'http://127.0.0.1:8090/v1/debug/force?status=stand_in'`.

---

## Дополнительно (без пошаговой паузы)

В [`scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh): `active-flow` (всё подряд), `switch-region`, `active-after-switch`, `standby-checks`, SSE и т.д. Перед защищёнными GET в лог печатается тот же блок про **Authorization: Bearer** и полный токен, что и в пошаговых скриптах. См. `./scripts/demo-two-dc.sh help`.

**TTL** для coordinator / worker register / job acquire: **10–300** секунд. После **смены activeRegion** смотрите **`regionalWritesBlocked`** в `GET …/active-region` (drain по TTL старого сегмента). Regional jobs после switch — заново; global — lease живёт в `…/global/jobs/…`.

---

## Сбои (коротко)

| Симптом | Действие |
|---------|----------|
| Скрипт не стартует | `chmod +x scripts/demo-step-*.sh` |
| Нет ответа от ACTIVE | `etcd-start`, `init-file`, `run-active`, проверка `curl -sS http://127.0.0.1:8080/actuator/health` |
| `run-active-rss` падает на старте | `rss-start` / `infra-up`, проверка `curl -sS http://127.0.0.1:8090/health`; без yaml `home` и без ответа RSS старт намеренно падает |
| Шаг stand_in падает на health | Запустите `run-standby` на **8081** |
| **HTTP 403** на regional job на 8081 | Ожидаемо (stand_in); для cross-DC — global jobs |
| **HTTP 409** на первом шаге happy path (лидер / job / worker уже есть) | Другой tenant или дождаться TTL старого lease |
| **HTTP 401** на `GET …/coordinator`, workers, jobs, SSE workers/jobs | Сначала **POST …/coordinator/acquire** в этом контуре, затем **`Authorization: Bearer <token>`** из ответа (в логах шага это печатается явно перед `curl`) |

Подробности: [`README.md`](../README.md), playbook и семантика job — [`PLATFORM_OVERVIEW_RU.md`](PLATFORM_OVERVIEW_RU.md).
