# Платформа coordination (REST + SSE + gRPC + Java SDK)

Монорепозиторий:

- [`coordination-hash/`](coordination-hash/) — общее кольцо consistent hash (Murmur3 / Ketama); зависимость сервера и клиента.
- [`coordination-server/`](coordination-server/) — сервис на Spring Boot (etcd + REST + SSE + gRPC + OpenAPI).
- [`coordination-client/`](coordination-client/) — тонкий Java SDK (REST + опционально блокирующий gRPC), тот же protobuf, что у сервера.
- [`proto/coordination/v1/coordination.proto`](proto/coordination/v1/coordination.proto) — канон для gRPC и сгенерированных стабов.

## Сборка

```bash
./gradlew test
```

Запуск сервера из корня репозитория (multi-project Gradle сам подтянет `:coordination-hash`):

```bash
./gradlew :coordination-server:bootRun
```

Или используйте **`./scripts/demo-two-dc.sh run-active`** — он делает то же самое. Из корня `./gradlew test` или `./gradlew build` собирают все модули.

## OpenAPI и Swagger UI

При запущенном сервере:

- JSON: `http://localhost:8080/v3/api-docs`
- UI: `http://localhost:8080/swagger-ui.html`

### Статическая спецификация (YAML)

В репозитории зафиксирован канонический контракт: [`openapi/openapi.yaml`](openapi/openapi.yaml) (OpenAPI 3.0.3).

После сборки тот же файл попадает в classpath и доступен с запущенного сервера:

- `http://localhost:8080/openapi.yaml`

## gRPC

Сервер слушает порт `grpc.server.port` (по умолчанию **9090**), сервис `coordination.v1.Coordination`. Источник контракта: [`proto/coordination/v1/coordination.proto`](proto/coordination/v1/coordination.proto).

Regional-координатор (сегмент etcd = `coordination.region` / `COORDINATION_REGION`, default `r1`):

```bash
grpcurl -plaintext -d '{"tenantId":"team-a","coordinatorId":"payments","instanceId":"pod-1","ttlSeconds":10,"metadata":{}}' \
  localhost:9090 coordination.v1.Coordination/AcquireCoordinator
```

Единый глобальный координатор (между ЦОД):

```bash
grpcurl -plaintext -d '{"tenantId":"team-a","coordinatorId":"payments","instanceId":"pod-1","ttlSeconds":10,"metadata":{}}' \
  localhost:9090 coordination.v1.Coordination/AcquireGlobalCoordinator
```

Consistent hash (без записи в etcd; та же математика, что у `POST /v1/rings/assign`):

```bash
grpcurl -plaintext -d '{"workerIds":["worker-a","worker-b"],"virtualNodes":128}' \
  localhost:9090 coordination.v1.Coordination/ComputeRanges
```

## Эпоха fencing (`fenceEpoch`)

Каждый успешный `acquire` координатора увеличивает устойчивую монотонную **`fenceEpoch`**: для **regional** — под `.../regions/{COORDINATION_REGION}/coordinationEpoch`, для **global** — под `.../global/coordinationEpoch`. Значение входит в JSON лидера (`CoordinatorRecord.fenceEpoch`). Используйте его в своих хранилищах и очередях как **fencing token** (отклоняйте записи со устаревшей эпохой).

## Идемпотентность HTTP (REST)

Когда `coordination.idempotency.enabled` равен **true** (см. `application.yml` в `coordination-server`), клиенты могут передавать **`Idempotency-Key`** или **`X-Idempotency-Key`** на выбранных мутациях. Сервер сохраняет в etcd тело ответа JSON и HTTP-статус успешных исходов (**200**, **201**, **409**) на настраиваемый TTL и при повторе того же ключа с **тем же** методом, путём и телом запроса отдаёт сохранённый ответ (для `PUT .../global/kv/{keyId}` токен доступа KV из заголовков тоже входит в отпечаток, чтобы create и replace не пересекались). Слишком большие тела запроса фильтр обходит — запрос выполняется как обычно. В gRPC эти заголовки сегодня не реализованы.

## Отказоустойчивость и TTL

Сервис следует обычной для etcd схеме с lease:

- **Координатор** (`acquire` / `renew`): новый лидер записывается только в транзакции, где ключ лидера **отсутствует** (`create_revision == 0`) и выполняется CAS по `coordinationEpoch`. Значение лидера привязано к **lease** и содержит **`token`** (как у воркеров): сохраните его из ответа `acquire` и передавайте на каждый `renew`. Если старый процесс перестал продлевать, etcd удалит ключ после TTL; до этого **никакой другой инстанс не сможет acquire** ту же роль (regional или global). Ключ эпохи — устойчивые метаданные для fencing, а не замена «живости» lease.
- **Воркеры** (`register` / `heartbeat`): та же идея — `register` проходит только когда ключа воркера ещё нет; ключ на lease и исчезает, когда heartbeat прекращаются.
- **Задачи** (`:acquire` / `:renew` / `:release`): **`:acquire`** проверяет, что `holder` — это активный regional (или global) **`coordinatorId`** или **зарегистрированный `workerId`** в этом контуре, и что **`participantToken`** совпадает с токеном `coordinator/acquire` или с токеном `workers/register` (одних id недостаточно). В ответе по-прежнему отдельный **`token`** лока для **`:renew`** / **`:release`** (тело `{"token":"..."}`), как у секрета lease координатора.
- **Чтение** (`GET` coordinator / workers / jobs и SSE `stream/workers` / `stream/jobs`): для соответствующего контура передавайте **`Authorization: Bearer <token из coordinator acquire>`** (для regional URL — токен regional-лидера; для `/global/...` — токен global-лидера). Та же строка, что для `renew`. Альтернатива: заголовок **`X-Coordinator-Token`**. Для SSE `.../stream/workers|jobs|keys` и `.../global/stream/workers|jobs` допускается статический токен **`coordination.sre.stream-listen-token`** (см. `application.yml`, переопределение через **`COORDINATION_SRE_STREAM_LISTEN_TOKEN`**). В JSON на чтениях renewal-**секреты** (`token` у coordinator/workers/jobs) опускаются. Без корректного доказательства: **401** (`COORDINATOR_READ_TOKEN_REQUIRED`) или **403** (`COORDINATOR_READ_FORBIDDEN` / `COORDINATOR_READ_NO_LEADER`).

Для **regional** coordinator / worker / job параметр `ttlSeconds` при acquire/register должен быть **от 10 до 300 секунд включительно** (проверка в сервисах, REST-валидация; в gRPC `IllegalArgumentException` мапится в `INVALID_ARGUMENT`).

После отказа или потери старой площадки **новые pod’ы в выжившем регионе могут занять координатора (или снова использовать тот же job id) только после истечения прежних ключей на lease** (или явного release). Нет API, которое выдало бы второго владельца, пока жив первый ключ.



## Java SDK (`coordination-client`)

Зависимость (Gradle multi-project или после `./gradlew :coordination-client:publishToMavenLocal`):

```kotlin
implementation("com.acme:coordination-client:0.1.0-SNAPSHOT")
// или в этом репо:
implementation(project(":coordination-client"))
```

```xml
<dependency>
  <groupId>com.acme</groupId>
  <artifactId>coordination-client</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### LesClient (продуктовый фасад)

Пакет `com.acme.coordination.sdk.les`: `LesClient.connect(url).sreToken(...).viaRss(rssUrl, recoveryGroup).drainTenant(t).build()`.

- Freestanding lock: `les.tenant(t).global().lock("job").tryRun(() -> …)` (без worker).
- JobQ: `becomeLeader` / `joinWorker` / `workers(leader).hashRing()`.
- Drain после RSS flip: client-only `viaRss` + `drainTenant` (сервер LES **не** знает RSS).
- `flags()` / OpsWatch / `MonitoringSse` — см. sketch и playbook ниже.

Playbook путей миграции A/B/C + примеры SDK: **[`docs/LES_SDK_USECASES_RU.md`](docs/LES_SDK_USECASES_RU.md)**.  
Короткий sketch: [`scripts/samples/les-client-sketch.md`](scripts/samples/les-client-sketch.md).

**Freestanding job на сервере:** `participantToken` опционален. **Drain:** `POST /v1/tenants/{t}/regions/{regionId}:drain` (Bearer SRE token).

### Sessions (рекомендуется): auto-renew / heartbeat

Пакет `com.acme.coordination.sdk.session`: `CoordinationSessions` + `holdCoordinator` / `holdWorker` / `holdJob` / `runExclusiveJob`.  
`ApiScope.REGIONAL` — short-path etcd segment `COORDINATION_REGION`; `ApiScope.GLOBAL` — cross-DC exclusivity.  
По умолчанию renew каждые **ttl/3**; `close()` останавливает цикл и для worker/job делает deregister/release.

```java
import com.acme.coordination.sdk.CoordinationRestClient;
import com.acme.coordination.sdk.session.ApiScope;
import com.acme.coordination.sdk.session.LeaseOptions;

try (var rest = new CoordinationRestClient("http://localhost:8080");
     var sessions = rest.sessions();
     var worker = sessions.holdWorker(
         "team-a", "worker-1", "pod-1", ApiScope.REGIONAL, LeaseOptions.ofTtlSeconds(30));
     var job = sessions.holdJob(
         "team-a", "pay-batch", worker.workerId(), worker.token(),
         ApiScope.REGIONAL, LeaseOptions.ofTtlSeconds(30).withOnLost(ev ->
             System.err.println("job lost: " + ev.reason() + " " + ev.detail())))) {
  // exclusive work; renew в фоне
}

// one-shot (acquire → work → release):
try (var rest = new CoordinationRestClient("http://localhost:8080");
     var sessions = rest.sessions();
     var worker = sessions.holdWorker(
         "team-a", "worker-1", "pod-1", ApiScope.GLOBAL, LeaseOptions.ofTtlSeconds(30))) {
  sessions.runExclusiveJob(
      "team-a", "pay-batch", worker.workerId(), worker.token(),
      ApiScope.GLOBAL, LeaseOptions.ofTtlSeconds(30),
      () -> { /* work */ });
}
```

### Low-level REST / gRPC

Набросок без sessions (сами крутите renew):

```java
// REST — JsonNode ответов (поля как в OpenAPI: token, ok, renewed, acquired, …)
try (var rest = new CoordinationRestClient("http://localhost:8080")) {
  var reg = rest.registerWorker("team-a", "worker-1", "pod-1", 15, Map.of("dc", "eu"));
  String token = reg.get("token").asText();
  rest.workerHeartbeat("team-a", "worker-1", token);

  var acqCoord = rest.acquireCoordinator("team-a", Map.of(
      "coordinatorId", "payments", "instanceId", "pod-1", "ttlSeconds", 30, "metadata", Map.of()));
  String coordRead = acqCoord.get("token").asText();
  rest.renewCoordinator("team-a", coordRead);
  rest.getCoordinatorActive("team-a", coordRead);

  var job = rest.acquireJob("team-a", "pay-batch", "worker-1", token, 30, Map.of());
  String jobTok = job.get("token").asText();
  rest.renewJob("team-a", "pay-batch", jobTok);
  rest.getJob("team-a", "pay-batch", coordRead);
  rest.releaseJob("team-a", "pay-batch", jobTok);
}
// gRPC — блокирующие вызовы; те же сценарии + *Global* методы
try (var grpc = new CoordinationGrpcClient("localhost", 9090)) {
  var w = grpc.registerWorker("team-a", "worker-1", "pod-1", 15, Map.of("dc", "eu"));
  grpc.workerHeartbeat("team-a", "worker-1", w.getToken());

  var acq = grpc.acquireCoordinator("team-a", "payments", "pod-1", 30, Map.of());
  String coordRead = acq.getToken();
  grpc.listWorkers("team-a", coordRead);

  var job = grpc.acquireJob("team-a", "pay-batch", "worker-1", w.getToken(), 30, Map.of());
  String jobTok = job.getToken();
  grpc.renewJob("team-a", "pay-batch", jobTok);
  grpc.getJob("team-a", "pay-batch", coordRead);
  grpc.releaseJob("team-a", "pay-batch", jobTok);
}
```

`CoordinationRestClient`: `registerWorker` / `workerHeartbeat` / `deregisterWorker`, те же имена с суффиксом `Global` и путём `.../global/workers/...`; для job — `acquireJob` → job `token` → `renewJob` / `releaseJob` (и `*Global`). **GET** (`getCoordinatorActive`, `getWorkerIdsActive`, `getJob`, …) принимают второй аргумент `coordinatorReadToken` (regional или global — в зависимости от URL). Для координатора: `acquireCoordinator` → `renewCoordinator` / **тот же token** на чтения.  
`CoordinationGrpcClient`: `getCoordinator`, `listWorkers`, `getJob` и global-аналоги принимают `coordinatorReadToken`; нет RPC **deregister** у воркера (только REST).

## Локальный запуск

### Для разработчика (Docker, CLI, demo-скрипты)

**Docker обязателен** для типичного локального цикла: **etcd** и **etcdkeeper** поднимаются через [`docker-compose.yml`](docker-compose.yml) / [`scripts/demo-two-dc.sh`](scripts/demo-two-dc.sh) (`etcd-start`). Без Docker daemon команды `etcd-start` / `etcd-verify` не смогут стартовать кластер; Spring по умолчанию ждёт etcd по [`coordination-server/src/main/resources/application.yml`](coordination-server/src/main/resources/application.yml) — `http://127.0.0.1:2379` (при другом адресе задайте `coordination.etcd.endpoints` или соответствующие переменные окружения).

**Быстрый старт одного ACTIVE** (из **корня репозитория**, JDK **21+**, `JAVA_HOME`, `./gradlew`):

```bash
chmod +x scripts/demo-two-dc.sh scripts/demo-step-*.sh
./scripts/demo-two-dc.sh etcd-start    # etcd :2379 + UI (docker compose)
./scripts/demo-two-dc.sh etcd-verify   # опционально: проверка доступности etcd
./scripts/demo-two-dc.sh init-file     # bootstrap active region (yaml / PUT; нужен запущенный ACTIVE)
./scripts/demo-two-dc.sh run-active    # coordination-server: HTTP :8080, gRPC :9090
```


Инфра: `./scripts/demo-two-dc.sh etcd-start` / `docker compose up -d`.

**All-in-one** (приложение + etcd в одном контейнере, без отдельного etcd-сервиса):

```bash
docker build -f Dockerfile.with-etcd -t coordination-server:with-etcd .
docker run --rm -p 8080:8080 -p 9090:9090 -p 2379:2379 coordination-server:with-etcd
# или:
docker compose --profile with-etcd up -d --build coordination-with-etcd
# HTTP :8080, gRPC :9090, etcd с хоста :12379 (внутри контейнера app → 127.0.0.1:2379)
```

Тот же сервер **без** `run-active`:

```bash
./gradlew :coordination-server:bootRun
```

Справка по всем командам демо-оркестратора: `./scripts/demo-two-dc.sh help` (два инстанса: `run-active` / `run-standby`, фон: `start-active-bg` / `start-standby-bg`, остановка: `stop-apps`, сценарий `active-flow` и т.д.).

**Пошаговые `demo-step-*.sh`** (пауза между шагами: **`DEMO_STEP_PAUSE_SEC`**, по умолчанию 20 с; база: **`BASE_ACTIVE`**, tenant: **`TENANT`**):

| Скрипт | Назначение |
|--------|------------|
| `scripts/demo-step-happy-path.sh` | Основной сценарий REST + опционально grpcurl |
| `scripts/demo-step-workers-and-keys.sh` | GET worker jobs + global KV, опционально очистка etcd tenant |
| `scripts/demo-step-job-lock-contention.sh` | Конкуренция за job lock |
| `scripts/demo-step-sse-workers-jobs.sh` | Два SSE в отдельном терминале (`COORD_SSE_BEARER`; для global — `DEMO_SSE_SCOPE=global`, `GLOBAL_SSE_BEARER`) |
| `scripts/demo-step-bench-rest.sh` / `scripts/demo-step-bench-grpc.sh` | Замеры латентности REST / gRPC (для gRPC нужен **grpcurl**, см. `BENCH_PROTO_ROOT`) |

После `chmod +x` запускайте, например: `./scripts/demo-step-happy-path.sh`.  
Порядок терминалов, что говорить ведущему, переменные для SSE и бенчей — **[`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md)**. В конце большинства `demo-step-*` при **`DEMO_STEP_ETCD_DELETE_TENANT=1`** очищается префикс tenant в etcd (если нет локального **etcdctl**, при запущенном контейнере etcd скрипт может вызвать `docker exec` — см. `scripts/lib/demo-step-helpers.sh`); отключить сброс: **`DEMO_STEP_ETCD_DELETE_TENANT=0`**.

### Демо (два LES, разные `COORDINATION_REGION`)

Живое демо на два процесса (**8080** / `region=r1`, **8081** / `region=r2`); клиент сам выбирает regional vs global API. `etcd-start` / `infra-up` поднимает [`docker-compose.yml`](docker-compose.yml) (**etcd** + **etcdkeeper**). В UI: **`http://127.0.0.1:2379`**, версия **v3**. Проверка: `./scripts/demo-two-dc.sh etcd-verify`.

- Пошаговый сценарий: [`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md)
- Обзор платформы: [`docs/PLATFORM_OVERVIEW_RU.md`](docs/PLATFORM_OVERVIEW_RU.md)
- Пути миграции + LesClient SDK: [`docs/LES_SDK_USECASES_RU.md`](docs/LES_SDK_USECASES_RU.md)
- Env: [`docs/ENV_VARIABLES_RU.md`](docs/ENV_VARIABLES_RU.md)
- [`scripts/demo-two-dc.sh`](scripts/demo-two-dc.sh) — `./scripts/demo-two-dc.sh help`. Из корня: `run-active` / `run-standby` (JDK **21+**, `./gradlew :coordination-server:bootRun`); фон: `start-active-bg` / `start-standby-bg`, стоп: `stop-apps`.

## Переезд: 1 ЦОД → 2 ЦОД (playbook клиента)

LES — **leader election**: coordinator / workers / jobs (regional + global). **Кто «активный ЦОД»** — на стороне клиента (или внешнего оркестратора), не в LES.

| Было (1 ЦОД) | Стало (2 ЦОД) |
|--------------|---------------|
| Один base URL | Два LES (`COORDINATION_REGION=r1` / `r2`) + **общий stretched etcd**; клиент ходит на **свой** DC |
| Один смысл «кто лидер / кто держит job» | **Regional** — один владелец в сегменте этого LES; **global** — один владелец на весь tenant между ЦОД |
| Один контур API | Клиент выбирает short-path (`/coordinator`, `/workers`, `/jobs`) или `/global/...` |

**Минимальный клиентский цикл:**

1. Выберите base URL LES своего ЦОД (или follow на «активный» сайт сами).
2. Для работы только в этом ЦОД — **regional** API; для exclusivity на оба сайта — **`/global/...`**.
3. После смены «активного» сайта на клиенте — заново register/acquire на нужном LES / контуре.

Подробнее: [`docs/PLATFORM_OVERVIEW_RU.md`](docs/PLATFORM_OVERVIEW_RU.md).  
Пути A/B/C и Java SDK: [`docs/LES_SDK_USECASES_RU.md`](docs/LES_SDK_USECASES_RU.md).  
Демо: [`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md).

## Поверхность API: regional и global

Публичный контракт — **две** семьи URL/gRPC (клиент **не** передаёт `regionId` в путях / protobuf):

1. **Regional** — `/v1/tenants/{tenant}/coordinator`, `/workers`, `/jobs`, `/stream/...`. Сегмент etcd: **`coordination.region`** (`COORDINATION_REGION`, default `r1`).
2. **Global** — `/v1/tenants/{tenant}/global/...` (общий пул coordinator/workers/jobs/kv на tenant между ЦОД).

API `GET/PUT /v1/active-region`, SSE `stream/active-region`, RSS и гейты stand_in/switch/drain **удалены**.

## Сквозной сценарий: как жить с сервисом (клиентское приложение)

Ниже сценарий для **regional** short-path. Для **global** замените префиксы на `/v1/tenants/{tenant}/global/...` и используйте `GET .../global/workers/ids`.

### 1. Выбор tenant

Один `tenantId` на изолированный контур (команду / продукт). Сегмент etcd задаёт конфиг LES (`COORDINATION_REGION`), не запрос клиента.

### 2. Воркеры (каждый процесс / pod)

Каждый исполнитель один раз регистрируется и дальше шлёт heartbeat, пока жив:

1. `POST /v1/tenants/{tenant}/workers/register` с уникальным `workerId` и `ttlSeconds`. Повторная регистрация с тем же `workerId`, пока lease жив, даёт **HTTP 409** (`WORKER_EXISTS`).
2. Сохраните `token` из ответа.
3. Периодически `POST .../workers/{workerId}/heartbeat` с тем же `token`.
4. При остановке — `POST .../workers/{workerId}/deregister`.

### 3. Координатор (один на tenant в этом regional сегменте)

1. `POST .../coordinator/acquire` с `coordinatorId` (логический id роли, как `workerId` у воркеров), `instanceId` (id процесса / pod, владелец lease), `ttlSeconds`, опционально `metadata`.
2. Если `acquired=true` — вы лидер; запомните `fenceEpoch` (и при необходимости `leaseId`) из ответа. Используйте `fenceEpoch` как **fencing token** к своим хранилищам/очередям (отвергайте старые эпохи).
3. Пока лидер — периодически `POST .../coordinator/renew` с телом `{"token":"<из ответа acquire>"}` (как heartbeat воркера).
4. Если лидер уже занят — **HTTP 409** с `error: COORDINATOR_EXISTS`, `message` и `currentHolder` в теле; повтор после освобождения lease (backoff).

### 4. Диапазоны consistent hash (координатор)

Чтобы не парсить полные объекты воркеров, координатор берёт **только id** (нужен Bearer — тот же `token`, что после `coordinator/acquire`):

```bash
curl -sS -H "Authorization: Bearer $COORD_READ" "$BASE/v1/tenants/$TENANT/workers/ids" | jq .
```

Дальше передать массив в калькулятор кольца (без записи в etcd):

```bash
curl -sS -X POST "$BASE/v1/rings/assign" \
  -H 'content-type: application/json' \
  -d '{"workerIds":["worker-1","worker-2","worker-3"],"virtualNodes":128}' | jq .
```

Ответ — карта «worker → диапазоны на кольце». Массив всех vnode с полем `pos` (`points`) добавляется только если в теле запроса указать `"withPos": true` (в JSON также принимается `withpos`); иначе только `ranges`, чтобы не раздувать ответ. Координатор рассылает карту воркерам (вне API, отдельным каналом) или кладёт версию в сторейдж; при изменении состава воркеров повторите `GET .../workers/ids` + пересчёт.

gRPC `ComputeRanges`: то же поведение — поле `with_points: true` в запросе заполняет `points` в ответе; по умолчанию только `ranges_by_worker`.

**Ключи сущностей (long, UUID):** алгоритм назначения воркера — Ketama по Murmur3-32 от **UTF-8** строки ключа и тем же `workerIds` / `virtualNodes`, что при расчёте кольца. Стабильно закодируйте id в строку на всех участниках (например `Long.toString(id)` или стандартный `UUID.toString()`), затем на клиенте либо повторите логику `lookupWorker` (hash ключа → первая vnode на кольце не ниже хеша), либо по полученным `ranges` найдите владельца для `unsigned32(Murmur3(key))` тем же правилом полуинтервалов, что и сервер (`ConsistentHashRing` в модуле **`coordination-hash`**). В Java SDK: `CoordinationRestClient.assignRing(...)` → `RingAssignment.workerFor(yourUuidOrLong)` или `RingAssignments.fromAssignResponse(json)` если JSON уже есть.

**Без SDK:** зависимость `coordination-hash`, классы `RingAssignment`, `RingIds`, `ConsistentHashRing` (тот же расчёт, что у сервера).

### 5. Задачи / блокировки — семантика dual-DC

Один API (`:acquire` / `:renew` / `:release`), **два контура**:

| | **Regional** `…/jobs/{jobId}:…` | **Global** `…/global/jobs/{jobId}:…` |
|--|----------------------------------|--------------------------------------|
| **Где ключ в etcd** | `…/regions/{COORDINATION_REGION}/jobs/…` | `…/global/jobs/…` |
| **Сколько владельцев** | Ровно один в сегменте этого LES | Ровно один на **весь tenant** (оба ЦОД) |
| **Когда выбирать** | Работа в контуре одного ЦОД / одного LES | Exclusivity на оба сайта |

Общие правила: **`holder`** — `coordinatorId` или **`workerId`** из того же контура; **`participantToken`** — токен acquire/register. При успехе — отдельный **`token`** лока для `:renew` / `:release`. Второй acquire при живом ключе — **409** `JOB_EXISTS`. Неверный участник — **403** `JOB_PARTICIPANT_INVALID`; без токена — **400** `JOB_PARTICIPANT_TOKEN_REQUIRED`. TTL **10…300** с.

**Список job воркера:** `GET …/workers/{workerId}/jobs` или `GET …/global/workers/{workerId}/jobs` — тот же **`token`**, что после register (`Authorization: Bearer` или **`X-Worker-Token`**).

### 5b. Простые KV-ticket (global)

`PUT /v1/tenants/{tenant}/global/kv/{keyId}` с телом `{"value":<любой JSON>,"ttlSeconds":10}` — создание (если ключа ещё нет). Без `ttlSeconds` ключ **без lease**. Ответ **201** содержит **`accessToken`** для `GET` / `DELETE` / обновления (`Authorization: Bearer` или **`X-Kv-Access-Token`**). Конфликт ревизий — **409** `KV_REVISION_CONFLICT`; ключ уже есть — **409** `KV_EXISTS`.

### 6. Наблюдение

- `GET .../stream/workers` / `stream/jobs` — изменения ключей в etcd; нужен Bearer coordinator (или SRE `COORDINATION_SRE_STREAM_LISTEN_TOKEN`).
- `GET .../workers` / `workers/ids` — с Bearer coordinator.

## Примеры REST

Допущения: `BASE=http://localhost:8080`, `TENANT=team-a`, LES с `COORDINATION_REGION=r1`.

### Acquire coordinator (сохраните token для GET ниже)

```bash
COORD_READ="$(curl -sS -X POST "$BASE/v1/tenants/$TENANT/coordinator/acquire" \
  -H 'content-type: application/json' \
  -d '{
    "coordinatorId":"payments",
    "instanceId":"pod-abc",
    "ttlSeconds":10,
    "metadata":{"region":"r1","shard":"primary"}
  }' | jq -r .token)"
echo "токен coordinator сохранён в COORD_READ"
```

### Координатор: regional и global (для GET нужен Bearer)

```bash
# Regional лидер (тот же token, что для renew)
curl -sS -H "Authorization: Bearer $COORD_READ" "$BASE/v1/tenants/$TENANT/coordinator" | jq .
curl -sS -H "Authorization: Bearer $COORD_READ" "$BASE/v1/tenants/$TENANT/coordinator/active" | jq .

# Глобальный лидер: сначала acquire, затем GET с этим token
GLOB_READ="$(curl -sS -X POST "$BASE/v1/tenants/$TENANT/global/coordinator/acquire" \
  -H 'content-type: application/json' \
  -d '{"coordinatorId":"payments-global","instanceId":"pod-abc","ttlSeconds":10,"metadata":{}}' | jq -r .token)"
curl -sS -H "Authorization: Bearer $GLOB_READ" "$BASE/v1/tenants/$TENANT/global/coordinator" | jq .
```

### Renew coordinator (если вы держатель)

```bash
curl -sS -X POST "$BASE/v1/tenants/$TENANT/coordinator/renew" \
  -H 'content-type: application/json' \
  -d '{"token":"<вставьте-token-из-ответа-acquire>"}' | jq .
```

### Worker ids (для consistent hash)

```bash
curl -sS -H "Authorization: Bearer $COORD_READ" "$BASE/v1/tenants/$TENANT/workers/ids" | jq .
curl -sS -H "Authorization: Bearer $GLOB_READ" "$BASE/v1/tenants/$TENANT/global/workers/ids" | jq .
```

### Регистрация воркера

```bash
TOKEN="$(curl -sS -X POST "$BASE/v1/tenants/$TENANT/workers/register" \
  -H 'content-type: application/json' \
  -d '{
    "workerId":"worker-1",
    "instanceId":"pod-abc",
    "ttlSeconds":15,
    "metadata":{"client-range":"a-m"}
  }' | jq -r .token)"
echo "token=$TOKEN"
```

### Heartbeat воркера

```bash
curl -sS -X POST "$BASE/v1/tenants/$TENANT/workers/worker-1/heartbeat" \
  -H 'content-type: application/json' \
  -d "{\"token\":\"$TOKEN\"}" | jq .
```

### Распределённый job lock

```bash
JOB_TOKEN="$(curl -sS -X POST "$BASE/v1/tenants/$TENANT/jobs/reconcile-2026-05-11:acquire" \
  -H 'content-type: application/json' \
  -d "$(jq -nc --arg h worker-1 --arg pt "$TOKEN" \
    '{holder:$h,participantToken:$pt,ttlSeconds:30,metadata:{kind:"reconcile",attempt:"1"}}')" | jq -r .token)"
# GET job: Bearer = coordinator token (не job token)
curl -sS -H "Authorization: Bearer $COORD_READ" "$BASE/v1/tenants/$TENANT/jobs/reconcile-2026-05-11" | jq .
curl -sS -X POST "$BASE/v1/tenants/$TENANT/jobs/reconcile-2026-05-11:renew" \
  -H 'content-type: application/json' \
  -d "{\"token\":\"$JOB_TOKEN\"}" | jq .
curl -sS -X POST "$BASE/v1/tenants/$TENANT/jobs/reconcile-2026-05-11:release" \
  -H 'content-type: application/json' \
  -d "{\"token\":\"$JOB_TOKEN\"}" | jq .
```

### Потоки SSE

```bash
curl -N -H 'Accept: text/event-stream' -H "Authorization: Bearer $COORD_READ" \
  "$BASE/v1/tenants/$TENANT/stream/workers"
```

```bash
curl -N -H 'Accept: text/event-stream' -H "Authorization: Bearer $COORD_READ" \
  "$BASE/v1/tenants/$TENANT/stream/jobs"
```

### Диапазоны consistent hash

```bash
curl -sS -X POST "$BASE/v1/rings/assign" \
  -H 'content-type: application/json' \
  -d '{"workerIds":["worker-1","worker-2","worker-3"],"virtualNodes":100}' | jq .

# с полным списком vnode (pos), только если нужно отладить кольцо:
curl -sS -X POST "$BASE/v1/rings/assign" \
  -H 'content-type: application/json' \
  -d '{"workerIds":["worker-1","worker-2","worker-3"],"virtualNodes":100,"withPos":true}' | jq .
```

## Дымовой тест изоляции tenant

1. Создайте воркеров для `TENANT=team-a`
2. Создайте воркеров для `TENANT=team-b`
3. Убедитесь, что `GET /v1/tenants/team-a/workers` и `GET .../workers/ids` не содержат воркеров из `team-b`
4. Убедитесь, что SSE-потоки для `team-a` не отдают события от `team-b`
