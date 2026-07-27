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

Координатор active-region (логический сегмент подставляет сервер из etcd; POV этого LES —
ключ `/coord/v1/cluster/regions/{home}/activeRegion`):

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

Каждый успешный `acquire` координатора увеличивает устойчивую монотонную **`fenceEpoch`**: для контура **active-region** — под `.../regions/{logicalRegion}/coordinationEpoch` (логический регион задаётся только конфигурацией сервера), для **global** — под `.../global/coordinationEpoch`. Значение входит в JSON лидера (`CoordinatorRecord.fenceEpoch`). Используйте его в своих хранилищах и очередях как **fencing token** (отклоняйте записи со устаревшей эпохой).

## Идемпотентность HTTP (REST)

Когда `coordination.idempotency.enabled` равен **true** (см. `application.yml` в `coordination-server`), клиенты могут передавать **`Idempotency-Key`** или **`X-Idempotency-Key`** на выбранных мутациях. Сервер сохраняет в etcd тело ответа JSON и HTTP-статус успешных исходов (**200**, **201**, **409**) на настраиваемый TTL и при повторе того же ключа с **тем же** методом, путём и телом запроса отдаёт сохранённый ответ (для `PUT .../global/kv/{keyId}` токен доступа KV из заголовков тоже входит в отпечаток, чтобы create и replace не пересекались). Слишком большие тела запроса фильтр обходит — запрос выполняется как обычно. В gRPC эти заголовки сегодня не реализованы.

## Отказоустойчивость, switchover и TTL

Сервис следует обычной для etcd схеме с lease:

- **Координатор** (`acquire` / `renew`): новый лидер записывается только в транзакции, где ключ лидера **отсутствует** (`create_revision == 0`) и выполняется CAS по `coordinationEpoch`. Значение лидера привязано к **lease** и содержит **`token`** (как у воркеров): сохраните его из ответа `acquire` и передавайте на каждый `renew`. Если старый процесс перестал продлевать, etcd удалит ключ после TTL; до этого **никакой другой инстанс не сможет acquire** ту же роль (regional или global). Ключ эпохи — устойчивые метаданные для fencing, а не замена «живости» lease.
- **Воркеры** (`register` / `heartbeat`): та же идея — `register` проходит только когда ключа воркера ещё нет; ключ на lease и исчезает, когда heartbeat прекращаются.
- **Задачи** (`:acquire` / `:renew` / `:release`): **`:acquire`** проверяет, что `holder` — это активный regional (или global) **`coordinatorId`** или **зарегистрированный `workerId`** в этом контуре, и что **`participantToken`** совпадает с токеном `coordinator/acquire` или с токеном `workers/register` (одних id недостаточно). В ответе по-прежнему отдельный **`token`** лока для **`:renew`** / **`:release`** (тело `{"token":"..."}`), как у секрета lease координатора.
- **Чтение** (`GET` coordinator / workers / jobs и SSE `stream/workers` / `stream/jobs`, не `active-region`): для соответствующего контура передавайте **`Authorization: Bearer <token из coordinator acquire>`** (для active-region URL — токен regional-лидера; для `/global/...` — токен global-лидера). Та же строка, что для `renew`. Альтернатива: заголовок **`X-Coordinator-Token`**. Для SSE `.../stream/workers|jobs|keys` и `.../global/stream/workers|jobs`, а также для **`PUT /v1/active-region`** / gRPC **`SetActiveRegion`**, допускается статический токен **`coordination.sre.stream-listen-token`** (см. `application.yml`, переопределение через **`COORDINATION_SRE_STREAM_LISTEN_TOKEN`**). В JSON на чтениях renewal-**секреты** (`token` у coordinator/workers/jobs) опускаются. Без корректного доказательства: **401** (`COORDINATOR_READ_TOKEN_REQUIRED`) или **403** (`COORDINATOR_READ_FORBIDDEN` / `COORDINATOR_READ_NO_LEADER`).

Для **regional** coordinator / worker / job параметр `ttlSeconds` при acquire/register должен быть **от 10 до 300 секунд включительно** (проверка в сервисах, REST-валидация; в gRPC `IllegalArgumentException` мапится в `INVALID_ARGUMENT`).

После отказа или потери старой площадки **новые pod’ы в выжившем регионе могут занять координатора (или снова использовать тот же job id) только после истечения прежних ключей на lease** (или явного release). Нет API, которое выдало бы второго владельца, пока жив первый ключ.

**Switchover active-region:** при смене **id** активного региона через **`PUT /v1/active-region`** (SRE token) сервер входит в фазу **drain**: в `GET /v1/active-region` (и в SSE) появляются `regionalWritesBlocked` и `drainingPreviousRegion`. Пока `regionalWritesBlocked` истинно, **active-region** lease-мутации (coordinator acquire/renew, worker register/heartbeat/deregister, job acquire/renew/release) возвращают **HTTP 409** с `REGION_DRAIN` (в gRPC: **FAILED_PRECONDITION**). Сервер снимает drain, когда в etcd под префиксом старого региона **нет** ключей, кроме **`coordinationEpoch`**. После этого клиенты снова могут acquire/register/job в новом сегменте.

**Ускоренный drain (`status=SWITCH` в etcd / RSS для home этого LES):** regional-мутации для клиентов по-прежнему **403** (как у `stand_in`), но этот хост **участвует в drain** (поэтому `regionalWritesBlocked` может оставаться **true**, а **409** `REGION_DRAIN` действует, пока есть старые ключи). На каждом опросе drain сервер **удаляет** lease-ключи под префиксом draining-региона (лидер, воркеры, задачи) для **всех tenant**, оставляя **`coordinationEpoch`**. Используйте только когда операторы **остановили** все процессы, которые ещё держали эти lease; иначе возможно неконсистентное поведение клиентов. После завершения переключения верните статус home в **`ACTIVE`** (RSS или `PUT /v1/active-region`).

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

### Sessions (рекомендуется): auto-renew / heartbeat

Пакет `com.acme.coordination.sdk.session`: `CoordinationSessions` + `holdCoordinator` / `holdWorker` / `holdJob` / `runExclusiveJob`.  
`ApiScope.REGIONAL` — short-path (только при local LES `status=active`); `ApiScope.GLOBAL` — cross-DC exclusivity.  
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

**Docker обязателен** для типичного локального цикла: **etcd**, **etcdkeeper** и (для RSS-демо) **rss-mock** поднимаются через [`docker-compose.yml`](docker-compose.yml) / [`scripts/demo-two-dc.sh`](scripts/demo-two-dc.sh) (`etcd-start`, `rss-start`, `infra-up`). Без Docker daemon команды `etcd-start` / `etcd-verify` не смогут стартовать кластер; Spring по умолчанию ждёт etcd по [`coordination-server/src/main/resources/application.yml`](coordination-server/src/main/resources/application.yml) — `http://127.0.0.1:2379` (при другом адресе задайте `coordination.etcd.endpoints` или соответствующие переменные окружения).

**Быстрый старт одного ACTIVE** (из **корня репозитория**, JDK **21+**, `JAVA_HOME`, `./gradlew`):

```bash
chmod +x scripts/demo-two-dc.sh scripts/demo-step-*.sh
./scripts/demo-two-dc.sh etcd-start    # etcd :2379 + UI (docker compose)
./scripts/demo-two-dc.sh etcd-verify   # опционально: проверка доступности etcd
./scripts/demo-two-dc.sh init-file     # bootstrap active region (yaml / PUT; нужен запущенный ACTIVE)
./scripts/demo-two-dc.sh run-active    # coordination-server: HTTP :8080, gRPC :9090
```

**RSS mock** (цикл `active` → `switch` → `stand_in` каждые ~8 с):

```bash
./scripts/demo-two-dc.sh rss-start       # :8090
./scripts/demo-two-dc.sh run-active-rss  # rss.required=true
./scripts/demo-step-rss-sync.sh
```

Или одной командой инфра: `./scripts/demo-two-dc.sh infra-up` / `docker compose up -d`.

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
| `scripts/demo-step-standby-regional-denied.sh` | STANDBY и отказ regional-мутаций |
| `scripts/demo-step-rss-sync.sh` | Bootstrap/sync active-region из RSS mock (нужны `rss-start` + `run-active-rss`) |
| `scripts/demo-step-sse-workers-jobs.sh` | Два SSE в отдельном терминале (`COORD_SSE_BEARER`; для global — `DEMO_SSE_SCOPE=global`, `GLOBAL_SSE_BEARER`) |
| `scripts/demo-step-bench-rest.sh` / `scripts/demo-step-bench-grpc.sh` | Замеры латентности REST / gRPC (для gRPC нужен **grpcurl**, см. `BENCH_PROTO_ROOT`) |

После `chmod +x` запускайте, например: `./scripts/demo-step-happy-path.sh`.  
Порядок терминалов, что говорить ведущему, переменные для SSE и бенчей — **[`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md)**. В конце большинства `demo-step-*` при **`DEMO_STEP_ETCD_DELETE_TENANT=1`** очищается префикс tenant в etcd (если нет локального **etcdctl**, при запущенном контейнере etcd скрипт может вызвать `docker exec` — см. `scripts/lib/demo-step-helpers.sh`); отключить сброс: **`DEMO_STEP_ETCD_DELETE_TENANT=0`**.

### Демо (ACTIVE + STANDBY + switchover)

Скрипт для живого демо на два процесса сервера (8080 ACTIVE / 8081 STANDBY), смена active region через **RSS** или **`PUT /v1/active-region`** (SRE) и проверка **403** на active-region мутациях со STANDBY. Команда `etcd-start` / `infra-up` поднимает стек из [`docker-compose.yml`](docker-compose.yml) (**etcd** + **etcdkeeper** sidecar + опционально **rss-mock**). В UI укажите **`http://127.0.0.1:2379`** и версию **v3**. Проверка: `./scripts/demo-two-dc.sh etcd-verify`.

- Пошаговый сценарий для ведущего (что сказать, какие терминалы): [`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md)
- Обзор для Confluence / пресейла (гарантии, API, **use-cases**, 2 ЦОД + etcd): [`docs/PLATFORM_OVERVIEW_RU.md`](docs/PLATFORM_OVERVIEW_RU.md)
- Краткий чеклист запуска и список `demo-step-*` — подраздел **«Для разработчика»** выше; полный текст сценария и переменных — в **DEMO_PLAN_RU.md**.
- [`scripts/demo-two-dc.sh`](scripts/demo-two-dc.sh) — `chmod +x scripts/demo-two-dc.sh` затем `./scripts/demo-two-dc.sh help`. Запуск двух инстансов из **корня репо**: `./scripts/demo-two-dc.sh run-active` и `run-standby` (JDK **21+**, `JAVA_HOME`, `./gradlew :coordination-server:bootRun`); фон: `start-active-bg` / `start-standby-bg`, остановка: `stop-apps`. В **`active-flow`** после REST `rings/assign` вызывается **`grpcurl …/ComputeRanges`** на `GRPC_ACTIVE_ADDR` (по умолчанию `127.0.0.1:9090`), если `grpcurl` установлен.

## Переезд: 1 ЦОД → 2 ЦОД (playbook клиента)

Цель платформы — дать **один владелец** роли/job при появлении второго ЦОД, без самописного etcd в каждом сервисе.

| Было (1 ЦОД) | Стало (2 ЦОД) |
|--------------|---------------|
| Один base URL coordination | Два LES (например r1 и r2) + **общий stretched etcd**; клиент ходит на **свой** DC или follow ACTIVE |
| Один смысл «кто лидер / кто держит job» | **Regional** job — один владелец в **активном** сегменте; на stand_in — **403**. Критичное между ЦОД → **global** job/coordinator |
| «Просто acquire» | После switch: ждать `regionalWritesBlocked=false`, затем **заново** register/acquire в новом сегменте |
| Роль из env на JVM | Роль (**status**) — POV **home** этого LES из etcd/RSS; ответ `GET …/active-region`: `region`=home, `activeRegion`=кто serves, `status`=active\|stand_in\|switch |

**Минимальный клиентский цикл:**

1. `GET /v1/active-region` (или SSE) на том base URL, куда шлёте API.
2. Если `status != active` — **не** делайте regional mutate (получите **403**); для cross-DC exclusivity используйте **`/global/...`**.
3. Если `regionalWritesBlocked` — ждите конца drain (**409** `REGION_DRAIN` на regional lease).
4. Выберите контур job: regional vs global (см. ниже).

Подробнее: [`docs/PLATFORM_OVERVIEW_RU.md`](docs/PLATFORM_OVERVIEW_RU.md) (§2 playbook, семантика job). Демо двух процессов: [`docs/DEMO_PLAN_RU.md`](docs/DEMO_PLAN_RU.md).

## Поверхность API: active-region и global

Публичный контракт делится на **две** семьи URL/gRPC (клиент **не передаёт** `regionId` в путях или в protobuf):

1. **Active region (regional)** — короткие пути `/v1/tenants/{tenant}/coordinator`, `/workers`, `/jobs`, `/stream/...` (кроме `stream/active-region`). Сервер подставляет **логический activeRegion** из **etcd** POV этого LES: ключ  
   **`/coord/v1/cluster/regions/{home}/activeRegion`**  
   (`home` = `coordination.active-region.home` или RSS `state.region`; задаётся RSS / yaml / `PUT /v1/active-region` / gRPC `SetActiveRegion` с SRE-токеном).  
   Данные tenant: `.../tenants/{tenant}/regions/{activeRegion}/...`.

   **Switchover:** подпишитесь на `GET /v1/tenants/{tenant}/stream/active-region` и следите за `regionalWritesBlocked`. Пока drain активен после смены id `activeRegion`, новые acquire/register/job в regional отклоняются (**409**). Когда `regionalWritesBlocked` стал **false**, заново зарегистрируйте coordinator/workers/jobs — короткие пути пишут в новый сегмент автоматически.

2. **Global** — `/v1/tenants/{tenant}/global/...` (общий пул и один coordinator/jobs на tenant между ЦОД; **не** блокируется regional drain и **разрешён** на stand_in).

### Статус этого LES (ACTIVE / STANDBY / SWITCH)

В ответе `GET …/active-region`: **`region`** = home этого инстанса, **`activeRegion`** = кто сейчас serves short-path, **`status`** = POV home (`active` / `stand_in` / `switch`). Источник статуса — etcd (и RSS, если `coordination.rss.required=true`), **не** отдельный env `COORDINATION_DEPLOYMENT_ROLE`.

Старт JVM:

- `rss.required=false` — в etcd пишутся yaml `active-region.home` / `value` / `status` (пустые → **r1** / **r1** / **ACTIVE**).
- `rss.required=true` **без** yaml-стартеров (`home` пустой) — первые значения **только из RSS** (иначе старт падает); seed для первого `?region=` — `rss.home-region`.
- `rss.required=true` **с** yaml-стартерами (`home` задан) — старт на yaml, дальше ждём RSS в фоне.

- **`active`** — regional + global мутации.
- **`stand_in`** — regional мутации **403**; global ок.
- **`switch`** — как stand_in для клиентов + участие в REGION_DRAIN и ускоренный purge старых ключей.

```bash
curl -sS -X PUT "$BASE/v1/active-region" \
  -H "Authorization: Bearer $SRE_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"region":"r1","status":"STANDBY"}'
```

(`region` в теле PUT — это **activeRegion** для short-path; home LES не меняется этим полем.)

## Сквозной сценарий: как жить с сервисом (клиентское приложение)

Ниже один «сквозной» сценарий для режима **active-region** (самый частый: один активный сегмент в etcd, короткие URL). Для **global** замените префиксы на `/v1/tenants/{tenant}/global/...` и используйте `GET .../global/workers/ids` — шаги те же по смыслу.

### 1. Выбор tenant и проверка active region

Один `tenantId` на изолированный контур (команду / продукт). Убедитесь, что вы знаете, какой logical active region сейчас (SRE задаёт через `PUT /v1/active-region`):

```bash
curl -sS "$BASE/v1/active-region" | jq .
# совместимый alias:
curl -sS "$BASE/v1/tenants/$TENANT/active-region" | jq .

# смена (нужен coordination.sre.stream-listen-token):
curl -sS -X PUT "$BASE/v1/active-region" \
  -H "Authorization: Bearer $SRE_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"region":"r1"}' | jq .
```

При смене **id** active region сначала смотрите SSE `GET .../stream/active-region` или JSON `GET /v1/active-region`: дождитесь **`regionalWritesBlocked: false`** (окончание drain после TTL старых ключей), затем перезапустите регистрации (координатор, воркеры, локи) в новом сегменте etcd.

### 2. Воркеры (каждый процесс / pod)

Каждый исполнитель один раз регистрируется и дальше шлёт heartbeat, пока жив:

1. `POST /v1/tenants/{tenant}/workers/register` с уникальным `workerId` и `ttlSeconds`. Повторная регистрация с тем же `workerId`, пока lease жив, даёт **HTTP 409** (`WORKER_EXISTS`).
2. Сохраните `token` из ответа.
3. Периодически `POST .../workers/{workerId}/heartbeat` с тем же `token`.
4. При остановке — `POST .../workers/{workerId}/deregister`.

### 3. Координатор (один активный на tenant в этом режиме)

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

Один и тот же API (`:acquire` / `:renew` / `:release`), **два контура** с разным смыслом при двух ЦОД:

| | **Regional** `…/jobs/{jobId}:…` | **Global** `…/global/jobs/{jobId}:…` |
|--|----------------------------------|--------------------------------------|
| **Где ключ в etcd** | `…/regions/{activeRegion}/jobs/…` | `…/global/jobs/…` |
| **Сколько владельцев** | Ровно один в **активном** логическом сегменте | Ровно один на **весь tenant** (оба ЦОД видят один ключ) |
| **На LES со `status=stand_in` / `switch`** | Мутации **403** (`STANDBY_DEPLOYMENT` / `SWITCH_DEPLOYMENT`) | Разрешены (если участник зарегистрирован в **global** пуле) |
| **Во время REGION_DRAIN** | **409** `REGION_DRAIN`, пока `regionalWritesBlocked` | **Не** блокируется drain’ом regional |
| **После смены `activeRegion`** | Старый сегмент «умирает» по TTL/release; в новом — **новый** acquire | Владение **не** рвётся из‑за switchover, пока жив lease |
| **Когда выбирать** | Работа только в «живом» ЦОД (batch в ACTIVE) | Exclusivity при переезде / active-active: один job на оба сайта |

Общие правила acquire (оба контура): **`holder`** — `coordinatorId` лидера или **`workerId`** из register **в том же** контуре; **`participantToken`** — токен acquire/register. При успехе — отдельный **`token`** лока для `:renew` / `:release`. Второй acquire при живом ключе — **409** `JOB_EXISTS`. Неверный участник — **403** `JOB_PARTICIPANT_INVALID`; без токена — **400** `JOB_PARTICIPANT_TOKEN_REQUIRED`. TTL **10…300** с.

**Список job воркера:** `GET …/workers/{workerId}/jobs` или `GET …/global/workers/{workerId}/jobs` — тот же **`token`**, что после register (`Authorization: Bearer` или **`X-Worker-Token`**). В теле только job с `holder` = этот `workerId`; lock-token в JSON опущен.

### 5b. Простые KV-ticket (global, без coordinator/worker/job)

`PUT /v1/tenants/{tenant}/global/kv/{keyId}` с телом `{"value":<любой JSON>,"ttlSeconds":10}` — создание (если ключа ещё нет). Без `ttlSeconds` ключ **без lease** (пока не удалите). Ответ **201** содержит **`accessToken`** — сохраните; он же нужен для `GET` / `DELETE` / обновления (`Authorization: Bearer` или **`X-Kv-Access-Token`**). При обновлении без `ttlSeconds` в теле сохраняется прежний lease (если был). Конфликт ревизий — **409** `KV_REVISION_CONFLICT`. Ключ уже есть при первом create — **409** `KV_EXISTS`.

### 6. Stand-in ЦОД

Если `GET …/active-region` на этом base URL даёт **`status: stand_in`** (или `switch`), **не** вызывайте на нём regional acquire/register/job (получите **403**). **GET** regional coordinator/workers/jobs без Bearer — **401**; с токеном regional лидера из etcd — чтение как на ACTIVE. `POST /v1/rings/assign` без tenant — без изменений. **Global** мутации и чтения — ок (Bearer **global** coordinator token для чтений пула).

### 7. Наблюдение

- `GET .../stream/workers` / `stream/jobs` — почти в реальном времени отражают изменения ключей в etcd; обычно нужен **тот же Bearer**, что для `GET .../workers` (токен coordinator). Для SRE можно задать **`coordination.sre.stream-listen-token`** (или env **`COORDINATION_SRE_STREAM_LISTEN_TOKEN`**) — тогда SSE workers/jobs/keys (regional + global workers/jobs) и **`PUT /v1/active-region`** принимают этот статический секрет в `Authorization: Bearer` или `X-Coordinator-Token`.
- `GET .../workers` / `workers/ids` — полные записи или только id; только с Bearer coordinator.

## Примеры REST

Допущения:

- `BASE=http://localhost:8080`
- `TENANT=team-a`
- Задайте logical region через SRE: `curl -X PUT "$BASE/v1/active-region" -H "Authorization: Bearer sre-stream-dev-token" -H 'content-type: application/json' -d '{"region":"r1"}'`

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

### Координатор: active region и global (для GET нужен Bearer)

```bash
# Лидер в текущем active region (тот же token, что для renew)
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

Смена active region (чтобы клиенты перерегистрировались):

```bash
curl -N "$BASE/v1/tenants/$TENANT/stream/active-region"
```

Текущий active region (JSON):

```bash
curl -sS "$BASE/v1/tenants/$TENANT/active-region" | jq .
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
