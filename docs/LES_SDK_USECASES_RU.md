# LES: playbook для команд (пути миграции + parallel execution)

Документ для тимлидов и продуктовых команд: **откуда вы пришли → куда вас ведёт платформа → как LES закрывает боль**.  
Сниппеты — публичный Java API `coordination-client` (`com.acme.coordination.sdk.les`). Короткий sketch: [`scripts/samples/les-client-sketch.md`](../scripts/samples/les-client-sketch.md).

---

## Зачем LES команде

Платформа переводит сервисы с одной VM на **Kubernetes и несколько DC**. Любой cron, claimer или миграция внезапно крутится на **нескольких подах сразу**. LES даёт общий контур:

- **эксклюзивность** (один победитель на работу);
- **раздачу параллельной обработки** между подами (JobQ, hashing, batch-locks);
- **failover** без зомби-leases (Drain);
- **общую метадату** между DC (GLOBAL KV: kill-switch, shard-map для router’ов).

**Контуры:** **REGIONAL** — namespace своего DC (`COORDINATION_REGION`); **GLOBAL** — один ключ на все площадки. Их не схлопывают.

**RSS** (кто active) — отдельный сервис. Сервер LES его **не** вызывает. Клиент при желании подключает RSS сам (`viaRss`) и зовёт Drain / `onlyWhen`.

### Что уже доступно в продукте

Нужные для adoption возможности уже в LES и Java SDK:

- freestanding lock: `lock(...).tryRun` / `tryHold` (с renew);
- JobQ: coordinator + workers + `hashRing()`;
- Drain (SRE) regional/global;
- GLOBAL KV: `flags()` put/get/watch (рубильник, shard-map);
- client-only RSS: `viaRss` + auto-drain;
- OpsWatch / MonitoringSse;
- job **metadata** (`Map<String,String>`) на acquire — для batch ownership (offset, ranges, произвольные поля).

---

## Подключение клиента

**Обычный app-pod SRE-токен не нужен.** `LesClient.connect(url)` достаточно для locks, JobQ, workers, hashing.

`.sreToken(...)` — **опционально**, только для ops/SRE: Drain, MonitoringSse, consume-all по `stream/keys`.

```java
import com.acme.coordination.sdk.les.LesClient;
import com.acme.coordination.sdk.session.ApiScope;
import com.acme.coordination.sdk.session.JobLease;
import com.acme.coordination.sdk.session.LeaseOptions;
import com.acme.coordination.ring.RingAssignment;
import java.time.Duration;
import java.util.Map;

// App pod — без SRE:
try (var les = LesClient.connect("http://les.local:8080")
        .homeRegion("r1")
        .build()) {
  les.tenant("payments").regional()
     .lock("nightly-reconcile")
     .tryRun(() -> reconcile());
}

// Ops / SRE / platform — токен только здесь:
try (var les = LesClient.connect("http://les.local:8080")
        .sreToken(System.getenv("COORDINATION_SRE_STREAM_LISTEN_TOKEN"))
        .homeRegion("r1")
        // .viaRss("http://rss:8080", "payments-rg")
        // .drainTenant("payments")
        .build()) {
  les.tenant("payments").drain("r1");
}
```

---

# Путь A — один процесс → много подов (часто ещё один DC)

*Боль уже от реплик в K8s; второй DC не обязателен.*

## Было → становится

**Было.** Один процесс на VM: ночной reconcile, публикация курсов, «я же один — очередь не нужна».

**Становится.** Deployment с **≥2 подами** (часто больше). Тот же cron стреляет **на каждом поде**. Очередь в БД, если появилась, видят все реплики.

Пример: 1 DC × 2 пода = уже гонка.

## Что ломается

- Двойные джобы (отчёты, письма, «уникальные» номера).
- Двойной stamp строк очереди.
- Самописный `INSERT INTO locks` хрупкий и не переживёт выход во 2-й DC.

## Паттерны LES на Пути A

| Паттерн | Зачем |
|---------|--------|
| **Exclusive lock / `tryRun`** | Один победитель для cron и one-shot |
| **JobQ + stamp `worker_id`** | Параллельная разборка очереди несколькими подами |
| **Hashing** | Разрезать keyspace без кривого `hash % N` |
| **OpsWatch** | Когда подов уже не два, а шесть |

### A1. Exclusive lock

```java
les.tenant("billing").regional()   // или .global() «на вырост»
   .lock("nightly-reconcile")
   .tryRun(() -> reconcile());     // остальные поды просто пропускают

les.tenant("billing").regional().lock("batch-x").tryHold().ifPresent(lease -> {
  try (JobLease held = lease) { /* renew в фоне */ }
  catch (Exception e) { throw new RuntimeException(e); }
});
```

### A2. JobQ — кто coordinator, кто worker (важно)

В JobQ участвуют **две роли**. Их путают чаще всего.

| Роль | Что делает под | Вызов SDK | Что SDK прячет |
|------|----------------|-----------|----------------|
| **Coordinator (claimer)** | Ровно **один** победитель штампует `worker_id` в вашей таблице | `becomeLeader("queue-claimer")` | **background renew** лидера |
| **Worker** | Каждый processing-под регистрируется и обрабатывает «свои» строки | `joinWorker(podId)` | **heartbeat renew** |

**Как выбирается claimer:** *каждый* claimer-под (или каждый под, у которого запущен claimer-loop) вызывает `becomeLeader` — это **гонка**. Победитель получает lease и штампует; остальные получают отказ / ждут, пока лидер жив. Вам не нужно вручную продлевать токен: renew крутится в фоне, пока `CoordinatorLease` не закрыт.

**Workers:** *каждый* worker-под вызывает `joinWorker` со своим id (обычно `HOSTNAME`). Пока сессия открыта, heartbeat идёт в фоне — иначе claimer не увидит под в `workers(leader).ids()` / `hashRing()`.

**Один JVM может делать оба loop’а** (claimer + worker в разных потоках). Обычно удобнее **разделить**: Deployment/роль «claimer» vs «worker», или два цикла в одном поде с разными обязанностями. Не смешивайте «я и лидер, и сразу думаю, что все поды — лидеры» — лидер один на имя роли.

#### Таблица `payment` до / после stamp

До (строки ничьи):

| id | amount | worker_id |
|----|--------|-----------|
| 1  | 100    | *null*    |
| 2  | 50     | *null*    |
| 3  | 75     | *null*    |
| 4  | 20     | *null*    |
| 5  | 200    | *null*    |

Живые workers в LES: `w-a`, `w-b`. Победивший claimer раздаёт:

| id | amount | worker_id |
|----|--------|-----------|
| 1  | 100    | **w-a**   |
| 2  | 50     | **w-b**   |
| 3  | 75     | **w-a**   |
| 4  | 20     | **w-b**   |
| 5  | 200    | **w-a**   |

#### End-to-end: два отдельных loop’а

```java
String podId = System.getenv().getOrDefault("HOSTNAME", "w-a");
var scoped = les.tenant("payments").regional();

// ============================================================
// LOOP 1 — COORDINATOR / CLAIMER (часто отдельный Deployment
// или отдельный поток). Запускайте на КАЖДОМ claimer-поде:
// все зовут becomeLeader → победитель один → только он штампует.
// ============================================================
void runClaimerLoop() throws Exception {
  // Гонка за уникальную роль "queue-claimer".
  // SDK сам renew’ит lease в background, пока leader не закрыт.
  try (var leader = scoped.becomeLeader("queue-claimer")) {
    // Сюда попал только победитель. Остальные поды сюда не входят
    // (или ждут/ретраят — по вашей политике обёртки).
    while (!Thread.currentThread().isInterrupted()) {
      var pool = scoped.workers(leader);       // нужен token лидера
      RingAssignment ring = pool.hashRing(); // живые worker id из LES

      for (Payment p : db.findUnassigned()) {
        String owner = ring.workerFor(p.id());
        // UPDATE payment SET worker_id = :owner
        //  WHERE id = :id AND worker_id IS NULL
        db.stampWorker(p.id(), owner);
      }
      Thread.sleep(500); // свой poll; renew лидера — не здесь, а в SDK
    }
  } // close() → stop renew; другой под сможет стать claimer’ом
}

// ============================================================
// LOOP 2 — WORKER (каждый processing-под). Не becomeLeader.
// joinWorker + heartbeat в фоне; читаем только свои строки.
// ============================================================
void runWorkerLoop() throws Exception {
  // Регистрация worker’а. SDK сам шлёт heartbeat renew в background.
  try (var me = scoped.joinWorker(podId)) {
    while (!Thread.currentThread().isInterrupted()) {
      // SELECT * FROM payment
      //  WHERE worker_id = :me AND status = 'NEW'
      db.processWhereWorker(me.workerId());
      Thread.sleep(200);
    }
  } // close() → deregister; claimer перестанет раздавать на этот id
}

// Типично в main пода:
//   if (role.equals("claimer")) runClaimerLoop();
//   else runWorkerLoop();
// Или оба потока в одном JVM — но это ДВА loop’а, не один смешанный блок.
```

**Почему так:** LES не хранит вашу очередь и не делает `SELECT FOR UPDATE` по строкам. Он даёт **одного claimer’а** (гонка `becomeLeader`) и **живой список workers** (`joinWorker`); stamp и обработка — в **вашей** БД → parallel execution.

---

# Путь B\* — с одной VM сразу на много подов **в двух DC**

*Укороченный сценарий: платформа сразу даёт 2 DC × ≥2 пода (≥4 процесса), без долгой фазы «только один DC».*

Боль та же, что на Пути A (гонки cron/очереди), плюс сразу вопрос **REGIONAL vs GLOBAL**:

- работа только с данными/очередью **своего** DC → **REGIONAL**;
- одна работа на весь продукт (nightly на весь мир) → **GLOBAL**;
- hot-standby на втором DC → `onlyWhen(rss::amIActive)` + Drain при flip (см. B5).

Практические сниппеты — те же, что A1/A2 и раздел **Parallel execution**, с выбором контура.

---

# Путь B — уже multi-instance в одном DC → ≥2 DC, много БД, AA или AA-partitioned

*Реплики уже были; новая боль — география данных, шарды и failover.*

## Было → становится

**Было.** Несколько подов в **одном** DC, часто **один большой Oracle**. Router’а «куда бьём» не было.

**Становится.** ≥2 DC; часто **много Postgres** вместо монолита; **router → executor → свой Postgres**; active/stand-in или уже active-active (AA) / AA с partition по шардам.

## Что ломается

- Поды чужого DC дерутся за строки, которых у них нет.
- Карта шардов в helm каждого router’а → рассинхрон после выкатки.
- RSS flip: старые leases живы → зомби-claimer до TTL.
- `hash % N` разъезжается при churn подов.

## Паттерны

| Паттерн | Роль |
|---------|------|
| **REGIONAL** locks / JobQ | Драка только внутри пула шарда / DC |
| **GLOBAL** lock / JobQ | Работа одна на весь продукт |
| **GLOBAL KV shard-map** | Одна карта ranges для всех router’ов (B7) |
| **Hashing в пуле** | Ring внутри REGIONAL (или GLOBAL при общем keyspace) |
| **Drain** | Чисто снять regional leases после flip / ухода DC |
| OpsWatch / KV kill-switch | Видимость и инциденты |

### B1. REGIONAL под свои данные

```java
try (var leader = les.tenant("payments").regional().becomeLeader("shard-claimer")) {
  var pool = les.tenant("payments").regional().workers(leader);
  String owner = pool.hashRing().workerFor(accountId);
}
```

### B2. Одна работа на мир → GLOBAL

```java
les.tenant("payments").global()
   .lock("rebuild-search-index")
   .tryRun(() -> rebuildGlobalIndex());
```

### B3. JobQ в региональной БД

Как A2, контур **REGIONAL**, stamp только в очередь **этого** DC.

### B4. Hashing внутри пула (не «один ring на оба DC» для partitioned данных)

```java
try (var leader = les.tenant("payments").regional().becomeLeader("balancer")) {
  var pool = les.tenant("payments").regional().workers(leader);
  RingAssignment[] ring = { pool.hashRing(128) };
  try (AutoCloseable watch = pool.onMembershipChanged(ids -> {
         try { ring[0] = pool.hashRing(128); } catch (Exception e) { throw new RuntimeException(e); }
       }, 1000)) {
    // if (ring[0].workerFor(id).equals(me)) handle(id);
  }
}
```

### B5. Drain при failover (RSS снаружи)

App-поды обычно **без** SRE; Drain делает ops-клиент или SDK с токеном при flip:

```java
// Ops / bootstrap с SRE + optional RSS:
try (var les = LesClient.connect("http://les-dc1:8080")
        .sreToken(sre)
        .homeRegion("r1")
        .viaRss("http://rss:8080", "payments-rg")
        .drainTenant("payments")
        .autoDrainOnFlip(true)
        .build()) {
  les.tenant("payments").regional()
     .onlyWhen(() -> les.rssOrNull() != null && les.rssOrNull().amIActive())
     .becomeLeader("shard-claimer");
}

// Ручной drain (нужен sreToken на клиенте):
les.tenant("payments").drain("r1");
```

### B6. Hot-standby

Боевые REGIONAL вызовы под `onlyWhen(() -> rss.amIActive())` + Drain на уходе — мост к Пути C.

### B7. Oracle → много Postgres: shard-map в GLOBAL KV

```text
клиент → [router × ≥2 на DC] → [executor shard-A | …] → свой Postgres
```

Control plane публикует одну map в GLOBAL KV; все router’ы читают/watch’ат её. Executor’ы в шарде остаются на **REGIONAL** JobQ/locks.

```java
var flags = les.tenant("payments").global().flags(); // GLOBAL-only

les.tenant("payments").global().lock("publish-shard-map").tryRun(() -> {
  try {
    flags.putJson("shard-map", shardMapJson);
  } catch (Exception e) { throw new RuntimeException(e); }
});

flags.get("shard-map").ifPresent(cache::replace);
try (var watch = flags.watch("shard-map", ev -> {
       if (ev.value() != null) cache.replace(ev.value());
     })) {
  // route(accountId) → executor
}

les.tenant("payments").regional()
   .lock("shard-compaction").tryRun(() -> compactLocalPg());
```

Drain снимает leases executor/claimer на уходящем DC; **shard-map в KV** обычно обновляют отдельно (новый `putJson`), а не удаляют «заодно с Drain».

---

# Путь C — Active / Stand-in → Active-Active

## Было → становится

**Было.** Один active DC по RSS; standby молчит (клиентский gate).  
**Стало AA:** оба DC боевые. Product-wide эксклюзивность — через **GLOBAL**; шарды БД по-прежнему **REGIONAL**.

| Тема | Stand-in (часто B) | Active-Active |
|------|--------------------|---------------|
| Кто работает | active DC | **все** DC |
| Одна работа на продукт | REGIONAL + RSS | **GLOBAL** |
| Очередь на шард | REGIONAL | REGIONAL |
| Рубильник | «погасить active» | **GLOBAL KV** flag |
| Shard-map | GLOBAL KV | та же map на всех router’ах |

### C1. GLOBAL lock вместо «только active»

```java
les.tenant("billing").global()
   .lock("nightly-reconcile")
   .tryRun(() -> reconcile());
```

### C2. GLOBAL JobQ

Те же **два loop’а**, что в A2, но контур `.global()`: claimer-поды гоняются за `becomeLeader("queue-claimer")` и штампуют; worker-поды только `joinWorker` + `WHERE worker_id = me`. Не склеивайте обе роли в один `try` без комментариев — см. end-to-end в A2.

```java
var g = les.tenant("payments").global();

// Claimer loop (гонка; победитель штампует):
try (var leader = g.becomeLeader("queue-claimer")) {
  var pool = g.workers(leader);
  db.stamp(row -> pool.hashRing().workerFor(row.id()));
}

// Worker loop (отдельный под / поток):
try (var me = g.joinWorker(podId)) {
  db.processWhereWorker(me.workerId());
}
```

### C3. Kill-switch на все DC

```java
var flags = les.tenant("payments").global().flags();
flags.put("payments-halt", Map.of("reason", "bad-fee"), Duration.ofHours(2));
if (flags.isSet("payments-halt")) return;
```

### C4. OpsWatch / Monitoring (SRE)

```java
// App: OpsWatch с токеном своего coordinator (не SRE):
try (var leader = les.tenant("payments").global().becomeLeader("ops");
     var watch = les.tenant("payments").global().opsWatch(leader.token())) {
  watch.onWorkers(System.out::println).onJobs(System.out::println);
}

// Platform: MonitoringSse — нужен SRE token:
try (var mon = com.acme.coordination.sdk.les.MonitoringSse
        .open("http://les.local:8080", sreToken, "payments")) {
  mon.onEtcdActivity(System.out::println);
}
```

### C5. Drain на AA

Для maintenance/аварии площадки: `les.tenant(t).drain(regionId)` (SRE). День за днём product-wide exclusivity держит **GLOBAL**, не RSS gate.

---

# Parallel execution — как LES помогает сделать параллельную обработку

**Питч для команды:** раньше один процесс на VM тянул всю очередь/батч. Теперь 2–4+ пода (один DC, hot-standby или AA). LES не заменяет вашу БД — он **раздаёт ответственность** между подами так, чтобы они работали **параллельно без двойной обработки**.

Ниже три рабочих способа (их можно комбинировать).

## 1) Hash / ranges: worker «владеет» частью ID

Каждый worker считает `ring.workerFor(id)` и обрабатывает только «свои» ключи. Подходит, когда нет центрального claimer’а или keyspace уже известен.

```java
try (var leader = les.tenant("payments").regional().becomeLeader("balancer");
     var me = les.tenant("payments").regional().joinWorker(podId)) {
  var pool = les.tenant("payments").regional().workers(leader);
  RingAssignment ring = pool.hashRing(128);

  for (long id : db.scanCandidateIds()) {
    if (!ring.workerFor(id).equals(me.workerId())) continue;
    process(id);
  }
}
```

Плюс: мало координации в БД. Минус: нужна аккуратность при смене membership (пересчёт ring / `onMembershipChanged`).

## 2) JobQ stamp `worker_id` (разбор очереди)

См. **A2** (таблица ролей + два loop’а): *все* claimer-поды зовут `becomeLeader` (гонка, renew в SDK); *все* worker-поды зовут `joinWorker` (heartbeat в SDK); штампует только победитель; workers читают `WHERE worker_id = me`.

Плюс: явное назначение в БД. Минус: нужен живой claimer.

## 3) Batch locks + **dynamic metadata** на job

**Не цель LES** — лок на каждую строку БД. Цель — **батч**: под (или claimer) берёт job-lock и в **metadata** пишет, *какой* диапазон/offset он обрабатывает. Другие поды видят занятость и берут соседние батчи.

На acquire job сервер уже принимает `metadata: Map<String, String>` (REST/gRPC). Через sessions:

```java
import com.acme.coordination.sdk.session.CoordinationSessions;

Map<String, String> meta = Map.of(
    "offsetStart", "105",
    "offsetEnd", "1005",
    "batchSize", "900",
    "table", "payment",
    "note", "rows 105..1005 inclusive"
);

try (var sessions = les.sessions(); // или les.rest().sessions()
     var batch = sessions.holdJob(
         "payments",
         "payment-batch-105-1005",  // id батча = уникальный ключ ownership
         podId,
         /* participantToken */ workerTokenOrNullForFreestanding,
         meta,
         ApiScope.REGIONAL,
         LeaseOptions.ofTtlSeconds(60),
         true)) {
  // Этот под — единственный владелец батча, пока держит renew.
  db.processRange(105, 1005);
}
```

Freestanding (без worker) — тот же `holdJob` с `participantToken = null` и своим `holderLabel`.

Прочитать чужой батч (кто держит, какие meta) — `GET .../jobs/{id}` под токеном coordinator/worker (секреты lease в ответе чтения опускаются; **metadata** доступна для наблюдаемости/отладки).

**Как выбирать job id:**

| Схема | Пример id | Когда |
|-------|-----------|--------|
| Диапазон в id | `pay-batch-0-999`, `pay-batch-1000-1999` | Фиксированные окна |
| Offset-окно | `pay-off-105` + meta `offsetEnd` | Скользящее окно |
| Произвольный батч | `pay-reprice-2026-03` + meta | Разовые кампании |

Простой фасад `lock(id).tryRun(...)` удобен для **одной** эксклюзивной работы без meta. Для **parallel batches** используйте `sessions.holdJob(..., metadata, ...)` (или REST `acquire` с телом `metadata`) — так на ключе LES живут динамические поля батча.

### Сводка: три пути к parallelism

| Способ | Кто решает «мою долю» | Где хранится раздача | Типичный кейс |
|--------|------------------------|----------------------|----------------|
| Hash ring | каждый worker локально | ring из membership LES | сканы ID, reprice по счетам |
| JobQ stamp | coordinator | колонка в **вашей** БД | очередь платежей / outbox |
| Batch job + metadata | acquire батч-ключа | **metadata** на job в LES | окна offset/range без row-lock |

---

# Карта паттернов → пути

| Паттерн | A (1→N подов) | B\* (сразу 2 DC) | B (1DC→≥2DC / Oracle→PG) | C (stand-in→AA) |
|---------|---------------|------------------|---------------------------|-----------------|
| Exclusive `tryRun` | ★ | ★ | GLOBAL или REGIONAL | ★ GLOBAL product-wide |
| JobQ stamp | ★ очередь | ★ | ★ REGIONAL на шард | GLOBAL очередь; REGIONAL шарды |
| Hash parallelism | опционально | да | ★ в пуле шарда | ★ GLOBAL pool если общий keyspace |
| Batch job + metadata | да | да | да | да |
| Drain | редко | при flip | ★ | maintenance DC |
| onlyWhen(RSS) | — | hot-standby | ★ | убрать для AA-ролей |
| GLOBAL KV flag / shard-map | редко | map если уже routers | ★ | ★ |

---

## Одной фразой

- **A:** с одной VM на ≥2 пода — сначала **lock**, для очереди — **JobQ stamp**.  
- **B\*:** сразу много подов в 2 DC — те же примитивы, сразу выбрать REGIONAL/GLOBAL.  
- **B:** шарды и router’ы — **REGIONAL** у executor’ов, **GLOBAL KV map** у router’ов, **Drain** на flip.  
- **C:** AA — **GLOBAL** locks/JobQ/рубильник; REGIONAL не выкидывать для локальных БД.  
- **Parallel execution:** hashing, JobQ stamp или **batch job + metadata** — LES раздаёт ownership, БД остаётся вашей.
