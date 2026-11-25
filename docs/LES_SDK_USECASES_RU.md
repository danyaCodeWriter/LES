# LES: playbook по путям миграции команд

Для тимлидов: не «список фич», а **откуда вы пришли → куда вас ведёт платформа → какие паттерны LES закрывают боль**. Паттерны (JobQ, lock, hashing, Drain, OpsWatch, KV, **shard map для router’ов**) встроены в пути A / B / C.

Сниппеты ниже соответствуют публичному API модуля **`coordination-client`** (`com.acme.coordination.sdk.les`) на `main`. Короткий sketch: [`scripts/samples/les-client-sketch.md`](../scripts/samples/les-client-sketch.md).

---

## Общий пререквизит платформы

| | Было у многих команд | Стало |
|--|----------------------|--------|
| Рантайм | 1 VM / один процесс | **Kubernetes, несколько DC** |
| Реплики | Можно было «один инстанс» | Платформа: **≥ ~2 пода на DC** → **всегда multi-instance** |
| Следствие | «Эксклюзивно» = просто единственный процесс | Любой cron/claimer/migration **гоняется с собой** (2 DC × 2 пода ≥ 4 процесса) |

**RSS** (кто active) — отдельный сервис. Сервер LES **никогда** от него не зависит. Java-клиент может читать RSS (`LesClient.viaRss`) и звать Drain / `onlyWhen`.

**Контуры LES:** **GLOBAL** = один ключ на все DC; **REGIONAL** = namespace этого DC/LES (`COORDINATION_REGION`). Не схлопывать.

**Почему LES:** stretched etcd на 3 DC; в компании нет общего leader-election / unique-key сервиса.

### Статус реализации (код в этом репо)

| Фича | Статус |
|------|--------|
| Freestanding job (`participantToken` optional) | ✅ server REST/gRPC + OpenAPI/proto |
| Drain regional/global (SRE token) | ✅ + `DrainAuthWebE2eTest` |
| Coordinator release | ✅ REST/gRPC |
| `LesClient` / `lock().tryRun` **with renew** / `tryHold` | ✅ |
| `viaRss` / `drainTenant` (client-only) | ✅ |
| OpsWatch / MonitoringSse / `flags()` + **`watch`** | ✅ |
| JobQ `workers(leader).hashRing()` + membership poll | ✅ |
| REST retry / simple circuit (`RestCallPolicy`) | ✅ |
| Samples | ✅ `scripts/samples/les-client-sketch.md` |

### Подключение клиента

```java
import com.acme.coordination.sdk.les.LesClient;
import com.acme.coordination.sdk.session.JobLease;
import com.acme.coordination.ring.RingAssignment;
import java.time.Duration;
import java.util.Map;

try (var les = LesClient.connect("http://localhost:8080")
        .sreToken("sre-stream-dev-token")   // drain / MonitoringSse / stream/keys
        .homeRegion("r1")
        // Optional RSS (client-only; Les server never calls RSS):
        // .viaRss("http://rss:8080", "payments-rg")
        // .drainTenant("payments")
        // .autoDrainOnFlip(true)
        .build()) {
  // …
}
```

---

# Путь A — были single-instance на VM → теперь K8s multi-instance (≥2 пода)

*Пока можете оставаться в одном DC; боль уже от реплик, не обязательно от второй БД.*

## Было → становится

**Было.** Один процесс на VM: ночной reconcile, публикация курсов, иногда простая очередь «я же один». Leader election не нужен.

**Становится.** Сервис в K8s: **минимум ~2 пода** (часто больше). Тот же cron аннотирован на Deployment — стреляет **на каждом поде**. Очередь в БД, если появилась, видят все реплики.

Пример: 1 DC × 2 пода = уже гонка; скоро 2 DC × 2 = ≥4.

## Проблемы перехода

- Двойные джобы (отчёты, письма, «уникальные» номера).
- Двойной stamp строк очереди / перетирание `worker_id`.
- Самописный `INSERT INTO locks` в одной БД ещё как-то живёт в одном DC, но команда всё равно тащит хрупкий код; при выходе во 2-й DC (Путь B) он развалится.

## Какие паттерны LES

| Паттерн | Зачем на Пути A |
|---------|------------------|
| **Exclusive lock / `tryRun`** | Вернуть «один победитель» для cron и one-shot |
| **JobQ squaring** (если выросла очередь) | Один claimer + живые workers + stamp в **своей** БД |
| Hashing (опционально) | Разрезать keyspace/очередь между ≥2 workers без кривого `hash % N` |
| OpsWatch (по мере роста) | Видеть, кто лидер, когда подов уже не два, а шесть |

GLOBAL vs REGIONAL на Пути A: пока один DC — часто достаточно **REGIONAL** (один LES) или даже GLOBAL «на вырост»; оба контура бьют локальный пул реплик. Главное — **появился lock**, которого не было на VM.

## Как решаем (Java)

### A1. Exclusive lock (главный первый шаг с VM)

```java
// Раньше: просто reconcile() в main одной VM
// Теперь: каждый под по cron — freestanding lock с background renew
les.tenant("billing").regional()   // или .global() на вырост
   .lock("nightly-reconcile")
   .tryRun(() -> reconcile());     // остальные поды — Optional.empty / skip

// Долгое удержание (закрыть вручную):
les.tenant("billing").regional().lock("batch-x").tryHold().ifPresent(lease -> {
  try (JobLease held = lease) {
    // held.isHeld() пока renew успешен
  } catch (Exception e) {
    throw new RuntimeException(e);
  }
});
```

Тот же примитив для миграции схемы из job: `lock("schema-migrate-v42").tryRun(migrate)` — LES не мигрирует схему, только не даёт прогнать DDL двум подам.

### A2. Появилась очередь в БД → JobQ

```java
String podId = System.getenv().getOrDefault("HOSTNAME", "w1");

try (var me = les.tenant("billing").regional().joinWorker(podId)) {
  db.processWhereWorker(me.workerId());
}

try (var leader = les.tenant("billing").regional().becomeLeader("queue-claimer");
     var unused = les.tenant("billing").regional().joinWorker(podId)) {
  var pool = les.tenant("billing").regional().workers(leader);
  RingAssignment ring = pool.hashRing(); // default 128 vnodes
  db.stampNew(row -> ring.workerFor(row.id()));
}
```

Stamp — ваша БД; LES только claimer + membership + ring.

## Зачем команде на Пути A

**Платформа отняла «единственный процесс» — LES возвращает эксклюзивность одной строкой API**, до того как вы вообще поедете во второй DC.

---

# Путь B — были multi-instance, но один DC / одна БД → теперь ≥2 DC и много БД

*Реплики уже были; новая боль — география данных, шардирование и failover.*

## Было → становится

**Было.** Несколько подов в **одном** DC, часто **один большой Oracle** (или одна БД): все клиенты/счета в одном месте. Может, уже был свой DB-lock. Очередь stamp’илась локально. Router’а «куда бьём» не было — всё и так в одном инстансе БД.

**Становится.** ≥2 DC; платформа/миграция: **много маленьких Postgres** (и т.п.) вместо монолита. Команда **обязана шардировать**: диапазоны id, регионы, витрины — у каждого executor’а своя БД. Спереди появляются **router**-сервисы (≥2 пода на DC), сзади — **executor**-сервисы в «своём» шарде. Плюс hot-standby или active на площадках.

Пример: 2 DC × ≥2 router-пода + N executor-пулов; без общей карты маршрутов каждый router держит свой yaml и разъезжается после выкатки.

## Проблемы перехода

- GLOBAL-драка подов чужого DC за строки, которых у них в БД нет (или есть stale read).
- Два regional «мира» без правил — или наоборот, всё затолкали в один GLOBAL и сломали шарды.
- **Карта шардов** размножена в конфигах router’ов → рассинхрон, «тихие» мисроуты на чужой Postgres.
- Failover/смена active (RSS): старые leases на уходящем DC живы → зомби-claimer, ожидание TTL.
- Свой `hash % N` при churn подов обоих DC разъезжается.
- Он-колл не видит, в каком DC жив лидер / какая версия shard-map сейчас у router’ов.

## Какие паттерны LES

| Паттерн | Роль на Пути B |
|---------|----------------|
| **REGIONAL locks / JobQ** | Шард уже в региональной БД → дерутся только поды **этого** DC / executor-пула |
| **GLOBAL lock / JobQ** | Когда работа **одна на весь продукт** (редко для stamp локальной очереди) |
| **GLOBAL KV shard map** | Общая карта ranges/маршрутов для **всех** router’ов перед executor’ами (кейс B7) |
| **Hashing внутри пула** | Ring над workers одного контура (обычно REGIONAL пул executor’ов) |
| **Drain** | После RSS flip / ухода DC — чисто снять regional namespace |
| **JobQ squaring** | Claimer+workers в выбранном контуре; stamp в **той** БД, где очередь |
| OpsWatch | Кто claimer; смена keys shard-map |
| KV kill-switch | Инциденты; см. также Путь C |

**Кто владеет partition map.** Бизнес-правило «какой id в какой Postgres» задаёт **команда** (миграция с Oracle). LES **не** придумывает шарды; LES даёт **общее хранилище карты** (GLOBAL KV) + контур contention (REGIONAL/GLOBAL) для исполнителей.

## Как решаем (Java)

### B1. Partitioned: REGIONAL под свои данные

```java
try (var leader = les.tenant("payments").regional().becomeLeader("shard-claimer")) {
  var pool = les.tenant("payments").regional().workers(leader);
  RingAssignment ring = pool.hashRing();
  String owner = ring.workerFor(accountId); // account гарантированно «наш» DC
}
```

Contrast: **не** `.global()` для повседневного stamp этой БД.

### B2. Работа на весь мир → GLOBAL

```java
les.tenant("payments").global()
   .lock("rebuild-search-index")
   .tryRun(() -> rebuildGlobalIndex());
```

### B3. JobQ в региональной БД

Как сценарий A2, но контур **REGIONAL** и stamp только в локальную очередь DC. Workers регистрируются в том же REGIONAL.

### B4. Hashing — в чём прикол на Пути B

В одном DC уже болели от `hash % N` при scale. В двух DC соблазн сделать один глобальный ring на 6 подов — для **partitioned данных** это ошибка. Правильно: ring **внутри** REGIONAL пула DC1 (2+ пода) для ключей западной БД; отдельно ring DC2.

```java
try (var leader = les.tenant("payments").regional().becomeLeader("balancer")) {
  var pool = les.tenant("payments").regional().workers(leader);
  RingAssignment[] ring = { pool.hashRing(128) };
  try (AutoCloseable watch = pool.onMembershipChanged(ids -> {
         try { ring[0] = pool.hashRing(128); } catch (Exception e) { throw new RuntimeException(e); }
       }, 1000)) {
    // меньше stamp-логики: if (ring[0].workerFor(id).equals(me)) handle(id);
  }
}
```

### B5. Drain при failover (RSS снаружи)

```java
// Вариант 1 — SDK сам drain’ит предыдущий region при flip:
try (var les = LesClient.connect("http://les-dc1:8080")
        .sreToken(sre)
        .homeRegion("r1")
        .viaRss("http://rss:8080", "payments-rg")
        .drainTenant("payments")
        .autoDrainOnFlip(true)
        .build()) {
  // боевые REGIONAL вызовы только когда home == active:
  les.tenant("payments").regional()
     .onlyWhen(() -> les.rssOrNull() != null && les.rssOrNull().amIActive())
     .becomeLeader("shard-claimer");
}

// Вариант 2 — вручную после flip (stretched etcd: любой LES чистит любой regionId):
les.tenant("payments").drain("r1"); // Bearer SRE из LesClient.sreToken
```

Без renew lease умрёт сам по TTL; **Drain** — чтобы плановый flip не ждал TTL и не ловил зомби. Сервер LES RSS не читает.

### B6. Hot-standby внутри Пути B

Если второй DC — hot-standby (не обрабатывает свой шард, а ждёт active): боевые REGIONAL вызовы только под `onlyWhen(() -> les.rssOrNull().amIActive())` + Drain на уходе. Это мост к Пути C, пока ещё не AA.

### B7. Oracle → много Postgres: shard ranges в GLOBAL KV для router → executor

#### Боль

С монолитного Oracle уезжают на **пачку маленьких Postgres** в новых DC. Появляется слой:

```text
клиент → [router × ≥2 на DC] → [executor shard-A | shard-B | …] → свой Postgres
```

Без общего места карта `accountId ranges → executor/shard/DC` живёт в helm-values каждого router’а. Выкатили половину router’ов — половина трафика ещё на старых ranges → **мисроут**, 404, запись не в ту БД.

#### Паттерн LES: GLOBAL KV как shared routing map

1. **Control plane** публикует в **GLOBAL KV** документ: ranges, shard id, endpoint executor’а, version.
2. **Все router-поды во всех DC** читают один и тот же ключ (`get` / `watch`) и кэшируют map локально.
3. Запрос: `route(key) → shard → executor`. Router **не** держит source-of-truth в своём деплое.
4. Исполнители в шарде берут **REGIONAL** locks/JobQ/hashing **внутри** своего пула (B1–B4).
5. Смена map: новый документ в KV → router’ы обновляются по watch.

LES здесь — **не** discovery Kubernetes и **не** замена service mesh; это **согласованная метадата маршрутизации** на stretched etcd между DC.

#### Java sketch

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

var json = new ObjectMapper();
var flags = les.tenant("payments").global().flags(); // GLOBAL-only

// --- control plane (редко; один победитель) ---
les.tenant("payments").global()
   .lock("publish-shard-map")
   .tryRun(() -> {
      ObjectNode map = json.createObjectNode();
      map.put("version", 3);
      // … ranges / executors …
      try {
        flags.putJson("shard-map", map); // без TTL
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
   });

// --- каждый router pod (все DC) ---
flags.get("shard-map").ifPresent(cache::replace);
try (var watch = flags.watch("shard-map", ev -> {
       if (ev.value() != null) cache.replace(ev.value());
     })) {
  // route(accountId) → executor из cache
}

// --- executor в своём шарде (REGIONAL) ---
les.tenant("payments").regional()
   .lock("shard-compaction").tryRun(() -> compactLocalPg());
```

#### Watch / SSE — как router’ы обновляются

| Способ | Смысл |
|--------|--------|
| Короткий poll `flags.get("shard-map")` | Просто; лаг = период poll |
| **`flags.watch("shard-map", …)`** поверх SSE `stream/keys` | Router видит PUT/DELETE без деплоя |
| `MonitoringSse` (SRE token) | Платформа видит активность etcd keys |

#### Топология и Drain

| Вопрос | Ответ |
|--------|--------|
| Почему **GLOBAL** KV? | Router’ы есть во **всех** DC; карта одна на продукт |
| Executor locks | **REGIONAL** (данные шарда локальны) |
| Публикация map | **GLOBAL** lock `publish-shard-map` |
| Drain | Снимает regional leases **executor/claimer** на уходящем DC; **не** обязан удалять GLOBAL shard-map. После ухода DC control plane **обновляет** map тем же `putJson` |
| RSS | Только если control/router на stand-in не должен публиковать; сервер LES RSS не знает |

## Зачем команде на Пути B

**Согласовать LES с уже разрезанными БД и с распилом Oracle→Postgres:** локальная драка там, где лежат данные; **общая GLOBAL map** для router’ов; failover executor’ов — через Drain; смена ranges — через KV, а не через деплой половины флота.

---

# Путь C — были Active / Stand-in (hot-standby) → теперь Active-Active

*Обе площадки боевые; «только active работает» больше не модель дня.*

## Было → становится

**Было (часто Путь B в режиме stand-in).** Один active DC по RSS; standby прогрет, но **не** брал боевые locks (клиентский gate). JobQ/compaction — REGIONAL на active. Drain при flip.

**Становится. Active-Active:** оба DC принимают работу. Нельзя полагаться на «standby просто молчит». Нужна **глобальная** эксклюзивность там, где сущность одна на продукт; и по-прежнему REGIONAL там, где данные остались локальными (гибрид с Пути B).

Пример: 2–3 DC × ≥2 пода; платежный процессор активен везде; один nightly reconcile на весь продукт; kill-switch должен гасить **все** площадки сразу.

## Что меняется относительно Пути B

| Тема | Путь B (часто + stand-in) | Путь C (Active-Active) |
|------|---------------------------|-------------------------|
| Кто работает | В основном active DC | **Все** DC |
| Типичный lock для «одной работы на продукт» | REGIONAL + RSS gate | **GLOBAL** (все поды в драке) |
| JobQ на весь мир | Редко / через active | **GLOBAL** workers + claimer |
| JobQ на шард БД | REGIONAL | REGIONAL **остаётся** |
| Failover | Drain + RSS central | RSS менее «выключатель работы»; Drain для ухода DC |
| Рубильник инцидента | Иногда хватало «погасить active» | Нужен **GLOBAL KV** kill-switch на все DC |
| Shard map (Oracle→PG) | GLOBAL KV для router’ов | В AA router’ы активны везде — **тот же GLOBAL map** |
| Наблюдаемость | Важно | Ещё важнее: лидеры, locks и **версия shard-map** |

## Проблемы перехода

- Привычка `onlyWhen(active)` оставляет целый DC без работы или наоборот — забыли убрать gate и AA «не взлетел».
- REGIONAL lock думают, что он «глобальный» — получают по победителю на DC = N победителей.
- Инцидент на комиссии: выключили «active» в голове, а второй DC продолжает списывать.
- Hashing только по старому active-пулу → дисбаланс после AA.

## Какие паттерны LES

| Паттерн | Роль на Пути C |
|---------|----------------|
| **GLOBAL exclusive lock** | Одна работа на весь AA-пул (≥4–6 подов) |
| **GLOBAL JobQ** | Одна логическая очередь / общий claimer на мир |
| **REGIONAL** | Сохраняется для региональных БД/шардов (как Путь B) |
| **Hashing across AA pool** | Ring над **GLOBAL** workers, если keyspace общий |
| **GLOBAL KV kill-switch** | Freeze на всех DC разом |
| **GLOBAL KV shard map** (B7) | Router’ы в AA на всех DC читают одну map |
| **OpsWatch / Monitoring SSE** | Кто держит GLOBAL vs REGIONAL; кто переписал shard-map |
| Drain | Уход площадки на обслуживание; map в KV обновляют отдельно |

## Как решаем (Java)

### C1. Вместо «только active» → GLOBAL lock

```java
// Было (stand-in):
// regional().onlyWhen(rss::amIActive).lock("reconcile").tryRun(...)

// Стало (AA):
les.tenant("billing").global()
   .lock("nightly-reconcile")
   .tryRun(() -> reconcile());   // 2 DC × 2+ пода → один победитель
```

### C2. GLOBAL JobQ

```java
try (var leader = les.tenant("payments").global().becomeLeader("queue-claimer");
     var w = les.tenant("payments").global().joinWorker(podId)) {
  var pool = les.tenant("payments").global().workers(leader);
  dbGlobalOrShared.stamp(row -> pool.hashRing().workerFor(row.id()));
}
```

Если очередь всё ещё в региональной БД — остаётесь на REGIONAL JobQ (Путь B); AA не отменяет шарды.

### C3. Hashing across AA pool

```java
try (var leader = les.tenant("payments").global().becomeLeader("balancer")) {
  var pool = les.tenant("payments").global().workers(leader);
  RingAssignment ring = pool.hashRing();
  try (AutoCloseable watch = pool.onMembershipChanged(ids -> { /* recompute / metrics */ }, 1000)) {
    // workers из всех DC в одном GLOBAL пуле
  }
}
```

### C4. Kill-switch на все DC (GLOBAL KV)

**История:** баг комиссии; процессоры активны в AA на обеих площадках. Дежурный:

```java
var flags = les.tenant("payments").global().flags();
flags.put("payments-halt", Map.of("reason", "bad-fee"), Duration.ofHours(2));

// каждый под в каждом DC:
if (flags.isSet("payments-halt")) return;
```

Раньше «погасить active» могло остановить stand-in мир; в AA без GLOBAL флага второй DC продолжает жечь деньги.

### C4b. Shard map в AA (тот же B7)

В stand-in ошибочно обновляли yaml «только на active». В AA **все** router-поды шлют трафик → map **обязана** жить в GLOBAL KV + `flags.watch`. Rebalance ranges = новый `putJson("shard-map")`, не dual deploy. Executor’ы по шардам — по-прежнему REGIONAL.

### C5. OpsWatch / Monitoring

```java
try (var leader = les.tenant("payments").global().becomeLeader("ops");
     var watch = les.tenant("payments").global().opsWatch(leader.token())) {
  watch.onWorkers(line -> metrics.ingest("workers", line))
       .onJobs(line -> metrics.ingest("jobs", line))
       .onKeys(line -> metrics.ingest("keys", line));
}

try (var mon = com.acme.coordination.sdk.les.MonitoringSse
        .open("http://localhost:8080", "sre-stream-dev-token", "payments")) {
  mon.onEtcdActivity(ev -> metrics.ingest("etcd", ev));
}
```

### C6. Drain на Пути C

Не исчезает: вывод DC на maintenance / авария площадки — `les.tenant(t).drain(regionId)` по-прежнему нужен. Но **день за днём** эксклюзивность продукта держит GLOBAL, а не RSS gate.

---

# Карта паттернов → пути

| Паттерн | Путь A (VM→K8s) | Путь B (1DC→≥2DC / Oracle→много PG) | Путь C (stand-in→AA) |
|---------|-----------------|--------------------------------------|----------------------|
| Exclusive `tryRun` | ★ первый шаг | GLOBAL или REGIONAL | ★ GLOBAL для product-wide |
| JobQ claimer+workers | если выросла очередь | ★ REGIONAL на шард; GLOBAL редко | ★ GLOBAL очередь; REGIONAL шарды |
| Hashing | опционально | ★ в regional пуле executor’ов | ★ в GLOBAL пуле если общий keyspace |
| Drain | редко | ★ failover / RSS | уход DC / maintenance |
| onlyWhen(RSS) | — | ★ hot-standby | убрать для AA-ролей |
| GLOBAL KV kill-switch | редко | инциденты | ★ kill-switch AA |
| **GLOBAL KV shard map** | — | ★ **router→executor ranges** | ★ те же router’ы на всех DC |
| OpsWatch | по росту | нужно (+ version map) | ★ обязательно |

---

# Одной фразой по путям

- **A:** с VM на ≥2 пода — сначала **exclusive lock**, при очереди — **JobQ**.
- **B:** с одной БД (часто Oracle) на ≥2 DC / много Postgres — **REGIONAL** у executor’ов, **GLOBAL KV shard-map** у router’ов, **Drain** на flip, hashing **внутри** пула шарда.
- **C:** уход от stand-in — **GLOBAL** locks/JobQ/hash, **KV рубильник + та же shard-map**, OpsWatch; REGIONAL не выкидывать, если шарды БД остались.

---

## Не делаем на сервере LES

Интеграцию RSS; active-гейты; stamp чужих БД; отдельные Cron/Migration API; схлопывание GLOBAL/REGIONAL.
