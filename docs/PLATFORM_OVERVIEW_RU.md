
## <span style="color:#5E4DB2">1. Назначение</span>

Сервис даёт **общий слой координации** для приложений в **multi-DC**: выбор лидера (coordinator), реестр воркеров с lease, job-lock’и с проверкой участника, **глобальный** контур (один coordinator и пул workers/jobs на tenant между ЦОД), **fencing epoch**, наблюдение за сменой active region (REST + SSE), опционально **идемпотентность REST** на выбранных мутациях.

Контракт REST описан в **OpenAPI** спецификации сервиса (файл `openapi/openapi.yaml` в репозитории); gRPC — в `proto/coordination/v1/coordination.proto`.

---

## <span style="color:#5E4DB2">2. Контекст: переезд с single-DC на 2 DC</span>

| <span style="color:#5E4DB2;font-weight:600">Было (один активный ЦОД)</span> | <span style="color:#5E4DB2;font-weight:600">Стало (два ЦОД)</span> |
|--------------------------|-----------------|
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Один «активный» кластер приложений, глобально согласованное состояние локально очевидно</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Два LES (home **r1** / **r2**) + **общий stretched etcd**; один ЦОД **active**, второй **stand_in**</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Один base URL и один смысл «кто лидер / кто владеет задачей»</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Клиент ходит на свой DC или follow ACTIVE; regional mutate только при `status=active`; cross-DC exclusivity → **global**</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Редко нужен явный fencing к внешним системам</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">После split-brain или lag сети **старый** процесс может ещё писать в БД/очередь — нужны **epoch / lease** и политика отказа</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">«Просто acquire» после рестарта</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">После смены `activeRegion`: ждать `regionalWritesBlocked=false`, затем **заново** register/acquire в новом сегменте</span> |

### <span style="color:#216E4E">Playbook клиента (минимум)</span>

1. `GET /v1/active-region` (или SSE) на том base URL, куда шлёте API. Поля: **`region`** = home этого LES, **`activeRegion`** = кто serves short-path, **`status`** = POV home (`active` / `stand_in` / `switch`).
2. Если `status != active` — **не** делайте regional mutate (**403**). Для «один job на оба ЦОД» используйте **`/global/jobs/...`**.
3. Если `regionalWritesBlocked` — ждите конца drain (**409** `REGION_DRAIN` на regional lease).
4. Выберите контур job: **regional** vs **global** (см. §5.3).

Платформа **не заменяет** БД и очереди, но даёт **согласованные примитивы** поверх **etcd**: lease + CAS + durable epoch, единый API для команд, мигрирующих с «один DC — один смысл active».

---

## <span style="color:#5E4DB2">3. Гарантии (что можно честно обещать)</span>

| <span style="color:#5E4DB2;font-weight:600">Область</span> | <span style="color:#5E4DB2;font-weight:600">Гарантия / механизм</span> |
|---------|---------------------|
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Лидер regional / global coordinator</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Ключ лидера в etcd создаётся только если **нет** текущего значения (txn + `create_revision == 0`); значение привязано к **lease**; renew только с **token** из acquire</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Потеря данных при нормальной работе</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Состояние в etcd; при **renew** и heartbeat данные **обновляются**, не «теряются» произвольно. Исчезновение ключа = **истечение TTL** или явный **release/deregister**, не молчаливый сброс</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Два лидера одной роли</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Пока lease старого лидера **жив**, второй acquire той же роли **не** получит ключ (409 / not acquired в семантике API)</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Fencing</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Монотонный **`fenceEpoch`** на acquire coordinator (regional и global) — для отсечения **устаревших** писателей во внешние системы</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Job-lock</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">**:acquire** проверяет, что `holder` — известный coordinator **или** зарегистрированный worker, и **`participantToken`** совпадает с секретом этого участника; отдельный **job `token`** на renew/release</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Чтение пулов</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">GET coordinator / workers / jobs (списки и один job), SSE workers/jobs — с **токеном чтения** лидера coordinator (**Bearer** или `X-Coordinator-Token`). **GET одного job** дополнительно: **токен регистрации worker-holder** (Bearer или `X-Worker-Token`)</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Смена active region</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">**Drain**: `regionalWritesBlocked` в `GET …/active-region` и SSE; пока drain активен — **409 REGION_DRAIN** на regional мутации; после очистки lease-ключей старого региона (кроме `coordinationEpoch`) — запись в новый сегмент</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">STANDBY / SWITCH</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`status` в etcd POV **home** этого LES (`stand_in` / `switch`): regional мутации **403**; **global** разрешены. **SWITCH** — участие в принудительной очистке draining-сегмента (операторская осторожность)</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Идемпотентность REST</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">При `coordination.idempotency.enabled=true`: заголовок `Idempotency-Key` / `X-Idempotency-Key` на allowlist-операциях; повтор с тем же ключом, методом, путём и телом — **повтор ответа** (200/201/409) из etcd-кэша до TTL. Для `PUT …/global/kv/{keyId}` в отпечаток входят заголовки доступа к KV, чтобы create/replace не пересекались</span> |

Ограничение: **gRPC идемпотентность заголовком** в текущей версии **не** дублирует REST.

---

## <span style="color:#5E4DB2">4. Референс развёртывания (продакшен и стенд)</span>

**Целевая картина:**

- **Два инстанса** coordination-server (LES): **DC1 home=r1** и **DC2 home=r2**, общий tenant, **общий stretched etcd**.
- У каждого LES свой ключ POV: **`/coord/v1/cluster/regions/{home}/activeRegion`** (r1 и r2 **не** перезаписывают статус друг друга). В значении: логический **`activeRegion`** (кто serves) + **`status`** (POV этого home).
- Роль (**active** / **stand_in** / **switch**) приходит из **RSS** (`rss.required=true`) или yaml / **`PUT /v1/active-region`** (SRE). Это **не** отдельный env `COORDINATION_DEPLOYMENT_ROLE`.
- **etcd на несколько ЦОД** (например узлы в r0/r1/r2): выносливость к потере сайта; кворум и RTT — предмет SRE.

**Локальный стенд:** один etcd на `127.0.0.1:2379` и два Spring (**8080** home=r1 ACTIVE, **8081** home=r2 stand_in); смена — RSS mock или **`PUT /v1/active-region`**. См. [`DEMO_PLAN_RU.md`](DEMO_PLAN_RU.md).

В проде те же **пути URL и семантика**, меняется топология etcd и сетевые политики.

---

## <span style="color:#5E4DB2">5. Поверхность API (сжато)</span>

### <span style="color:#216E4E">5.1. Две семьи</span>

| <span style="color:#5E4DB2;font-weight:600">Семья</span> | <span style="color:#5E4DB2;font-weight:600">Путь (REST)</span> | <span style="color:#5E4DB2;font-weight:600">etcd (логически)</span> | <span style="color:#5E4DB2;font-weight:600">Назначение</span> |
|-------|-------------|------------------|------------|
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">**Active region**</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`/v1/tenants/{tenant}/coordinator`, `/workers`, `/jobs`, `/stream/workers`, `/stream/jobs`</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`.../tenants/{tenant}/regions/{activeRegion}/...`</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Пул в **текущем** `activeRegion`; `regionId` в URL **не** передаётся — сервер подставляет из etcd POV **home**</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">**Global**</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`/v1/tenants/{tenant}/global/...`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`.../tenants/{tenant}/global/...`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Один global coordinator и общий пул workers/jobs/**kv** между ЦОД</span> |

Публичный снимок без секретов: **`GET …/active-region`**. SSE: **`…/stream/active-region`**.

### <span style="color:#216E4E">5.2. Операции (чеклист интеграции)</span>

| <span style="color:#5E4DB2;font-weight:600">Сущность</span> | <span style="color:#5E4DB2;font-weight:600">Создание / продление</span> | <span style="color:#5E4DB2;font-weight:600">Чтение (часть)</span> | <span style="color:#5E4DB2;font-weight:600">Завершение</span> |
|----------|----------------------|----------------|------------|
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Regional coordinator</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`POST …/coordinator/acquire`, `…/renew`</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`GET …/coordinator` (Bearer / X-Coordinator-Token)</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Отпуск lease = перестать renew</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Global coordinator</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`POST …/global/coordinator/acquire`, `…/renew`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`GET …/global/coordinator`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">То же</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Worker (regional / global)</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`POST …/workers/register`, `…/heartbeat`</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`GET …/workers/ids`, `GET …/workers`, `GET …/workers/{id}/jobs` (**X-Worker-Token**)</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`…/deregister`</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Job-lock (regional / global)</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`POST …/jobs/{id}:acquire` или `…/global/jobs/{id}:…`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`GET …/jobs/{id}` (coordinator **или** token holder-worker)</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`:release` с job token</span> |
| <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">Global KV (ticket)</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`PUT …/global/kv/{keyId}`</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">`GET`/`DELETE` с **accessToken** из ответа PUT</span> | <span style="background-color:#E3FCEF;padding:2px 6px;border-radius:4px">DELETE</span> |
| <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Ring</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">`POST /v1/rings/assign`</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">Чистое вычисление, без tenant pool в etcd</span> | <span style="background-color:#F4F5F7;padding:2px 6px;border-radius:4px">—</span> |

**TTL** lease для coordinator / worker / job (acquire/register): **10…300** с (единый контракт REST и gRPC).

### <span style="color:#216E4E">5.3. Семантика job под dual-DC</span>

Один API (`:acquire` / `:renew` / `:release`), **два контура**:

| | <span style="color:#5E4DB2;font-weight:600">Regional</span> `…/jobs/{jobId}` | <span style="color:#5E4DB2;font-weight:600">Global</span> `…/global/jobs/{jobId}` |
|--|----------------------------------|--------------------------------------|
| Ключ etcd | `…/regions/{activeRegion}/jobs/…` | `…/global/jobs/…` |
| Сколько владельцев | Ровно один в **активном** логическом сегменте | Ровно один на **весь tenant** (оба ЦОД) |
| LES `status=stand_in` / `switch` | Мутации **403** | Разрешены (участник из **global** пула) |
| REGION_DRAIN | **409** пока `regionalWritesBlocked` | **Не** блокируется |
| После смены `activeRegion` | Нужен **новый** acquire в новом сегменте | Lease **не** рвётся из‑за switchover |
| Когда выбирать | Работа только в «живом» ЦОД | Exclusivity при переезде / оба сайта |

Общее: `holder` + `participantToken` из **того же** контура; отдельный job `token` на renew/release; второй acquire при живом ключе — **409** `JOB_EXISTS`.

---

## <span style="color:#5E4DB2">6. Use-cases (подробно)</span>

### <span style="color:#216E4E">Визуализация: кто откуда узнаёт регион и роль инстанса</span>

Pod вызывает `GET /v1/tenants/{tenant}/active-region` без `Authorization` и получает JSON с полями `region` (home), `activeRegion`, `status`, `regionalWritesBlocked` и др. (схема **ActiveRegionInfo** в OpenAPI).

```
  [Pod]  --GET .../active-region (no Bearer)-->  [LES / coordination-server]
                                                      |
                                                      v
                                { region=home, activeRegion, status, ... }
                                                      |
                                                      v
                                                  [Pod решает:
                                                   regional vs global job,
                                                   писать ли regional API,
                                                   ждать ли drain]
```

---

### <span style="color:#216E4E">UC-1. Единственный «активный» писатель в regional пуле (классический лидер)</span>

**Ситуация:** в каждом логическом регионе должен быть **ровно один** процесс, который шлёт исходящие платежи / индексацию / репликацию в legacy.

**Шаги:**  
1) Все кандидаты периодически пытаются `POST …/coordinator/acquire` с одним `coordinatorId` и своим `instanceId`.  
2) Успех только у одного (`acquired=true`); остальные получают 409 или `acquired=false` с `currentHolder`.  
3) Лидер шлёт `renew` до TTL.  
4) Любой мониторинг читает `GET …/coordinator` с **токеном чтения** (тот же, что renew).  
5) Во внешнюю БД при записи передаётся **`fenceEpoch`** из ответа acquire — storage отвергает запись с устаревшей эпохой.

**Выигрыш:** явная модель «кто пишет», без самописного ZooKeeper/etcd-клиента в каждом сервисе.

---

### <span style="color:#216E4E">UC-2. Воркеры в active region + список своих job-lock’ов</span>

**Ситуация:** горизонтально масштабируемые воркеры; каждый должен видеть **только свои** захваченные job’ы.

**Шаги:**  
1) `POST …/workers/register` → сохранить `token`.  
2) `heartbeat` с периодом < TTL.  
3) `POST …/jobs/{jobId}:acquire` с `holder=workerId`, `participantToken=worker token`.  
4) `GET …/workers/{workerId}/jobs` с заголовком **`X-Worker-Token`** (или Bearer с тем же значением).

**Выигрыш:** coordinator token для этого чтения **не** нужен; оператору проще выдать минимальные права воркеру.

---

### <span style="color:#216E4E">UC-3. Оператор / SRE читает состояние job по токену координатора</span>

**Ситуация:** дашборд или runbook: «кто держит job X» без знания worker secret.

**Шаги:**  
1) Иметь **coordinator read token** (из acquire лидера в этом контуре).  
2) `GET …/jobs/{jobId}` с `Authorization: Bearer <coordinator token>`.

**Альтернатива:** воркер-holder читает тот же GET со **своим** register token (Bearer или `X-Worker-Token`).

---

### <span style="color:#216E4E">UC-4. Global coordinator + global jobs (два ЦОД, общая exclusivity)</span>

**Ситуация:** нужен **ровно один** владелец задачи на оба ЦОД; regional drain/switchover не должен рвать владение.

**Шаги:**  
1) `POST …/global/coordinator/acquire` на любом LES (и ACTIVE, и stand_in).  
2) Регистрация `POST …/global/workers/register`, job `POST …/global/jobs/{id}:acquire` с global participant token.  
3) Чтения с `GET …/global/...` и токеном **global** лидера.

**Не путать с regional job:** `…/jobs/{id}` на stand_in даст **403**; после смены `activeRegion` regional lock в старом сегменте протухнет по TTL — для cross-DC выбирайте **global** (см. §5.3).

**Выигрыш:** один кодовый путь для «между ЦОД», отдельный от regional drain/switchover.

---

### <span style="color:#216E4E">UC-5. Смена active region (active-standby по сегменту etcd)</span>

**Ситуация:** команда / RSS переключает логический **`activeRegion`** с **r1** на **r2**; LES в каждом DC обновляют свой per-home ключ.

**Шаги:**  
1) Подписчики на `GET …/stream/active-region` (или поллинг) видят **`regionalWritesBlocked: true`**.  
2) Новые regional `coordinator/workers/jobs` получают **409 REGION_DRAIN**.  
3) После TTL старых lease (кроме `coordinationEpoch`) — `regionalWritesBlocked: false`.  
4) Клиенты **заново** acquire/register/job в новом сегменте (те же short URL). **Global** jobs при этом можно не трогать.

**Выигрыш:** предсказуемый контракт «сначала drain, потом запись» (кроме аварийного **SWITCH** purge — README).

---

### <span style="color:#216E4E">UC-6. Пассивный ЦОД (stand_in): чтение и global, без regional мутаций</span>

**Ситуация:** во втором DC поднят тот же бинарь (home=r2) для теплого резерва.

**Шаги:**  
1) У LES в DC2 в etcd POV `status=stand_in` (RSS или SRE PUT).  
2) `GET …/active-region`, SSE — **да** (`region`=r2, `activeRegion` часто r1, `status`=stand_in).  
3) `POST …/coordinator/acquire`, `workers/register`, `jobs/...:acquire` — **403** `STANDBY_DEPLOYMENT`.  
4) `POST …/global/coordinator/acquire` и global worker/job — **да**.

**Выигрыш:** один артефакт деплоя; меньше ошибок «случайно пишем regional в DC2».

---

### <span style="color:#216E4E">UC-7. Идемпотентные мутации при обрыве сети (REST)</span>

**Ситуация:** клиент не уверен, дошёл ли `acquire` до сервера; повторяет запрос.

**Шаги:**  
1) Включить на сервере `coordination.idempotency.enabled` и попасть в allowlist путь/метод.  
2) Передавать стабильный **`Idempotency-Key`**.  
3) Повтор с тем же телом → тот же HTTP-статус и тело из etcd.

**Выигрыш:** меньше дубликатов лидеров/регистраций при ретраях на границе сети.

---

### <span style="color:#216E4E">UC-8. Global KV ticket (ключ–значение с отдельным access token)</span>

**Ситуация:** маленький общий флаг / checkpoint между сервисами в двух DC без поднятия отдельной БД.

**Шаги:**  
1) `PUT …/global/kv/{keyId}` без coordinator Bearer → в ответе **`accessToken`**.  
2) `GET`/`DELETE` только с этим токеном (или `X-Kv-Access-Token`).  
3) На **STANDBY** этот API доступен (global scope).

**Выигрыш:** простой ticket-модель поверх того же etcd-кластера.

---

### <span style="color:#216E4E">UC-9. Конкуренция за job и валидация TTL</span>

**Ситуация:** два воркера борются за один `jobId`.

**Ожидание:** второй acquire при живом lock — **409 JOB_EXISTS** с телом, помогающим диагностике. Некорректный TTL — **400** (валидация).

---

### <span style="color:#216E4E">UC-10. Наблюдение за пулом (SSE)</span>

**Ситуация:** UI или reconciler должен реагировать на изменения workers/jobs без поллинга всего списка.

**Шаги:**  
1) `GET …/stream/workers` и/или `…/stream/jobs` с **coordinator read token** (regional или global в зависимости от пути).  
2) Парсинг `text/event-stream`.

---

### <span style="color:#216E4E">UC-11. Любой под: «в каком я home», «кто activeRegion», «можно ли regional-мутации»</span>

**Ситуация:** после переезда в 2 DC нужно **без секретов** понять: home этого LES, кто сейчас serves short-path, идёт ли drain, и можно ли писать regional на **этот** base URL.

**Шаги:**  
1) **`GET /v1/tenants/{tenant}/active-region`** (или `/v1/active-region`) на том base URL, куда шлёте API. **Authorization не нужен**.  
2) Разбор JSON (**ActiveRegionInfo** в OpenAPI):  
   - **`region`** — **home** этого LES (yaml / RSS `state.region`).  
   - **`activeRegion`** — логический сегмент для short-path (`…/regions/{activeRegion}/…`).  
   - **`status`** — **`active`** | **`stand_in`** | **`switch`**: POV home из **etcd** (RSS или SRE). От этого — **403** на regional при `stand_in`/`switch`.  
   - **`regionalWritesBlocked`**, **`drainingPreviousRegion`** — REGION_DRAIN; пока `true` — regional acquire/register/job → **409**.  
   - **`source`** — `etcd` / `etcd-missing` / `initial` (отладка).

3) Опционально: **`GET …/stream/active-region`** (SSE).

**Нюанс:** за балансировщиком несколько LES с разными home — снимок всегда про **тот** инстанс, который ответил. Regional mutate шлите на LES с `status=active` (или используйте global).

**Выигрыш:** единый контракт для init/readiness и бизнес-логики («мы stand_in — только global» / «drain — не шлём acquire»).

---

### <span style="color:#216E4E">UC-12. Один под — coordinator; остальные — workers; выборка из БД по кольцу или по `workerId`</span>

**Ситуация:** нужен паттерн **один оркестратор / много исполнителей**: один процесс владеет ролью **coordinator** (lease + token + `fenceEpoch` для внешних систем), остальные регистрируются как **workers** и **тягут из БД только «свои» строки** — либо по явной колонке **`worker_id`**, либо по **диапазону хеша** (`client_range` / partition range), согласованному с **consistent hash ring** на том же наборе `workerId`, что и в координаторе.

**Шаги (оркестрация):**  
1) Кандидат в coordinator: `POST …/coordinator/acquire` с фиксированным `coordinatorId` (роль «scheduler»). Один под получает `acquired=true`, остальные ждут или получают `currentHolder`.  
2) Исполнители: `POST …/workers/register` с **уникальным** `workerId` на pod / StatefulSet ordinal.  
3) Состав пула для шардирования: `GET …/workers/ids` (с Bearer лидера coordinator) — упорядоченный список id.  
4) **Кольцо (ring):**  
   - REST: **`POST /v1/rings/assign`** с телом `{ "workerIds": [...], "virtualNodes": 128 }` (и опционально `withPoints`).  
   - gRPC: **`ComputeRanges`** — тот же алгоритм (Murmur3-32, Ketama, модуль **coordination-hash** в репозитории).  
   Ответ: для каждого worker — набор **полуинтервалов** `[start, end)` на окружности `uint32`; вместе они покрывают кольцо без дыр (при непустом списке воркеров).

5) **Назначение строки воркеру (два распространённых варианта):**  
   - **По `worker_id` в БД:** при INSERT/UPDATE координатор или сам воркер записывает колонку `worker_id` = зарегистрированный id; воркер делает `SELECT … WHERE worker_id = :myWorkerId AND status = 'pending' LIMIT …`. Проще операционно, ring не обязателен.  
   - **По hash / `client_range`:** для первичного ключа или бизнес-ключа стабильно считается `h = unsigned32(Murmur3_32(utf8(key)))` (как в README и coordination-hash); воркер **владеет** ключом, если `h` попадает в один из его **диапазонов** из `ranges_by_worker[myWorkerId]`. В SQL — либо предвычисленная колонка `shard_hash`, либо выражение в том же виде, что и на клиенте, и фильтр «hash попадает в мой список диапазонов» (часто материализуют `assigned_worker` batch-джобой, которую ведёт coordinator).

6) При **изменении** состава воркеров coordinator пересчитывает ring (новый `POST /rings/assign`) и инициирует rebalance / переназначение строк (вне зоны ответственности coordination-server, но контракт диапазонов **детерминирован**).

```
                    +------------------+
                    |  coordinator    |
                    |  acquire/renew  |
                    +--------+---------+
                             |
              GET workers/ids + POST /rings/assign
                             |
        +--------------------+--------------------+
        |                    |                    |
   +----+----+          +----+----+          +----+----+
   | worker A |          | worker B | ...    | worker N |
   +----+----+          +----+----+
        |                    |
        v                    v
   ranges A              ranges B
        \                  /
         v                v
              +---------------------+
              |        БД           |
              | worker_id / hash    |
              +---------------------+
```

**Выигрыш:** coordinator + workers дают **живость** и **единственность** роли лидера; **ring** — готовая **детерминированная** нарезка пространства ключей между теми же `workerId`, без отдельного сервиса «shard map»; **fenceEpoch** координатора можно использовать при записи в БД/очередь для отсечения устаревшего лидера.

---

## <span style="color:#5E4DB2">7. Как пользоваться API</span>

**Базовый URL.** Подставьте хост и порт своего инстанса, например `http://127.0.0.1:8080`. Ниже плейсхолдер `{BASE}`.

**Путь tenant.** Почти все ресурсы под префиксом `/v1/tenants/{tenantId}/…`. Идентификатор tenant изолированный строковый сегмент (без `/`).

**Тело и заголовки мутаций.**  
- `Content-Type: application/json` на всех `POST`/`PUT` с телом.  
- Опционально `Idempotency-Key` или `X-Idempotency-Key` — если на сервере включена идемпотентность и путь в allowlist.

**Авторизация чтений (coordinator).**  
Для `GET` coordinator, workers, jobs (кроме отдельных исключений в спецификации), SSE `stream/workers` / `stream/jobs`:  
`Authorization: Bearer <token из POST …/coordinator/acquire или …/global/coordinator/acquire в том же контуре>`  
или заголовок `X-Coordinator-Token` с тем же значением.

**Воркер.** Для `GET …/workers/{workerId}/jobs`: `X-Worker-Token` или `Authorization: Bearer` с токеном из `POST …/workers/register`.

**Global KV.** После `PUT …/global/kv/{keyId}` в ответе поле `accessToken`; для `GET`/`DELETE` того же ключа: `Authorization: Bearer <accessToken>` или `X-Kv-Access-Token`.

**Ошибки.** Типичные коды: **401** (нет нужного токена), **403** (роль STANDBY / неверный токен), **409** (конфликт ресурса, REGION_DRAIN), **400** (валидация, в т.ч. TTL).

**Документация контракта.** Полный перечень путей, тел и кодов ошибок — в OpenAPI YAML сервиса; при запущенном сервере часто доступны JSON API docs и Swagger UI на стандартных путях Springdoc (см. README репозитория).

---

## <span style="color:#5E4DB2">8. End-to-end сценарии REST (последовательность вызовов)</span>

Раздел для **агента или разработчика**, который воспроизводит цепочку без контекста чата: не только *что* вызвать, но *зачем* шаг и к каким **use-cases** из раздела 6 он относится. Замените `{BASE}`, `{TENANT}`, идентификаторы сущностей; из ответов сохраняйте `token`, `accessToken`, `fenceEpoch` и т.д.

**Примеры ответов** ниже — иллюстративные тела при **HTTP 200** (или **201** для первого `PUT` KV); фактические `leaseId`, `revision`, даты и токены зависят от окружения. Схемы полей совпадают с `openapi/openapi.yaml`.

### <span style="color:#216E4E">Сценарий A — active region: снимок региона, coordinator, worker, job, ring, SSE</span>

**Цель:** полный «региональный» контур — от проверки «можно ли писать в etcd-сегмент» до наблюдения за пулом. Покрывает UC-1 (лидер), UC-2/UC-3 (воркер и job), UC-5/UC-11 (active region и drain), UC-10 (SSE), UC-12 (ring).

**Зачем шаги (для агента; номера совпадают с нумерованным списком ниже).**

- **1 — `GET …/active-region`:** до coordinator/workers/jobs проверить **REGION_DRAIN** (`regionalWritesBlocked`), логический **region** и **status** — иначе POST дадут 409/403. UC-11, UC-5.
- **2 — acquire / renew coordinator:** один лидер с lease, **read token** для защищённых GET и SSE по regional пулу; **fenceEpoch** — fencing во внешних хранилищах. UC-1.
- **3 — `GET …/coordinator`:** убедиться, что лидер виден с тем же Bearer; health / runbook. UC-1.
- **4 — register / heartbeat worker:** зарегистрировать исполнителя в regional пуле; без heartbeat ключ снимется по TTL. UC-2, основа UC-12.
- **5 — `GET …/workers/ids`:** актуальный состав пула для шардирования или отладки кольца. UC-12.
- **6 — job acquire:** именованный lock с проверкой `participantToken` воркера. UC-2, UC-3.
- **7 — `GET …/jobs/{id}`:** состояние lock с токеном координатора **или** воркера (разные роли наблюдателя). UC-3.
- **8 — `GET …/workers/{id}/jobs`:** воркер без coordinator secret видит только свои job-lock’и. UC-2.
- **9 — `POST /v1/rings/assign`:** детерминированная нарезка ключей по воркерам (как gRPC `ComputeRanges`) для выборок из БД по hash-диапазону. UC-12.
- **10 — release job и deregister worker:** освободить lock и снять регистрацию, не блокируя других и не держать мёртвые lease.
- **11 — SSE `stream/workers`:** наблюдение за пулом без поллинга; алерты и UI. UC-10. (`stream/jobs` — то же для job-ключей; `stream/active-region` — смена региона без Bearer, UC-5.)

1) **Снимок региона (без секрета).**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/active-region"
```

**Пример ответа** (`ActiveRegionInfo`, поля зависят от конфига и роли инстанса):

```json
{
  "tenantId": "demo",
  "region": "r1",
  "activeRegion": "r1",
  "status": "active",
  "source": "etcd",
  "regionalWritesBlocked": false,
  "drainingPreviousRegion": null
}
```

Убедиться, что `status=active` и `regionalWritesBlocked` приемлем для regional записи; запомнить `region` (home), `activeRegion`.

2) **Regional coordinator: acquire и renew.**

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/coordinator/acquire" \
  -H 'Content-Type: application/json' \
  -d '{"coordinatorId":"payments","instanceId":"pod-1","ttlSeconds":60,"metadata":{}}'
```

Из ответа сохранить `token` (для renew и для чтений) и при необходимости `fenceEpoch`.

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/coordinator/renew" \
  -H 'Content-Type: application/json' \
  -d '{"token":"<COORD_TOKEN>"}'
```

**Примеры ответов.** Успешный `acquire` (`AcquireCoordinatorResult`):

```json
{
  "acquired": true,
  "leaseId": 7581938270123456789,
  "revision": 42,
  "fenceEpoch": 7,
  "token": "ct_01HZZExampleTokenReplaceMe"
}
```

Если лидер уже есть — **409** и тело `ResourceConflict` с `error: "COORDINATOR_EXISTS"` и блоком `currentHolder` (см. OpenAPI).

`renew` при успехе:

```json
{ "renewed": true }
```

3) **Чтение лидера (секрет в теле ответа для renew скрывается в GET — в OpenAPI описано поведение).**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/coordinator" \
  -H "Authorization: Bearer <COORD_TOKEN>"
```

**Пример ответа** (`CoordinatorState`; в `GET` поле `record.token` может быть опущено — см. описание API):

```json
{
  "record": {
    "coordinatorId": "payments",
    "instanceId": "pod-1",
    "token": null,
    "metadata": {},
    "issuedAt": "2026-05-11T10:15:30Z",
    "fenceEpoch": 7
  },
  "leaseId": 7581938270123456789,
  "modRevision": 42
}
```

4) **Worker: register, heartbeat.**

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/workers/register" \
  -H 'Content-Type: application/json' \
  -d '{"workerId":"worker-a","instanceId":"pod-a","ttlSeconds":60,"metadata":{}}'
```

Сохранить `token` как `WORKER_TOKEN`.

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/workers/worker-a/heartbeat" \
  -H 'Content-Type: application/json' \
  -d '{"token":"<WORKER_TOKEN>"}'
```

**Примеры ответов.** `POST .../workers/register` (`WorkerRegisterResult`):

```json
{
  "created": true,
  "token": "wt_01HZZExampleWorkerToken",
  "leaseId": 7581938270999888777
}
```

`heartbeat`:

```json
{ "ok": true }
```

5) **Список воркеров (нужен coordinator read token).**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/workers/ids" \
  -H "Authorization: Bearer <COORD_TOKEN>"
```

**Пример ответа** — JSON-массив `workerId`, отсортированный по возрастанию:

```json
["worker-a", "worker-b"]
```

6) **Job-lock: acquire с holder = worker, participantToken = worker register token.**

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/jobs/batch-1:acquire" \
  -H 'Content-Type: application/json' \
  -d '{"holder":"worker-a","participantToken":"<WORKER_TOKEN>","ttlSeconds":60,"metadata":{}}'
```

**Пример ответа** (`AcquireJobResult`):

```json
{
  "acquired": true,
  "leaseId": 7581938270111223344,
  "revision": 55,
  "token": "jt_01HZZExampleJobToken"
}
```

Сохранить `token` из ответа как `JOB_TOKEN`.

7) **GET job: coordinator ИЛИ worker token.**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/jobs/batch-1" \
  -H "Authorization: Bearer <COORD_TOKEN>"
```

либо

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/jobs/batch-1" \
  -H "Authorization: Bearer <WORKER_TOKEN>"
```

**Пример ответа** (`GET …/jobs/{jobId}` → `JobState`; секрет `record.token` в части ответов может опускаться):

```json
{
  "record": {
    "jobId": "batch-1",
    "holder": "worker-a",
    "token": "jt_01HZZExampleJobToken",
    "metadata": {},
    "issuedAt": "2026-05-11T10:16:00Z"
  },
  "leaseId": 7581938270111223344,
  "modRevision": 55
}
```

8) **Список job воркера.**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/workers/worker-a/jobs" \
  -H "X-Worker-Token: <WORKER_TOKEN>"
```

**Пример ответа** — массив `JobState`; в этом списке секрет lock-токена **часто опущен** (см. OpenAPI):

```json
[
  {
    "record": {
      "jobId": "batch-1",
      "holder": "worker-a",
      "metadata": {},
      "issuedAt": "2026-05-11T10:16:00Z"
    },
    "leaseId": 7581938270111223344,
    "modRevision": 55
  }
]
```

9) **Ring (без tenant в пути):**

```bash
curl -sS -X POST "{BASE}/v1/rings/assign" \
  -H 'Content-Type: application/json' \
  -d '{"workerIds":["worker-a","worker-b"],"virtualNodes":128}'
```

**Пример ответа** (`RingAssignResponse`; числа на кольце `uint32`, интервалы полуоткрыты `[start,end)`):

```json
{
  "ranges": {
    "worker-a": [{ "start": 0, "end": 2147483648 }],
    "worker-b": [{ "start": 2147483648, "end": 4294967296 }]
  }
}
```

(Реальное разбиение зависит от хеша и `virtualNodes`; при `withPos: true` добавляется массив `points`.)

10) **Завершение job и воркера.**

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/jobs/batch-1:release" \
  -H 'Content-Type: application/json' \
  -d '{"token":"<JOB_TOKEN>"}'

curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/workers/worker-a/deregister" \
  -H 'Content-Type: application/json' \
  -d '{"token":"<WORKER_TOKEN>"}'
```

**Примеры ответов:** `release` — `{ "released": true }`; `deregister` — `{ "deleted": true }`.

11) **SSE workers (долгоживущее соединение; остановка Ctrl+C).**

```bash
curl -N -H 'Accept: text/event-stream' \
  -H "Authorization: Bearer <COORD_TOKEN>" \
  "{BASE}/v1/tenants/{TENANT}/stream/workers"
```

**Пример фрагмента тела** (`text/event-stream`; имена `event:` и форма `data:` зависят от реализации, типично приходит снимок и далее дельты):

```text
event: workers
data: {"workerIds":["worker-a","worker-b"]}

: ping

```

Аналогично `…/stream/jobs`. Для смены региона без поллинга: `…/stream/active-region` без Bearer.

---

### <span style="color:#216E4E">Сценарий B — global: coordinator, global KV ticket, опционально SSE global</span>

**Цель:** контур **не привязанный** к одному логическому active region: общий coordinator, ticket-KV между ЦОД, поток событий global workers. UC-4, UC-8, UC-6 (global на STANDBY), UC-10 в global-варианте.

**Зачем шаги (для агента).**

- **1 — `POST …/global/coordinator/acquire`:** лидер и read token для `GET …/global/…` и SSE под префиксом `global`; сценарии общей очереди или аудита между DC. UC-4.
- **2 — `PUT …/global/kv/{keyId}`:** малый общий флаг или секрет без отдельной БД; `accessToken` ограничивает чтение/удаление. UC-8.
- **3 — `GET` / `DELETE` KV с ticket:** прочитать или снять значение, не отдавая coordinator-секреты шире, чем нужно. UC-8.
- **4 — SSE `global/stream/workers`:** наблюдение за глобальным пулом (например active-active обработчики в двух ЦОД). UC-4, UC-10. Путь `…/global/stream/jobs` — то же для global job-ключей.

1) **Global coordinator.**

```bash
curl -sS -X POST "{BASE}/v1/tenants/{TENANT}/global/coordinator/acquire" \
  -H 'Content-Type: application/json' \
  -d '{"coordinatorId":"global-lead","instanceId":"pod-g1","ttlSeconds":60,"metadata":{}}'
```

**Пример ответа** — та же схема, что у regional `…/coordinator/acquire` (`AcquireCoordinatorResult`):

```json
{
  "acquired": true,
  "leaseId": 7581938270555666777,
  "revision": 3,
  "fenceEpoch": 2,
  "token": "gct_01HZZExampleGlobalCoordToken"
}
```

Сохранить `token` как `GLOBAL_COORD_TOKEN`.

2) **PUT global KV (создание ticket; тело с value и ttlSeconds по схеме OpenAPI).**

```bash
curl -sS -X PUT "{BASE}/v1/tenants/{TENANT}/global/kv/checkpoint-1" \
  -H 'Content-Type: application/json' \
  -d '{"value":{"step":1},"ttlSeconds":120}'
```

**Пример ответа** при первом создании ключа — **HTTP 201**, тело `KvPutResponse` (поле `accessToken` только при `created: true`):

```json
{
  "created": true,
  "keyId": "checkpoint-1",
  "accessToken": "kv_01HZZExampleKvAccessToken",
  "leaseId": 7581938270666777888,
  "modRevision": 10
}
```

Сохранить `accessToken` из ответа как `KV_ACCESS`.

3) **GET / DELETE KV с ticket.**

```bash
curl -sS "{BASE}/v1/tenants/{TENANT}/global/kv/checkpoint-1" \
  -H "Authorization: Bearer <KV_ACCESS>"

curl -sS -X DELETE "{BASE}/v1/tenants/{TENANT}/global/kv/checkpoint-1" \
  -H "Authorization: Bearer <KV_ACCESS>"
```

**Примеры ответов.** `GET` → `KvGetResponse`:

```json
{
  "value": { "step": 1 },
  "leaseId": 7581938270666777888,
  "modRevision": 10
}
```

`DELETE` при успехе:

```json
{ "deleted": true }
```

4) **SSE global workers (Bearer = global coordinator read token).**

```bash
curl -N -H 'Accept: text/event-stream' \
  -H "Authorization: Bearer <GLOBAL_COORD_TOKEN>" \
  "{BASE}/v1/tenants/{TENANT}/global/stream/workers"
```

**Пример фрагмента** (как у regional SSE; точный JSON в `data:` зависит от версии сервиса):

```text
event: workers
data: {"workerIds":["worker-a"]}

```

Путь `…/global/stream/jobs` — аналогично для событий по global job-ключам.

---

## <span style="color:#5E4DB2">9. Преимущества «в одном абзаце на слайд»</span>

1. **Один сервис** вместо набора самописных leader-election + registry + locks на etcd.  
2. **Явные** контракты OpenAPI + gRPC + Java SDK.  
3. **Regional vs global** — снижение смешения сценариев active-active и cross-DC.  
4. **Fencing epoch + lease** — готовая связка с классическими статьями про split-brain.  
5. **Drain при смене региона** — снижение риска двойной записи в момент переключения.  
6. **Идемпотентность** на критичных POST/PUT (по конфигурации).  
7. **`GET …/active-region`** — любой под без секретов узнаёт логический регион, drain и роль инстанса.  
8. **Ring / ComputeRanges** + workers — готовый каркас для шардирования очереди в БД.

---

## <span style="color:#5E4DB2">10. Источники в репозитории (без гиперссылок)</span>

Имена путей от корня монорепозитория: `README.md`, `openapi/openapi.yaml`, `proto/coordination/v1/coordination.proto`, каталоги `coordination-server`, `coordination-client`, `coordination-hash`.
