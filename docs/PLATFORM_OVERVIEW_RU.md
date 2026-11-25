# Платформа LES (Leader Election Service) — обзор

## 1. Назначение

LES даёт **общий слой координации** для приложений: выбор лидера (coordinator), реестр воркеров с lease, job-lock’и с проверкой участника, **global**-контур (один coordinator и пул workers/jobs на tenant между ЦОД), **fencing epoch**, SSE по пулам, опционально **идемпотентность REST**.

**Не входит в LES:** master-data «кто активный регион», RSS, status `stand_in`/`switch`, drain. Это решает **клиент** (или внешний оркестратор), выбирая base URL LES и контур API (**regional** vs **global**).

Контракт REST: [`openapi/openapi.yaml`](../openapi/openapi.yaml). gRPC: [`proto/coordination/v1/coordination.proto`](../proto/coordination/v1/coordination.proto). Env: [`ENV_VARIABLES_RU.md`](ENV_VARIABLES_RU.md).

---

## 2. Контекст: переезд с single-DC на 2 DC

| Было (один ЦОД) | Стало (два ЦОД) |
|-----------------|-----------------|
| Один base URL, один смысл «кто лидер» | Два LES (`COORDINATION_REGION=r1` / `r2`) + **общий stretched etcd** |
| Один контур API | Клиент ходит на свой DC; для cross-DC exclusivity → **`/global/...`** |
| Редко нужен fencing | После split-brain старый процесс может ещё писать — нужны **epoch / lease** |

### Playbook клиента (минимум)

1. Выберите LES своего ЦОД (regional short-path с `coordination.region`) **или** зовите `/global/...`.
2. Для «один job на оба ЦОД» используйте **`/global/jobs/...`**.
3. После смены «активного» сайта на стороне клиента — заново register/acquire на нужном LES / контуре.

Платформа **не заменяет** БД и очереди; даёт lease + CAS + durable epoch поверх **etcd**.

---

## 3. Гарантии

| Область | Гарантия / механизм |
|---------|---------------------|
| Лидер regional / global | Ключ создаётся только если **нет** текущего значения (txn + `create_revision == 0`); значение на **lease**; renew только с **token** из acquire |
| Два лидера одной роли | Пока lease жив — второй acquire **не** получит ключ (409) |
| Fencing | Монотонный **`fenceEpoch`** на acquire coordinator |
| Job-lock | `:acquire` проверяет `holder` + **`participantToken`**; отдельный job `token` на renew/release |
| Чтение пулов | GET / SSE — с токеном лидера coordinator (или SRE stream token для SSE) |
| Идемпотентность REST | При `coordination.idempotency.enabled=true`: `Idempotency-Key` / `X-Idempotency-Key` |

gRPC-идемпотентность заголовком в текущей версии **не** дублирует REST.

---

## 4. Развёртывание

- **Два LES**: DC1 `COORDINATION_REGION=r1`, DC2 `r2`, общий tenant, **общий stretched etcd**.
- Regional ключи: `…/tenants/{tenant}/regions/{COORDINATION_REGION}/…`.
- Global ключи: `…/tenants/{tenant}/global/…`.

**Локальный стенд:** etcd `127.0.0.1:2379`, два Spring (**8080**/r1, **8081**/r2). См. [`DEMO_PLAN_RU.md`](DEMO_PLAN_RU.md).

---

## 5. Поверхность API

### 5.1. Две семьи

| Семья | REST | etcd | Назначение |
|-------|------|------|------------|
| **Regional** | `/v1/tenants/{tenant}/coordinator`, `/workers`, `/jobs`, `/stream/...` | `…/regions/{COORDINATION_REGION}/…` | Пул в сегменте этого LES; `regionId` в URL **не** передаётся |
| **Global** | `/v1/tenants/{tenant}/global/...` | `…/global/...` | Один coordinator и общий пул workers/jobs/kv между ЦОД |

### 5.2. Операции

| Сущность | Создание / продление | Чтение | Завершение |
|----------|----------------------|--------|------------|
| Regional / global coordinator | `POST …/acquire`, `…/renew` | `GET …/coordinator` (Bearer) | Перестать renew |
| Worker | `register`, `heartbeat` | `GET …/workers`, `…/ids`, `…/{id}/jobs` | `deregister` |
| Job-lock | `…/jobs/{id}:acquire` (или global) | `GET …/jobs/{id}` | `:release` |
| Global KV | `PUT …/global/kv/{keyId}` | GET/DELETE с accessToken | DELETE |
| Ring | `POST /v1/rings/assign` | Чистое вычисление | — |

**TTL** lease (acquire/register): **10…300** с.

### 5.3. Семантика job dual-DC

| | **Regional** | **Global** |
|--|--------------|------------|
| Ключ etcd | `…/regions/{COORDINATION_REGION}/jobs/…` | `…/global/jobs/…` |
| Владельцев | Один в сегменте этого LES | Один на весь tenant |
| Когда выбирать | Работа в одном ЦОД | Exclusivity на оба сайта |

---

## 6. Типовые сценарии

### UC-1. Один лидер роли (regional)

`POST …/coordinator/acquire` → сохранить `token` + `fenceEpoch` → периодический `renew`. Второй процесс при живом lease получает **409**.

### UC-2. Реестр воркеров + consistent hash

`register` / `heartbeat` → координатор `GET …/workers/ids` → `POST /v1/rings/assign` (или gRPC `ComputeRanges`). Назначение ключей — Murmur3/Ketama (`coordination-hash`).

### UC-3. Job-lock в одном ЦОД

`:acquire` с `holder` + `participantToken` участника **того же** regional контура. После TTL/release — новый владелец.

### UC-4. Cross-DC exclusivity (global)

`POST …/global/coordinator/acquire` и/или `…/global/jobs/{id}:acquire` — один владелец на оба LES при общем etcd. Regional drain/status на LES **нет**.

### UC-5. Наблюдение

SSE `…/stream/workers` / `stream/jobs` (Bearer coordinator или SRE token). API `stream/active-region` **удалён**.

### UC-6. Global KV ticket

`PUT …/global/kv/{keyId}` → `accessToken` для последующих GET/DELETE.

---

## 7. Что удалено (раньше было в LES)

- RSS / `rss-mock`
- `GET` / `PUT /v1/active-region`, tenant alias, SSE `…/stream/active-region`
- Status `active` / `stand_in` / `switch`, гейты **403**, REGION_DRAIN **409**
- Per-home etcd ключ `/coord/v1/cluster/regions/{home}/activeRegion`

Сегмент regional etcd теперь только из конфига: **`COORDINATION_REGION`**.

---

## 8. Ссылки

- README: [`../README.md`](../README.md)
- Демо: [`DEMO_PLAN_RU.md`](DEMO_PLAN_RU.md), [`../scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh)
- Env: [`ENV_VARIABLES_RU.md`](ENV_VARIABLES_RU.md)
