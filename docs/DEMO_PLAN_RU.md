# Демо coordination (план по терминалам)

Корень репо: каталог с `settings.gradle.kts` / `gradlew`. Tenant по умолчанию: **`demo-team`**. Инфраструктура: [`scripts/demo-two-dc.sh`](../scripts/demo-two-dc.sh). Префикс в etcd: **`/coord/v1/tenants`**.

Порты: **2379** etcd, **8099** Etcd UI, **8080** / **9090** LES region **r1**, **8081** / **9091** LES region **r2**. Нужны **curl** и **jq**; для шага gRPC — **grpcurl** (опционально). Инфра: [`docker-compose.yml`](../docker-compose.yml) (`etcd` + `etcdkeeper`). Active region / RSS / stand_in gates **удалены** — клиент выбирает regional vs global API.


### GET и токен координатора (что видно в логах)

Чтение **regional** данных лидера (`GET …/coordinator`, `…/workers`, `…/workers/ids`, `…/jobs/{id}`, SSE `stream/workers` и `stream/jobs`, а также **global** coordinator) **без** заголовка **`Authorization: Bearer`** (или **`X-Coordinator-Token`**) с секретом из **POST …/coordinator/acquire** / **global/coordinator/acquire** в том же контуре сервер не отдаёт: ответ **401** и код **`COORDINATOR_READ_TOKEN_REQUIRED`**. API active-region **удалён**.

В пошаговых скриптах (`demo-step-*.sh`) и в **`active-flow` / `demo-step-sse-workers-jobs.sh`** в консоль перед запросом выводится правило и строка **`Заголовок запроса: Authorization: Bearer …`** (если токен передан) либо явное **«нет Authorization»** для демонстрации 401.

---

