# Переменные окружения coordination-server

Сервер — Spring Boot 3; свойства из [`application.yml`](../coordination-server/src/main/resources/application.yml) переопределяются через env по [relaxed binding](https://docs.spring.io/spring-boot/reference/features/external-config.html#features.external-config.typesafe-configuration-properties.relaxed-binding):

- точки в yaml → `_` в env (`coordination.etcd…` → `COORDINATION_ETCD…`);
- дефисы убираются (`root-prefix` → `ROOTPREFIX`, `stream-listen-token` → `STREAMLISTENTOKEN`);
- элементы списка — с индексом: `endpoints[0]` → `COORDINATION_ETCD_ENDPOINTS_0`.

Источник правды по полям: [`CoordinationProperties.java`](../coordination-server/src/main/java/com/acme/coordination/config/CoordinationProperties.java).

---

## Минимальный набор (k8s / Docker, внешний etcd)

Достаточно указать адрес etcd. Остальное — дефолты из `application.yml` (`rss.required=false`, `home=r1`, `status=ACTIVE`, порты 8080/9090).

| Env | Пример | Обязательно |
|-----|--------|-------------|
| `COORDINATION_ETCD_ENDPOINTS_0` | `http://etcd:2379` | **да** (если etcd не localhost внутри pod) |
| `COORDINATION_ETCD_ROOTPREFIX` | `/coord/v1/tenants` | нет (это значение по умолчанию) |

```yaml
# k8s fragment
env:
  - name: COORDINATION_ETCD_ENDPOINTS_0
    value: "http://etcd:2379"
  - name: COORDINATION_ETCD_ROOTPREFIX
    value: "/coord/v1/tenants"
```

```bash
# docker run (образ coordination-server:local)
docker run --rm -p 8080:8080 -p 9090:9090 \
  -e COORDINATION_ETCD_ENDPOINTS_0=http://host.docker.internal:2379 \
  coordination-server:local
```

`COORDINATION_APIKEY` **не нужен** для старта: пустое значение отключает API-key фильтр.

---

## Docker: образ `with-etcd` (app + etcd в одном контейнере)

Сборка и запуск — [`Dockerfile.with-etcd`](../Dockerfile.with-etcd), entrypoint — [`docker/entrypoint-with-etcd.sh`](../docker/entrypoint-with-etcd.sh).

**Переменные приложения:** можно **ничего не задавать** — entrypoint выставляет `COORDINATION_ETCD_ENDPOINTS_0=http://127.0.0.1:2379` и стартует embedded etcd.

```bash
docker build -f Dockerfile.with-etcd -t coordination-server:with-etcd .
docker run --rm -p 8080:8080 -p 9090:9090 -p 2379:2379 coordination-server:with-etcd
```

Или через compose:

```bash
docker compose --profile with-etcd up -d --build coordination-with-etcd
```

### Рекомендуемые env для dev (compose-профиль)

| Env | Значение | Зачем |
|-----|----------|-------|
| `COORDINATION_ETCD_ENDPOINTS_0` | `http://127.0.0.1:2379` | уже в образе; явно в compose для ясности |
| `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | `sre-stream-dev-token` | `PUT /v1/active-region`, SSE без coordinator-токена |

### Env entrypoint (embedded etcd, опционально)

| Env | По умолчанию | Описание |
|-----|--------------|----------|
| `ETCD_DATA_DIR` | `/var/lib/etcd` | каталог данных etcd |
| `ETCD_CLIENT_URL` | `http://127.0.0.1:2379` | client URL для healthcheck |
| `ETCD_PEER_URL` | `http://127.0.0.1:2380` | peer URL single-node кластера |
| `ETCD_BIN` | `/usr/local/bin/etcd` | путь к бинарнику |
| `ETCDCTL_BIN` | `/usr/local/bin/etcdctl` | путь к etcdctl |
| `COORDINATION_ETCD_ENDPOINTS_0` | `http://127.0.0.1:2379` | задаётся entrypoint, если не переопределён |

**Не использовать в prod:** один контейнер = один узел etcd без HA.

---

## Два ЦОД (два LES, общий etcd)

На **каждый** pod/инстанс — свой home (разные ключи `/coord/v1/cluster/regions/{home}/activeRegion`):

| Инстанс | Env |
|---------|-----|
| ЦОД 1 | `COORDINATION_ACTIVE_REGION_HOME=r1` |
| ЦОД 2 | `COORDINATION_ACTIVE_REGION_HOME=r2` |

Плюс общий etcd (`COORDINATION_ETCD_ENDPOINTS_0`). `value` / `status` на старте — из yaml (`r1` / `ACTIVE`) или позже из RSS / `PUT /v1/active-region`.

---

## RSS (`coordination.rss.required=true`)

| Env | Пример | Обязательно при RSS |
|-----|--------|---------------------|
| `COORDINATION_RSS_REQUIRED` | `true` | **да** |
| `COORDINATION_RSS_BASE_URL` | `http://rss:8090` | **да** |
| `COORDINATION_RSS_RECOVERY_GROUP` | `demo-rg` | **да** |
| `COORDINATION_ACTIVE_REGION_HOME` | `r1` | рекомендуется (yaml-стартеры; на втором ЦОД — `r2`) |
| `COORDINATION_RSS_HOME_REGION` | `r1` | только если `home` пустой — seed для первого SSE `?region=` |

Без yaml-стартеров (`home` пустой) процесс **блокируется** до первого состояния RSS.

---

## Prod (дополнительно к минимуму)

| Env | Описание |
|-----|----------|
| `COORDINATION_APIKEY` | включает проверку `X-Api-Key` / `Authorization` на мутациях (secret) |
| `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | SRE-секрет для `PUT /v1/active-region`, SSE workers/jobs/keys |
| `COORDINATION_ETCD_USERNAME` | etcd RBAC |
| `COORDINATION_ETCD_PASSWORD` | etcd RBAC (secret) |
| `COORDINATION_ETCD_TLS_ENABLED` | `true` + endpoints `https://…` |
| `COORDINATION_ETCD_TLS_CACERTPATH` | PEM CA (mount secret) |
| `COORDINATION_ETCD_TLS_CERTPATH` | client cert (mTLS) |
| `COORDINATION_ETCD_TLS_KEYPATH` | client key PEM |
| `COORDINATION_ETCD_TLS_AUTHORITY` | SNI / gRPC authority override |

---

## Полный справочник

### Порты (Spring Boot)

| Свойство yaml | Env | По умолчанию |
|---------------|-----|--------------|
| `server.port` | `SERVER_PORT` | `8080` |
| `grpc.server.port` | `GRPC_SERVER_PORT` | `9090` |

Health probes (k8s): `GET /actuator/health/readiness`, `GET /actuator/health/liveness` на `SERVER_PORT`.

### `coordination.etcd`

| Свойство yaml | Env | По умолчанию |
|---------------|-----|--------------|
| `coordination.etcd.endpoints[0]` | `COORDINATION_ETCD_ENDPOINTS_0` | `http://127.0.0.1:2379` |
| `coordination.etcd.endpoints[1]` | `COORDINATION_ETCD_ENDPOINTS_1` | — |
| `coordination.etcd.root-prefix` | `COORDINATION_ETCD_ROOTPREFIX` | `/coord/v1/tenants` |
| `coordination.etcd.dial-timeout` | `COORDINATION_ETCD_DIALTIMEOUT` | `3s` |
| `coordination.etcd.request-timeout` | `COORDINATION_ETCD_REQUESTTIMEOUT` | `3s` |
| `coordination.etcd.username` | `COORDINATION_ETCD_USERNAME` | `""` |
| `coordination.etcd.password` | `COORDINATION_ETCD_PASSWORD` | `""` |
| `coordination.etcd.tls.enabled` | `COORDINATION_ETCD_TLS_ENABLED` | `false` |
| `coordination.etcd.tls.ca-cert-path` | `COORDINATION_ETCD_TLS_CACERTPATH` | `""` |
| `coordination.etcd.tls.cert-path` | `COORDINATION_ETCD_TLS_CERTPATH` | `""` |
| `coordination.etcd.tls.key-path` | `COORDINATION_ETCD_TLS_KEYPATH` | `""` |
| `coordination.etcd.tls.authority` | `COORDINATION_ETCD_TLS_AUTHORITY` | `""` |

### `coordination.active-region`

| Свойство yaml | Env | По умолчанию | Примечание |
|---------------|-----|--------------|------------|
| `coordination.active-region.home` | `COORDINATION_ACTIVE_REGION_HOME` | `r1` | home этого LES |
| `coordination.active-region.value` | `COORDINATION_ACTIVE_REGION_VALUE` | `r1` | bootstrap `activeRegion` |
| `coordination.active-region.status` | `COORDINATION_ACTIVE_REGION_STATUS` | `ACTIVE` | `ACTIVE`, `STAND_IN`, `SWITCH` |
| `coordination.active-region.poll-interval-ms` | `COORDINATION_ACTIVE_REGION_POLLINTERVALMS` | `5000` | опрос etcd |

При `rss.required=false` пустые `home`/`value`/`status` → `r1`/`r1`/`ACTIVE`.  
При `rss.required=true` и пустом `home` — первые значения только из RSS.

### `coordination.rss`

| Свойство yaml | Env | По умолчанию |
|---------------|-----|--------------|
| `coordination.rss.required` | `COORDINATION_RSS_REQUIRED` | `false` |
| `coordination.rss.base-url` | `COORDINATION_RSS_BASEURL` | `""` |
| `coordination.rss.recovery-group` | `COORDINATION_RSS_RECOVERYGROUP` | `""` |
| `coordination.rss.home-region` | `COORDINATION_RSS_HOMEREGION` | `""` |
| `coordination.rss.connect-timeout` | `COORDINATION_RSS_CONNECTTIMEOUT` | `30s` |

### Прочее `coordination.*`

| Свойство yaml | Env | По умолчанию |
|---------------|-----|--------------|
| `coordination.api-key` | `COORDINATION_APIKEY` | `""` |
| `coordination.sre.stream-listen-token` | `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | `sre-stream-dev-token` |
| `coordination.idempotency.enabled` | `COORDINATION_IDEMPOTENCY_ENABLED` | `true` |
| `coordination.idempotency.ttl-seconds` | `COORDINATION_IDEMPOTENCY_TTLSECONDS` | `86400` |
| `coordination.idempotency.max-header-length` | `COORDINATION_IDEMPOTENCY_MAXHEADERLENGTH` | `128` |
| `coordination.idempotency.max-body-bytes` | `COORDINATION_IDEMPOTENCY_MAXBODYBYTES` | `262144` |
| `coordination.limits.max-keys-per-tenant` | `COORDINATION_LIMITS_MAXKEYSPERTENANT` | `10000` |

---

## Примеры манифестов

### k8s: минимум

См. [`coordination-server/k8s/deployment.yaml`](../coordination-server/k8s/deployment.yaml). Secret `coordination-api` / `COORDINATION_APIKEY` — опционален (нужен только если включаете API key).

### k8s: два ЦОД + RSS

```yaml
env:
  - name: COORDINATION_ETCD_ENDPOINTS_0
    value: "http://etcd:2379"
  - name: COORDINATION_ACTIVE_REGION_HOME
    value: "r1"   # на втором Deployment — r2
  - name: COORDINATION_RSS_REQUIRED
    value: "true"
  - name: COORDINATION_RSS_BASE_URL
    value: "http://rss:8090"
  - name: COORDINATION_RSS_RECOVERY_GROUP
    value: "demo-rg"
  - name: COORDINATION_SRE_STREAM_LISTEN_TOKEN
    valueFrom:
      secretKeyRef:
        name: coordination-sre
        key: token
```

### docker-compose: with-etcd

См. [`docker-compose.yml`](../docker-compose.yml), сервис `coordination-with-etcd` (профиль `with-etcd`).

---

## Чего нет в env

- **`COORDINATION_DEPLOYMENT_ROLE`** — не используется; `status` берётся из etcd / RSS / yaml, не из отдельной env.
- **JAVA_OPTS / JVM** — стандартные флаги в `ENTRYPOINT` образа (`-XX:MaxRAMPercentage=75.0`); для k8s можно переопределить `command`/`args` Deployment.
