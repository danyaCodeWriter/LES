# Переменные окружения coordination-server

Сервер — Spring Boot 3; свойства из [`application.yml`](../coordination-server/src/main/resources/application.yml)
переопределяются через env (relaxed binding): точки → `_`, дефисы убираются,
`endpoints[0]` → `COORDINATION_ETCD_ENDPOINTS_0`.

LES **не** управляет active region / RSS / stand_in|switch. Клиент сам выбирает
regional short-path или `/global/...`. Сегмент etcd для regional API задаётся
статически: `coordination.region` / `COORDINATION_REGION`.

---

## Минимальный набор (k8s / Docker, внешний etcd)

| Env | Пример | Обязательно |
|-----|--------|-------------|
| `COORDINATION_ETCD_ENDPOINTS_0` | `http://etcd:2379` | **да** (если etcd не localhost) |
| `COORDINATION_ETCD_ROOTPREFIX` | `/coord/v1/tenants` | нет |
| `COORDINATION_REGION` | `r1` | нет (default `r1`) |

```yaml
env:
  - name: COORDINATION_ETCD_ENDPOINTS_0
    value: "http://etcd:2379"
  - name: COORDINATION_REGION
    value: "r1"
```

---

## Docker: образ `with-etcd`

Можно не задавать env: entrypoint поднимает etcd и выставляет
`COORDINATION_ETCD_ENDPOINTS_0=http://127.0.0.1:2379`. Опционально:

| Env | Значение |
|-----|----------|
| `COORDINATION_REGION` | `r1` |
| `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | SRE для SSE workers/jobs/keys |

---

## Два LES (разные ЦОД, общий etcd)

| Инстанс | Env |
|---------|-----|
| ЦОД 1 | `COORDINATION_REGION=r1` |
| ЦОД 2 | `COORDINATION_REGION=r2` |

Regional lease ключи пишутся в `.../regions/{COORDINATION_REGION}/...`.
Клиент решает, звать regional API этого LES или `/global/...`.

---

## Prod (дополнительно)

| Env | Описание |
|-----|----------|
| `COORDINATION_APIKEY` | API key filter (пусто = выкл) |
| `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | SSE workers/jobs/keys без токена лидера |
| `COORDINATION_ETCD_USERNAME` / `PASSWORD` | etcd RBAC |
| `COORDINATION_ETCD_TLS_ENABLED` | `true` + `https://` endpoints |
| `COORDINATION_ETCD_TLS_CACERTPATH` / `CERTPATH` / `KEYPATH` / `AUTHORITY` | TLS/mTLS |
| `SERVER_PORT` | HTTP, default `8080` |
| `GRPC_SERVER_PORT` | gRPC, default `9090` |

---

## Полный справочник `coordination.*`

| Yaml | Env | Default |
|------|-----|---------|
| `coordination.region` | `COORDINATION_REGION` | `r1` |
| `coordination.etcd.endpoints[0]` | `COORDINATION_ETCD_ENDPOINTS_0` | `http://127.0.0.1:2379` |
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
| `coordination.api-key` | `COORDINATION_APIKEY` | `""` |
| `coordination.sre.stream-listen-token` | `COORDINATION_SRE_STREAM_LISTEN_TOKEN` | `sre-stream-dev-token` |
| `coordination.idempotency.enabled` | `COORDINATION_IDEMPOTENCY_ENABLED` | `true` |
| `coordination.idempotency.ttl-seconds` | `COORDINATION_IDEMPOTENCY_TTLSECONDS` | `86400` |
| `coordination.limits.max-keys-per-tenant` | `COORDINATION_LIMITS_MAXKEYSPERTENANT` | `10000` |
| `server.port` | `SERVER_PORT` | `8080` |
| `grpc.server.port` | `GRPC_SERVER_PORT` | `9090` |

## Удалено (больше не используется)

`COORDINATION_RSS_*`, `COORDINATION_ACTIVE_REGION_*`, `COORDINATION_DEPLOYMENT_ROLE`,
API `GET/PUT /v1/active-region`, SSE `.../stream/active-region`, RSS mock.
