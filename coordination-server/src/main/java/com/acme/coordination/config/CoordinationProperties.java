package com.acme.coordination.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "coordination")
public record CoordinationProperties(
    Etcd etcd,
    String apiKey,
    ActiveRegion activeRegion,
    Idempotency idempotency,
    Sre sre,
    Limits limits
) {
  public CoordinationProperties {
    activeRegion = activeRegion == null
        ? new ActiveRegion("COORDINATION_ACTIVE_REGION", "", "", 5000L)
        : activeRegion;
    idempotency = idempotency == null
        ? new Idempotency(false, 86400, 128, 262_144)
        : idempotency;
    sre = sre == null ? new Sre("") : sre;
    limits = limits == null ? new Limits(10_000) : limits;
  }

  public record Etcd(
      List<String> endpoints,
      String rootPrefix,
      Duration dialTimeout,
      Duration requestTimeout
  ) {}

  /**
   * Active region is polled from {@code filePath} (if set), else {@code System.getenv(envVar)},
   * else {@code value} from config. Used for tenant-scoped paths without explicit {@code regionId}.
   */
  public record ActiveRegion(
      String envVar,
      String value,
      String filePath,
      Long pollIntervalMs
  ) {}

  /**
   * HTTP replay cache: {@code Idempotency-Key} / {@code X-Idempotency-Key} + method + path + body hash → сохранённый
   * ответ (etcd lease {@code ttlSeconds}).
   */
  public record Idempotency(
      boolean enabled,
      int ttlSeconds,
      int maxHeaderLength,
      int maxBodyBytes
  ) {}

  /**
   * SRE: статический секрет (обычно из env), которым можно подписаться на SSE {@code .../stream/workers} и
   * {@code .../stream/jobs} в regional и global контурах без токена лидера coordinator. Пустая строка — выключено.
   */
  public record Sre(String streamListenToken) {}

  /**
   * Лимит ключей coordination на tenant (см. {@link com.acme.coordination.service.TenantKeyQuotaService}).
   * {@code maxKeysPerTenant == 0} — без лимита.
   */
  public record Limits(int maxKeysPerTenant) {
    public Limits {
      if (maxKeysPerTenant < 0) {
        throw new IllegalArgumentException("maxKeysPerTenant must be >= 0");
      }
    }
  }
}

