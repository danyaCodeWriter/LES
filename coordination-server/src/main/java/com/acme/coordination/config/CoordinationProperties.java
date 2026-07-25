package com.acme.coordination.config;

import com.acme.coordination.deployment.DeploymentRole;
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
    Limits limits,
    Rss rss
) {
  public CoordinationProperties {
    activeRegion = activeRegion == null ? new ActiveRegion("r1", "ACTIVE", 5000L) : activeRegion;
    idempotency = idempotency == null
        ? new Idempotency(false, 86400, 128, 262_144)
        : idempotency;
    sre = sre == null ? new Sre("") : sre;
    limits = limits == null ? new Limits(10_000) : limits;
    rss = rss == null ? new Rss(false, "", "", "r1", Duration.ofSeconds(30)) : rss;
  }

  public record Etcd(
      List<String> endpoints,
      String rootPrefix,
      Duration dialTimeout,
      Duration requestTimeout,
      /** etcd RBAC user; empty disables auth. Prefer env/secret over plaintext in prod. */
      String username,
      /** etcd RBAC password; empty disables auth. */
      String password,
      Tls tls
  ) {
    public Etcd {
      if (username == null) {
        username = "";
      }
      if (password == null) {
        password = "";
      }
      if (tls == null) {
        tls = Tls.disabled();
      }
    }

    public boolean authEnabled() {
      return username != null && !username.isBlank();
    }

    /** Local/dev helper (no TLS, no auth). */
    public static Etcd plain(
        List<String> endpoints, String rootPrefix, Duration dialTimeout, Duration requestTimeout) {
      return new Etcd(endpoints, rootPrefix, dialTimeout, requestTimeout, "", "", Tls.disabled());
    }
  }

  /**
   * TLS for jetcd → etcd. Enable in prod with PEM paths from mounted secrets.
   * Endpoints should use {@code https://…} when {@code enabled=true}.
   */
  public record Tls(
      boolean enabled,
      /** PEM CA / bundle to trust the etcd server cert. */
      String caCertPath,
      /** PEM client certificate (mTLS). */
      String certPath,
      /** PEM client private key (unencrypted). */
      String keyPath,
      /**
       * Optional gRPC authority / SNI override (useful when connecting by IP
       * while the cert is issued for a DNS name).
       */
      String authority
  ) {
    public Tls {
      caCertPath = blankToEmpty(caCertPath);
      certPath = blankToEmpty(certPath);
      keyPath = blankToEmpty(keyPath);
      authority = blankToEmpty(authority);
    }

    public static Tls disabled() {
      return new Tls(false, "", "", "", "");
    }

    public boolean hasCaCert() {
      return !caCertPath.isBlank();
    }

    public boolean hasClientCert() {
      return !certPath.isBlank() && !keyPath.isBlank();
    }

    private static String blankToEmpty(String s) {
      return s == null ? "" : s.trim();
    }
  }

  /**
   * Local bootstrap when {@code coordination.rss.required=false}.
   * When RSS is required, these are ignored for bootstrap (RSS is master data).
   */
  public record ActiveRegion(String value, String status, Long pollIntervalMs) {
    public ActiveRegion {
      if (value == null || value.isBlank()) {
        value = "r1";
      } else {
        value = value.trim();
      }
      if (status == null || status.isBlank()) {
        status = "ACTIVE";
      } else {
        status = DeploymentRole.parse(status).name();
      }
      if (pollIntervalMs == null || pollIntervalMs <= 0) {
        pollIntervalMs = 5000L;
      }
    }

    public String bootstrapRegionId() {
      return value;
    }

    public DeploymentRole bootstrapStatus() {
      return DeploymentRole.parse(status);
    }
  }

  public record Idempotency(
      boolean enabled,
      int ttlSeconds,
      int maxHeaderLength,
      int maxBodyBytes
  ) {}

  public record Sre(String streamListenToken) {}

  public record Limits(int maxKeysPerTenant) {
    public Limits {
      if (maxKeysPerTenant < 0) {
        throw new IllegalArgumentException("maxKeysPerTenant must be >= 0");
      }
    }
  }

  /**
   * Region State Service (RSS) — master data for logical active region + status.
   * SSE: {@code GET {baseUrl}/v1/recovery-groups/{recoveryGroup}/active-region?region={homeRegion}}.
   */
  public record Rss(
      boolean required,
      String baseUrl,
      String recoveryGroup,
      String homeRegion,
      Duration connectTimeout
  ) {
    public Rss {
      if (baseUrl == null) {
        baseUrl = "";
      } else {
        baseUrl = baseUrl.trim().replaceAll("/+$", "");
      }
      if (recoveryGroup == null) {
        recoveryGroup = "";
      } else {
        recoveryGroup = recoveryGroup.trim();
      }
      if (homeRegion == null || homeRegion.isBlank()) {
        homeRegion = "r1";
      } else {
        homeRegion = homeRegion.trim();
      }
      if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
        connectTimeout = Duration.ofSeconds(30);
      }
    }

    public boolean isConfigured() {
      return baseUrl != null && !baseUrl.isBlank() && recoveryGroup != null && !recoveryGroup.isBlank();
    }
  }
}
