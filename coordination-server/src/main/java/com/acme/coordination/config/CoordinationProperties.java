package com.acme.coordination.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "coordination")
public record CoordinationProperties(
    Etcd etcd,
    String apiKey,
    /** Etcd region segment for short-path regional APIs. Env: {@code COORDINATION_REGION}. */
    String region,
    Idempotency idempotency,
    Sre sre,
    Limits limits
) {
  public CoordinationProperties {
    if (region == null || region.isBlank()) {
      region = "r1";
    } else {
      region = region.trim();
    }
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

}
