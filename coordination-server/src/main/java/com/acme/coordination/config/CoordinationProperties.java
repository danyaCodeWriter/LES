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
    activeRegion = activeRegion == null ? new ActiveRegion("", "r1", "ACTIVE", 5000L) : activeRegion;
    idempotency = idempotency == null
        ? new Idempotency(false, 86400, 128, 262_144)
        : idempotency;
    sre = sre == null ? new Sre("") : sre;
    limits = limits == null ? new Limits(10_000) : limits;
    rss = rss == null ? new Rss(false, "", "", "", Duration.ofSeconds(30)) : rss;
  }

  /**
   * Config-only home seed (yaml). Prefer {@link com.acme.coordination.region.InstanceHomeRegion}
   * at runtime (may be filled from RSS).
   */
  public String instanceHomeRegion() {
    String home = activeRegion.homeRegion();
    if (home != null && !home.isBlank()) {
      return home;
    }
    String rssHome = rss.homeRegion();
    if (rssHome != null && !rssHome.isBlank()) {
      return rssHome;
    }
    // yaml mode (rss.required=false): blank → r1; RSS mode leaves blank until InstanceHomeRegion/RSS
    return rss.required() ? "" : "r1";
  }

  /**
   * Explicit yaml starters for home/activeRegion/status ({@code active-region.home} set).
   * When {@code rss.required=true} and this is true — LES may start on yaml and wait for RSS.
   * When false — first values must come from RSS or the process must not become operational.
   */
  public boolean hasYamlActiveRegionStarters() {
    String home = activeRegion.home();
    return home != null && !home.isBlank();
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
   * Yaml bootstrap for active-region.
   *
   * <ul>
   *   <li>{@code rss.required=false}: always used (blank fields → home via {@code InstanceHomeRegion}
   *       default {@code r1}, value {@code r1}, status {@code ACTIVE}).
   *   <li>{@code rss.required=true}: used only when {@link CoordinationProperties#hasYamlActiveRegionStarters()}
   *       — temporary starters while waiting for RSS; otherwise first values must come from RSS.
   * </ul>
   *
   * @param home this LES home; blank → {@code r1} if RSS off, else seed/{@code state.region}
   * @param value bootstrap {@code activeRegion}; blank → {@code r1}
   * @param status bootstrap POV status; blank → {@code ACTIVE}
   */
  public record ActiveRegion(String home, String value, String status, Long pollIntervalMs) {
    public ActiveRegion {
      if (value == null || value.isBlank()) {
        value = "r1";
      } else {
        value = value.trim();
      }
      if (home == null) {
        home = "";
      } else {
        home = home.trim();
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

    public String homeRegion() {
      return home;
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
      if (homeRegion == null) {
        homeRegion = "";
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
