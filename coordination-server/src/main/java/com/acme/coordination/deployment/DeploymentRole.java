package com.acme.coordination.deployment;

/**
 * Cluster deployment mode stored in etcd (via {@code PUT /v1/active-region} / {@code SetActiveRegion}),
 * not via env/file. Controls whether this JVM accepts regional short-path mutations.
 */
public enum DeploymentRole {
  /** Accept regional + global mutations. */
  ACTIVE,
  /** Stand-in: refuse regional mutations; global allowed. */
  STANDBY,
  /**
   * Like STANDBY for clients, but participates in REGION_DRAIN and may purge old lease keys.
   */
  SWITCH;

  public static DeploymentRole parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return ACTIVE;
    }
    String t = raw.trim().toLowerCase().replace('-', '_');
    return switch (t) {
      case "active" -> ACTIVE;
      case "standby", "stand_in", "standin" -> STANDBY;
      case "switch", "switch_over", "switchover" -> SWITCH;
      default -> throw new IllegalArgumentException(
          "Unknown deployment status '" + raw + "' (expected ACTIVE, STANDBY, or SWITCH)");
    };
  }

  /** Wire value for JSON/SSE: {@code active}, {@code stand_in}, {@code switch}. */
  public String wire() {
    return switch (this) {
      case ACTIVE -> "active";
      case STANDBY -> "stand_in";
      case SWITCH -> "switch";
    };
  }
}
