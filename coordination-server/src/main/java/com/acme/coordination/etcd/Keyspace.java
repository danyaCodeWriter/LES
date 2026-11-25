package com.acme.coordination.etcd;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public final class Keyspace {
  private final String rootPrefix; // e.g. /coord/v1/tenants

  public Keyspace(String rootPrefix) {
    this.rootPrefix = normalizeRoot(rootPrefix);
  }

  public String tenantRoot(String tenantId) {
    return rootPrefix + "/" + sanitizeSegment(tenantId);
  }

  /** Prefix listing all tenant subtrees, e.g. {@code /coord/v1/tenants/}. */
  public String allTenantsScanPrefix() {
    return rootPrefix + "/";
  }

  /** All regional keys for a tenant: {@code .../regions/}. */
  public String regionsPrefix(String tenantId) {
    return tenantRoot(tenantId) + "/regions/";
  }

  public String regionRoot(String tenantId, String regionId) {
    return regionsPrefix(tenantId) + sanitizeSegment(regionId);
  }

  /**
   * Cross-region scope for a tenant: one coordinator lease + shared workers/jobs (active-active across DC).
   * Keys live under {@code .../tenants/{tenant}/global/...}, not under {@code regions/}.
   */
  public String globalRoot(String tenantId) {
    return tenantRoot(tenantId) + "/global";
  }

  public String globalCoordinationEpochKey(String tenantId) {
    return globalRoot(tenantId) + "/coordinationEpoch";
  }

  public String globalLeaderKey(String tenantId) {
    return globalRoot(tenantId) + "/leader";
  }

  public String globalWorkersPrefix(String tenantId) {
    return globalRoot(tenantId) + "/workers/";
  }

  public String globalWorkerKey(String tenantId, String workerId) {
    return globalWorkersPrefix(tenantId) + sanitizeSegment(workerId);
  }

  public String globalJobsPrefix(String tenantId) {
    return globalRoot(tenantId) + "/jobs/";
  }

  public String globalJobKey(String tenantId, String jobId) {
    return globalJobsPrefix(tenantId) + sanitizeSegment(jobId);
  }

  public String globalSubtreePrefix(String tenantId) {
    return globalRoot(tenantId) + "/";
  }

  /** Tenant-global simple KV (tickets), not under a logical region segment. */
  public String globalKvPrefix(String tenantId) {
    return globalRoot(tenantId) + "/kv/";
  }

  public String globalKvKey(String tenantId, String keyId) {
    return globalKvPrefix(tenantId) + sanitizeSegment(keyId);
  }

  /** Idempotency replay entries per tenant (fingerprint = 64 hex chars). */
  public String idempotencyKey(String tenantId, String fingerprintHex64) {
    return tenantRoot(tenantId) + "/idempotency/" + fingerprintHex64;
  }

  /** Monotonic fencing epoch for this tenant+region (no lease; survives leader failover). */
  public String coordinationEpochKey(String tenantId, String regionId) {
    return regionRoot(tenantId, regionId) + "/coordinationEpoch";
  }

  public String leaderKey(String tenantId, String regionId) {
    return regionRoot(tenantId, regionId) + "/leader";
  }

  public String workersPrefix(String tenantId, String regionId) {
    return regionRoot(tenantId, regionId) + "/workers/";
  }

  public String workerKey(String tenantId, String regionId, String workerId) {
    return workersPrefix(tenantId, regionId) + sanitizeSegment(workerId);
  }

  public String jobsPrefix(String tenantId, String regionId) {
    return regionRoot(tenantId, regionId) + "/jobs/";
  }

  public String jobKey(String tenantId, String regionId, String jobId) {
    return jobsPrefix(tenantId, regionId) + sanitizeSegment(jobId);
  }

  /**
   * Parses {@code regionId} from a key ending with {@code /leader}, e.g.
   * {@code /coord/v1/tenants/acme/regions/eu/leader} → {@code eu}.
   */
  public Optional<String> parseRegionFromLeaderKey(String keyUtf8, String tenantId) {
    String suffix = "/leader";
    if (!keyUtf8.endsWith(suffix)) {
      return Optional.empty();
    }
    String pref = tenantRoot(tenantId) + "/regions/";
    if (!keyUtf8.startsWith(pref)) {
      return Optional.empty();
    }
    String region = keyUtf8.substring(pref.length(), keyUtf8.length() - suffix.length());
    if (region.isEmpty() || region.contains("/")) {
      return Optional.empty();
    }
    return Optional.of(region);
  }

  public String keyUtf8(io.etcd.jetcd.KeyValue kvp) {
    return kvp.getKey().toString(StandardCharsets.UTF_8);
  }

  private static String normalizeRoot(String rootPrefix) {
    Objects.requireNonNull(rootPrefix, "rootPrefix");
    String r = rootPrefix.trim();
    if (r.isEmpty()) {
      throw new IllegalArgumentException("rootPrefix must not be empty");
    }
    if (!r.startsWith("/")) {
      r = "/" + r;
    }
    if (r.endsWith("/")) {
      r = r.substring(0, r.length() - 1);
    }
    return r;
  }

  private static String sanitizeSegment(String s) {
    Objects.requireNonNull(s, "segment");
    String v = s.trim();
    if (v.isEmpty()) {
      throw new IllegalArgumentException("segment must not be empty");
    }
    if (v.contains("/") || v.contains("\u0000")) {
      throw new IllegalArgumentException("segment contains forbidden characters");
    }
    return v;
  }
}
