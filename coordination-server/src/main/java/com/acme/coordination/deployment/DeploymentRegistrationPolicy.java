package com.acme.coordination.deployment;

import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

/**
 * На {@link DeploymentRole#STANDBY} и {@link DeploymentRole#SWITCH} клиенты не должны вызывать regional
 * coordinator/workers/jobs. Global scope всегда разрешён.
 * Роль — status из etcd-ключа этого home ({@code .../cluster/regions/{home}/activeRegion}), POV RSS.
 */
@Component
public class DeploymentRegistrationPolicy {

  private static final Pattern TENANT_GLOBAL =
      Pattern.compile("^/v1/tenants/[^/]+/global(?:/.*)?$");

  private static final Pattern ACTIVE_REGION_STREAM_WORKERS_JOBS_KEYS =
      Pattern.compile("^/v1/tenants/[^/]+/stream/(workers|jobs|keys)$");

  private static final Set<String> GRPC_REGIONAL_MUTATIONS = Set.of(
      "coordination.v1.Coordination/AcquireCoordinator",
      "coordination.v1.Coordination/RenewCoordinator",
      "coordination.v1.Coordination/RegisterWorker",
      "coordination.v1.Coordination/WorkerHeartbeat",
      "coordination.v1.Coordination/AcquireJob",
      "coordination.v1.Coordination/ReleaseJob",
      "coordination.v1.Coordination/RenewJob");

  private final DeploymentRoleSource roleSource;

  public DeploymentRegistrationPolicy(DeploymentRoleSource roleSource) {
    this.roleSource = roleSource;
  }

  public boolean isStandby() {
    return roleSource.currentRole() == DeploymentRole.STANDBY;
  }

  public boolean isSwitch() {
    return roleSource.currentRole() == DeploymentRole.SWITCH;
  }

  public boolean isActive() {
    return roleSource.currentRole() == DeploymentRole.ACTIVE;
  }

  public boolean participatesInRegionalDrain() {
    DeploymentRole r = roleSource.currentRole();
    return r == DeploymentRole.ACTIVE || r == DeploymentRole.SWITCH;
  }

  public boolean blocksRegionalTenantMutations() {
    return isStandby() || isSwitch();
  }

  public String instanceStatusWire() {
    return roleSource.currentRole().wire();
  }

  public boolean shouldBlockRest(String rawPath, HttpMethod method) {
    if (!blocksRegionalTenantMutations()) {
      return false;
    }
    String path = normalizePath(rawPath);
    if (!path.startsWith("/v1/tenants/")) {
      return false;
    }
    if (isGlobalTenantPath(path)) {
      return false;
    }
    HttpMethod m = method != null ? method : HttpMethod.GET;
    if (m == HttpMethod.GET || m == HttpMethod.HEAD || m == HttpMethod.OPTIONS) {
      if (isStandbyGetAllowlist(path)) {
        return false;
      }
      if (ACTIVE_REGION_STREAM_WORKERS_JOBS_KEYS.matcher(path).matches()) {
        return true;
      }
      return false;
    }
    return m == HttpMethod.POST || m == HttpMethod.PUT || m == HttpMethod.PATCH || m == HttpMethod.DELETE;
  }

  private static boolean isStandbyGetAllowlist(String path) {
    return path.endsWith("/active-region") || path.endsWith("/stream/active-region");
  }

  public boolean shouldBlockGrpcMutation(String fullMethodName) {
    if (!blocksRegionalTenantMutations()) {
      return false;
    }
    if (fullMethodName == null) {
      return false;
    }
    return GRPC_REGIONAL_MUTATIONS.contains(fullMethodName);
  }

  public static boolean isGlobalTenantPath(String normalizedPath) {
    return TENANT_GLOBAL.matcher(normalizedPath).matches();
  }

  public static String normalizePath(String rawPath) {
    if (rawPath == null || rawPath.isEmpty()) {
      return "";
    }
    String p = rawPath;
    int q = p.indexOf('?');
    if (q >= 0) {
      p = p.substring(0, q);
    }
    if (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  public static String standbyBodyJson() {
    return "{\"error\":\"STANDBY_DEPLOYMENT\",\"message\":\"This instance is STANDBY (stand-in): "
        + "active-region coordinator, worker, and job mutations are refused. "
        + "Set status to ACTIVE via PUT /v1/active-region (SRE) for regional writes. "
        + "Global APIs under /v1/tenants/{tenant}/global/ stay allowed.\"}";
  }

  public static String switchBodyJson() {
    return "{\"error\":\"SWITCH_DEPLOYMENT\",\"message\":\"This instance is SWITCH: active-region coordinator, "
        + "worker, and job mutations are refused until status returns to ACTIVE via PUT /v1/active-region. "
        + "Global APIs stay allowed. During REGION_DRAIN SWITCH may purge old regional keys faster.\"}";
  }

  public static String standbyGrpcDetail() {
    return "STANDBY: active-region coordinator/worker/job RPCs are disabled; use ACTIVE status or global RPCs.";
  }

  public static String switchGrpcDetail() {
    return "SWITCH: active-region coordinator/worker/job RPCs are disabled; use global RPCs or set status ACTIVE.";
  }
}
