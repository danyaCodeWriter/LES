package com.acme.coordination.deployment;

import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

/**
 * На {@link DeploymentRole#STANDBY} и {@link DeploymentRole#SWITCH} инстансе клиенты не должны вызывать regional
 * coordinator/workers/jobs (ключи etcd за пределами {@code /global/}). Global scope всегда разрешён.
 * {@link DeploymentRole#SWITCH} дополнительно участвует в regional drain (см. {@code RegionSwitchDrainPolicy}).
 */
@Component
public class DeploymentRegistrationPolicy {

  private static final Pattern TENANT_GLOBAL =
      Pattern.compile("^/v1/tenants/[^/]+/global(?:/.*)?$");

  /** Active-region worker/job/key watch (не {@code stream/active-region}). */
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

  /** ACTIVE или SWITCH: участвует в логике {@code REGION_DRAIN} (STANDBY drain не касает). */
  public boolean participatesInRegionalDrain() {
    DeploymentRole r = roleSource.currentRole();
    return r == DeploymentRole.ACTIVE || r == DeploymentRole.SWITCH;
  }

  /** Regional tenant API (не global) для клиентов закрыт — STANDBY или SWITCH. */
  public boolean blocksRegionalTenantMutations() {
    return isStandby() || isSwitch();
  }

  /**
   * Wire value for {@code GET .../active-region} and SSE {@code stream/active-region}: this JVM is the
   * primary site ({@code active}), switch-over ({@code switch}), или stand-in ({@code stand_in}).
   */
  public String instanceStatusWire() {
    if (isSwitch()) {
      return "switch";
    }
    return isActive() ? "active" : "stand_in";
  }

  /**
   * HTTP: на standby/switch блокируем мутации и watch workers/jobs для non-global tenant API.
   */
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

  /** gRPC: на standby/switch блокируем только regional «запись» RPC (global — всегда ок). */
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
        + "active-region coordinator, worker, and job mutations are refused on this host. "
        + "Call the ACTIVE deployment for those paths (same URLs, different base host/port). "
        + "Global APIs under /v1/tenants/{tenant}/global/ stay allowed here. "
        + "RU: вы вызываете regional coordinator/workers/jobs на stand-in узле — отправляйте их на ACTIVE-инстанс.\"}";
  }

  public static String switchBodyJson() {
    return "{\"error\":\"SWITCH_DEPLOYMENT\",\"message\":\"This instance is SWITCH: active-region coordinator, "
        + "worker, and job mutations are refused until deployment role returns to ACTIVE. "
        + "Global APIs under /v1/tenants/{tenant}/global/ stay allowed. "
        + "During REGION_DRAIN the operator may set SWITCH to purge old regional keys faster (see README). "
        + "RU: regional мутации отключены в режиме switch; global доступен.\"}";
  }

  public static String standbyGrpcDetail() {
    return "STANDBY (stand-in): active-region coordinator/worker/job RPCs are disabled on this host; use ACTIVE or global RPCs.";
  }

  public static String switchGrpcDetail() {
    return "SWITCH: active-region coordinator/worker/job RPCs are disabled; use global RPCs or wait for ACTIVE.";
  }
}
