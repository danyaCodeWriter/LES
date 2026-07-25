package com.acme.coordination.sdk.session;

import com.acme.coordination.sdk.CoordinationRestClient;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Held coordinator lease with background {@code renew}. There is no explicit release API — {@link #close()}
 * only stops renew (lease expires by TTL).
 */
public final class CoordinatorLease implements AutoCloseable {

  private final CoordinationRestClient client;
  private final String tenantId;
  private final ApiScope scope;
  private final String coordinatorId;
  private final String token;
  private final long fenceEpoch;
  private final JsonNode acquireResponse;
  private final PeriodicRenewer renewer;

  CoordinatorLease(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      ApiScope scope,
      String coordinatorId,
      JsonNode acquireResponse,
      LeaseOptions options) {
    this.client = Objects.requireNonNull(client);
    this.tenantId = Objects.requireNonNull(tenantId);
    this.scope = Objects.requireNonNull(scope);
    this.coordinatorId = Objects.requireNonNull(coordinatorId);
    this.acquireResponse = Objects.requireNonNull(acquireResponse);
    if (!JsonLeaseResponses.acquiredFlag(acquireResponse)) {
      throw new LeaseNotAcquiredException(
          "coordinator acquire not held: coordinatorId=" + coordinatorId + " body=" + acquireResponse);
    }
    this.token = JsonLeaseResponses.requireToken(acquireResponse, "coordinator");
    this.fenceEpoch = JsonLeaseResponses.fenceEpoch(acquireResponse);
    String label = "coordinator scope=" + scope + " id=" + coordinatorId + " tenant=" + tenantId;
    this.renewer =
        new PeriodicRenewer(
            scheduler,
            this::doRenew,
            options.effectiveRenewEvery(),
            label,
            options.onLost());
    this.renewer.start();
  }

  private void doRenew() throws Exception {
    if (scope == ApiScope.GLOBAL) {
      client.renewCoordinatorGlobal(tenantId, token);
    } else {
      client.renewCoordinator(tenantId, token);
    }
  }

  public String token() {
    return token;
  }

  /** Same string as {@link #token()} — use for protected GET / SSE in this contour. */
  public String readToken() {
    return token;
  }

  public long fenceEpoch() {
    return fenceEpoch;
  }

  public String coordinatorId() {
    return coordinatorId;
  }

  public ApiScope scope() {
    return scope;
  }

  public JsonNode acquireResponse() {
    return acquireResponse;
  }

  public boolean isHeld() {
    return renewer.isHeld();
  }

  /** Stop renew; lease will expire server-side after TTL. */
  @Override
  public void close() {
    renewer.stopAndNotify();
  }

  static CoordinatorLease acquire(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      String coordinatorId,
      String instanceId,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options)
      throws Exception {
    java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
    body.put("coordinatorId", coordinatorId);
    body.put("instanceId", instanceId);
    body.put("ttlSeconds", options.ttlSeconds());
    body.put("metadata", metadata == null ? Map.of() : metadata);
    JsonNode resp;
    try {
      if (scope == ApiScope.GLOBAL) {
        resp = client.acquireCoordinatorGlobal(tenantId, body);
      } else {
        resp = client.acquireCoordinator(tenantId, body);
      }
    } catch (IllegalStateException e) {
      throw new LeaseNotAcquiredException(
          "coordinator acquire failed: " + coordinatorId + " — " + e.getMessage(), e);
    }
    return new CoordinatorLease(client, scheduler, tenantId, scope, coordinatorId, resp, options);
  }
}
