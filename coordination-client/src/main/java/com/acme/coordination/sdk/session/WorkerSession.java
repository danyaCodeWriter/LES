package com.acme.coordination.sdk.session;

import com.acme.coordination.sdk.CoordinationRestClient;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registered worker with background heartbeat. {@link #close()} stops heartbeat and calls deregister.
 */
public final class WorkerSession implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(WorkerSession.class);

  private final CoordinationRestClient client;
  private final String tenantId;
  private final ApiScope scope;
  private final String workerId;
  private final String token;
  private final JsonNode registerResponse;
  private final PeriodicRenewer renewer;
  private final boolean deregisterOnClose;

  WorkerSession(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      ApiScope scope,
      String workerId,
      JsonNode registerResponse,
      LeaseOptions options,
      boolean deregisterOnClose) {
    this.client = Objects.requireNonNull(client);
    this.tenantId = Objects.requireNonNull(tenantId);
    this.scope = Objects.requireNonNull(scope);
    this.workerId = Objects.requireNonNull(workerId);
    this.registerResponse = Objects.requireNonNull(registerResponse);
    if (!JsonLeaseResponses.acquiredFlag(registerResponse)) {
      throw new LeaseNotAcquiredException(
          "worker register not held: workerId=" + workerId + " body=" + registerResponse);
    }
    this.token = JsonLeaseResponses.requireToken(registerResponse, "worker");
    this.deregisterOnClose = deregisterOnClose;
    String label = "worker scope=" + scope + " id=" + workerId + " tenant=" + tenantId;
    this.renewer =
        new PeriodicRenewer(
            scheduler, this::doHeartbeat, options.effectiveRenewEvery(), label, options.onLost());
    this.renewer.start();
  }

  private void doHeartbeat() throws Exception {
    if (scope == ApiScope.GLOBAL) {
      client.workerHeartbeatGlobal(tenantId, workerId, token);
    } else {
      client.workerHeartbeat(tenantId, workerId, token);
    }
  }

  public String token() {
    return token;
  }

  public String workerId() {
    return workerId;
  }

  public ApiScope scope() {
    return scope;
  }

  public JsonNode registerResponse() {
    return registerResponse;
  }

  public boolean isHeld() {
    return renewer.isHeld();
  }

  @Override
  public void close() {
    renewer.stopQuietly();
    if (deregisterOnClose) {
      try {
        if (scope == ApiScope.GLOBAL) {
          client.deregisterWorkerGlobal(tenantId, workerId, token);
        } else {
          client.deregisterWorker(tenantId, workerId, token);
        }
      } catch (Exception e) {
        log.warn("worker deregister failed ({}): {}", workerId, e.toString());
      }
    }
    renewer.notifyStopped();
  }

  static WorkerSession register(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      String workerId,
      String instanceId,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options,
      boolean deregisterOnClose)
      throws Exception {
    JsonNode resp;
    try {
      if (scope == ApiScope.GLOBAL) {
        resp =
            client.registerWorkerGlobal(
                tenantId, workerId, instanceId, options.ttlSeconds(), metadata);
      } else {
        resp =
            client.registerWorker(tenantId, workerId, instanceId, options.ttlSeconds(), metadata);
      }
    } catch (IllegalStateException e) {
      throw new LeaseNotAcquiredException(
          "worker register failed: " + workerId + " — " + e.getMessage(), e);
    }
    return new WorkerSession(
        client, scheduler, tenantId, scope, workerId, resp, options, deregisterOnClose);
  }
}
