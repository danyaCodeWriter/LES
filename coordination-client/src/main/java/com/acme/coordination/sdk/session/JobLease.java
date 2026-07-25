package com.acme.coordination.sdk.session;

import com.acme.coordination.sdk.CoordinationRestClient;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exclusive job lock with background renew. {@link #close()} stops renew and releases the job.
 *
 * <p>Use {@link ApiScope#REGIONAL} for one owner in the active DC segment; {@link ApiScope#GLOBAL} for one
 * owner across both DCs (see platform docs).
 */
public final class JobLease implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(JobLease.class);

  private final CoordinationRestClient client;
  private final String tenantId;
  private final ApiScope scope;
  private final String jobId;
  private final String token;
  private final JsonNode acquireResponse;
  private final PeriodicRenewer renewer;
  private final boolean releaseOnClose;

  JobLease(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      ApiScope scope,
      String jobId,
      JsonNode acquireResponse,
      LeaseOptions options,
      boolean releaseOnClose) {
    this.client = Objects.requireNonNull(client);
    this.tenantId = Objects.requireNonNull(tenantId);
    this.scope = Objects.requireNonNull(scope);
    this.jobId = Objects.requireNonNull(jobId);
    this.acquireResponse = Objects.requireNonNull(acquireResponse);
    if (!JsonLeaseResponses.acquiredFlag(acquireResponse)) {
      throw new LeaseNotAcquiredException(
          "job acquire not held: jobId=" + jobId + " body=" + acquireResponse);
    }
    this.token = JsonLeaseResponses.requireToken(acquireResponse, "job");
    this.releaseOnClose = releaseOnClose;
    String label = "job scope=" + scope + " id=" + jobId + " tenant=" + tenantId;
    this.renewer =
        new PeriodicRenewer(
            scheduler, this::doRenew, options.effectiveRenewEvery(), label, options.onLost());
    this.renewer.start();
  }

  private void doRenew() throws Exception {
    if (scope == ApiScope.GLOBAL) {
      client.renewJobGlobal(tenantId, jobId, token);
    } else {
      client.renewJob(tenantId, jobId, token);
    }
  }

  public String token() {
    return token;
  }

  public String jobId() {
    return jobId;
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

  /** Run work while holding the lease; always releases on exit. */
  public <T> T run(Callable<T> work) throws Exception {
    Objects.requireNonNull(work, "work");
    try {
      return work.call();
    } finally {
      close();
    }
  }

  public void run(Runnable work) throws Exception {
    run(
        () -> {
          work.run();
          return null;
        });
  }

  @Override
  public void close() {
    renewer.stopQuietly();
    if (releaseOnClose) {
      try {
        if (scope == ApiScope.GLOBAL) {
          client.releaseJobGlobal(tenantId, jobId, token);
        } else {
          client.releaseJob(tenantId, jobId, token);
        }
      } catch (Exception e) {
        log.warn("job release failed ({}): {}", jobId, e.toString());
      }
    }
    renewer.notifyStopped();
  }

  static JobLease acquire(
      CoordinationRestClient client,
      ScheduledExecutorService scheduler,
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options,
      boolean releaseOnClose)
      throws Exception {
    JsonNode resp;
    try {
      if (scope == ApiScope.GLOBAL) {
        resp =
            client.acquireJobGlobal(
                tenantId, jobId, holder, participantToken, options.ttlSeconds(), metadata);
      } else {
        resp =
            client.acquireJob(
                tenantId, jobId, holder, participantToken, options.ttlSeconds(), metadata);
      }
    } catch (IllegalStateException e) {
      throw new LeaseNotAcquiredException("job acquire failed: " + jobId + " — " + e.getMessage(), e);
    }
    return new JobLease(
        client, scheduler, tenantId, scope, jobId, resp, options, releaseOnClose);
  }
}
