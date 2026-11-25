package com.acme.coordination.sdk.session;

import com.acme.coordination.sdk.CoordinationRestClient;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for {@link CoordinatorLease}, {@link WorkerSession}, and {@link JobLease} with shared
 * auto-renew scheduler.
 *
 * <pre>{@code
 * try (var sessions = new CoordinationSessions(rest);
 *      var worker = sessions.holdWorker(tenant, "w1", "pod-1", ApiScope.REGIONAL, LeaseOptions.ofTtlSeconds(30));
 *      var job = sessions.holdJob(tenant, "pay-batch", worker.workerId(), worker.token(),
 *                                 ApiScope.REGIONAL, LeaseOptions.ofTtlSeconds(30))) {
 *   // exclusive work; renew runs in background
 * }
 * }</pre>
 */
public final class CoordinationSessions implements AutoCloseable {

  private static final AtomicInteger POOL = new AtomicInteger();

  private final CoordinationRestClient client;
  private final ScheduledExecutorService scheduler;
  private final boolean shutdownSchedulerOnClose;

  public CoordinationSessions(CoordinationRestClient client) {
    this(
        client,
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "coordination-lease-" + POOL.incrementAndGet());
              t.setDaemon(true);
              return t;
            }),
        true);
  }

  public CoordinationSessions(CoordinationRestClient client, ScheduledExecutorService scheduler) {
    this(client, scheduler, false);
  }

  private CoordinationSessions(
      CoordinationRestClient client, ScheduledExecutorService scheduler, boolean shutdownSchedulerOnClose) {
    this.client = Objects.requireNonNull(client);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.shutdownSchedulerOnClose = shutdownSchedulerOnClose;
  }

  public CoordinationRestClient client() {
    return client;
  }

  public CoordinatorLease holdCoordinator(
      String tenantId,
      String coordinatorId,
      String instanceId,
      ApiScope scope,
      LeaseOptions options)
      throws Exception {
    return holdCoordinator(tenantId, coordinatorId, instanceId, Map.of(), scope, options);
  }

  public CoordinatorLease holdCoordinator(
      String tenantId,
      String coordinatorId,
      String instanceId,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options)
      throws Exception {
    return CoordinatorLease.acquire(
        client, scheduler, tenantId, coordinatorId, instanceId, metadata, scope, options);
  }

  public WorkerSession holdWorker(
      String tenantId, String workerId, String instanceId, ApiScope scope, LeaseOptions options)
      throws Exception {
    return holdWorker(tenantId, workerId, instanceId, Map.of(), scope, options, true);
  }

  public WorkerSession holdWorker(
      String tenantId,
      String workerId,
      String instanceId,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options,
      boolean deregisterOnClose)
      throws Exception {
    return WorkerSession.register(
        client,
        scheduler,
        tenantId,
        workerId,
        instanceId,
        metadata,
        scope,
        options,
        deregisterOnClose);
  }

  public JobLease holdJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      ApiScope scope,
      LeaseOptions options)
      throws Exception {
    return holdJob(tenantId, jobId, holder, participantToken, Map.of(), scope, options, true);
  }

  /**
   * Freestanding job lock (no worker/coordinator participant) with background renew.
   * Prefer this for long-running exclusive work.
   */
  public JobLease holdFreestandingJob(
      String tenantId, String jobId, String holderLabel, ApiScope scope, LeaseOptions options)
      throws Exception {
    return holdJob(tenantId, jobId, holderLabel, null, Map.of(), scope, options, true);
  }

  public JobLease holdJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      Map<String, String> metadata,
      ApiScope scope,
      LeaseOptions options,
      boolean releaseOnClose)
      throws Exception {
    return JobLease.acquire(
        client,
        scheduler,
        tenantId,
        jobId,
        holder,
        participantToken,
        metadata,
        scope,
        options,
        releaseOnClose);
  }

  /**
   * Acquire job, run {@code work}, release. Prefer {@link ApiScope#GLOBAL} when exclusivity must survive DC
   * switchover.
   */
  public <T> T runExclusiveJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      ApiScope scope,
      LeaseOptions options,
      Callable<T> work)
      throws Exception {
    try (JobLease lease = holdJob(tenantId, jobId, holder, participantToken, scope, options)) {
      return work.call();
    }
  }

  public void runExclusiveJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      ApiScope scope,
      LeaseOptions options,
      Runnable work)
      throws Exception {
    runExclusiveJob(
        tenantId,
        jobId,
        holder,
        participantToken,
        scope,
        options,
        () -> {
          work.run();
          return null;
        });
  }

  @Override
  public void close() {
    if (!shutdownSchedulerOnClose) {
      return;
    }
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
