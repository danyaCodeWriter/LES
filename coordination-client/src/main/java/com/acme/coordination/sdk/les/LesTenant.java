package com.acme.coordination.sdk.les;

import com.acme.coordination.ring.RingAssignment;
import com.acme.coordination.sdk.session.ApiScope;
import com.acme.coordination.sdk.session.CoordinatorLease;
import com.acme.coordination.sdk.session.JobLease;
import com.acme.coordination.sdk.session.LeaseNotAcquiredException;
import com.acme.coordination.sdk.session.LeaseOptions;
import com.acme.coordination.sdk.session.WorkerSession;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Scoped Les operations for one tenant. */
public final class LesTenant {

  private final LesClient client;
  private final String tenantId;

  LesTenant(LesClient client, String tenantId) {
    this.client = client;
    this.tenantId = Objects.requireNonNull(tenantId);
  }

  public String tenantId() {
    return tenantId;
  }

  public Scoped global() {
    return new Scoped(ApiScope.GLOBAL);
  }

  /** Regional short-path on the LES this client talks to ({@code COORDINATION_REGION}). */
  public Scoped regional() {
    return new Scoped(ApiScope.REGIONAL);
  }

  /** Drain a specific regional namespace (stretched etcd — any LES can clear any region). */
  public JsonNode drain(String regionId) throws Exception {
    return client.rest().drainRegional(tenantId, regionId, requireSre());
  }

  public JsonNode drainGlobal() throws Exception {
    return client.rest().drainGlobal(tenantId, requireSre());
  }

  private String requireSre() {
    String sre = client.options().sreToken();
    if (sre == null || sre.isBlank()) {
      throw new IllegalStateException("sreToken required for drain — LesClient.connect(...).sreToken(...)");
    }
    return sre;
  }

  public final class Scoped {
    private final ApiScope scope;
    private Supplier<Boolean> onlyWhen = () -> true;

    Scoped(ApiScope scope) {
      this.scope = scope;
    }

    /** Skip acquires when supplier is false (e.g. RSS {@code amIActive}). */
    public Scoped onlyWhen(Supplier<Boolean> gate) {
      this.onlyWhen = gate == null ? () -> true : gate;
      return this;
    }

    public boolean isActiveGateOpen() {
      return Boolean.TRUE.equals(onlyWhen.get());
    }

    public CoordinatorLease becomeLeader(String coordinatorId) throws Exception {
      return becomeLeader(coordinatorId, UUID.randomUUID().toString());
    }

    public CoordinatorLease becomeLeader(String coordinatorId, String instanceId) throws Exception {
      requireGate();
      return client.sessions().holdCoordinator(tenantId, coordinatorId, instanceId, scope, defaultLease());
    }

    public WorkerSession joinWorker(String workerId) throws Exception {
      return joinWorker(workerId, workerId);
    }

    public WorkerSession joinWorker(String workerId, String instanceId) throws Exception {
      requireGate();
      return client.sessions().holdWorker(tenantId, workerId, instanceId, scope, defaultLease());
    }

    /** Live worker pool helpers (ids + hash ring) for a held coordinator. */
    public WorkerPool workers(CoordinatorLease leader) throws Exception {
      Objects.requireNonNull(leader, "leader");
      return new WorkerPool(client, tenantId, scope, leader.token());
    }

    /** Freestanding exclusive lock. */
    public ExclusiveLock lock(String jobId) {
      return new ExclusiveLock(jobId);
    }

    public KvFlags flags() {
      if (scope != ApiScope.GLOBAL) {
        throw new IllegalStateException("flags()/KV helpers are GLOBAL-only");
      }
      return new KvFlags(client, tenantId);
    }

    public OpsWatch opsWatch(String coordinatorReadToken) {
      return new OpsWatch(client.rest(), tenantId, coordinatorReadToken, scope);
    }

    public RingAssignment hashRing(List<String> workerIds) throws Exception {
      return client.rest().assignRing(workerIds, 128);
    }

    public RingAssignment hashRing(List<String> workerIds, int virtualNodes) throws Exception {
      return client.rest().assignRing(workerIds, virtualNodes);
    }

    private LeaseOptions defaultLease() {
      return LeaseOptions.ofTtlSeconds((int) client.options().defaultTtl().toSeconds());
    }

    private void requireGate() {
      if (!isActiveGateOpen()) {
        throw new IllegalStateException("Les gate closed (onlyWhen / RSS not active)");
      }
    }

    public final class ExclusiveLock {
      private final String jobId;

      ExclusiveLock(String jobId) {
        this.jobId = jobId;
      }

      /**
       * Hold freestanding lock with <strong>background renew</strong> until closed.
       * Prefer for long work; {@link #tryRun} also renews while the runnable runs.
       */
      public Optional<JobLease> tryHold() throws Exception {
        return tryHold(UUID.randomUUID().toString());
      }

      public Optional<JobLease> tryHold(String holderLabel) throws Exception {
        if (!isActiveGateOpen()) {
          return Optional.empty();
        }
        try {
          return Optional.of(
              client
                  .sessions()
                  .holdFreestandingJob(tenantId, jobId, holderLabel, scope, defaultLease()));
        } catch (LeaseNotAcquiredException e) {
          return Optional.empty();
        } catch (IllegalStateException e) {
          if (e.getMessage() != null
              && (e.getMessage().contains("409") || e.getMessage().contains("JOB_EXISTS"))) {
            return Optional.empty();
          }
          throw e;
        }
      }

      /** Acquire with renew, run work, release on exit. Empty if 409 / gate closed. */
      public Optional<Void> tryRun(Runnable work) throws Exception {
        return tryRun(UUID.randomUUID().toString(), work);
      }

      public Optional<Void> tryRun(String holderLabel, Runnable work) throws Exception {
        Optional<JobLease> held = tryHold(holderLabel);
        if (held.isEmpty()) {
          return Optional.empty();
        }
        try (JobLease lease = held.get()) {
          work.run();
        }
        return Optional.of(null);
      }
    }
  }

  /** Coordinator-authenticated worker listing + consistent hash. */
  public static final class WorkerPool {
    private final LesClient client;
    private final String tenantId;
    private final ApiScope scope;
    private final String coordToken;

    WorkerPool(LesClient client, String tenantId, ApiScope scope, String coordToken) {
      this.client = client;
      this.tenantId = tenantId;
      this.scope = scope;
      this.coordToken = coordToken;
    }

    public List<String> ids() throws Exception {
      JsonNode n =
          scope == ApiScope.GLOBAL
              ? client.rest().getWorkerIdsGlobal(tenantId, coordToken)
              : client.rest().getWorkerIdsActive(tenantId, coordToken);
      List<String> out = new ArrayList<>();
      if (n != null && n.isArray()) {
        n.forEach(el -> out.add(el.asText()));
      }
      return out;
    }

    public RingAssignment hashRing() throws Exception {
      return client.rest().assignRing(ids(), 128);
    }

    public RingAssignment hashRing(int virtualNodes) throws Exception {
      return client.rest().assignRing(ids(), virtualNodes);
    }

    /**
     * Poll membership and invoke {@code onChange} when the sorted id list changes.
     * Returns a closer that stops the poller.
     */
    public AutoCloseable onMembershipChanged(Consumer<List<String>> onChange, long pollMillis) {
      Thread t =
          new Thread(
              () -> {
                List<String> prev = List.of();
                while (!Thread.currentThread().isInterrupted()) {
                  try {
                    List<String> cur = ids();
                    if (!cur.equals(prev)) {
                      prev = List.copyOf(cur);
                      onChange.accept(prev);
                    }
                    Thread.sleep(Math.max(200, pollMillis));
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                  } catch (Exception ignored) {
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                      Thread.currentThread().interrupt();
                      return;
                    }
                  }
                }
              },
              "les-worker-pool-watch");
      t.setDaemon(true);
      t.start();
      return () -> t.interrupt();
    }
  }
}
