package com.acme.coordination.sdk.les;

import com.acme.coordination.ring.RingAssignment;
import com.acme.coordination.sdk.session.ApiScope;
import com.acme.coordination.sdk.session.CoordinatorLease;
import com.acme.coordination.sdk.session.LeaseOptions;
import com.acme.coordination.sdk.session.WorkerSession;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
    return new Scoped(ApiScope.GLOBAL, null);
  }

  /** Regional short-path on the LES this client talks to ({@code COORDINATION_REGION}). */
  public Scoped regional() {
    return new Scoped(ApiScope.REGIONAL, client.options().homeRegion());
  }

  /** Drain a specific regional namespace (stretched etcd — any LES can clear any region). */
  public JsonNode drain(String regionId) throws Exception {
    String sre = requireSre();
    return client.rest().drainRegional(tenantId, regionId, sre);
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

    Scoped(ApiScope scope, String ignoredRegionHint) {
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
      return client
          .sessions()
          .holdCoordinator(
              tenantId,
              coordinatorId,
              instanceId,
              scope,
              defaultLease());
    }

    public WorkerSession joinWorker(String workerId) throws Exception {
      return joinWorker(workerId, workerId);
    }

    public WorkerSession joinWorker(String workerId, String instanceId) throws Exception {
      requireGate();
      return client.sessions().holdWorker(tenantId, workerId, instanceId, scope, defaultLease());
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

      /** Acquire freestanding lock, run work, release. Empty if 409 / gate closed. */
      public Optional<Void> tryRun(Runnable work) throws Exception {
        return tryRun(UUID.randomUUID().toString(), work);
      }

      public Optional<Void> tryRun(String holderLabel, Runnable work) throws Exception {
        if (!isActiveGateOpen()) {
          return Optional.empty();
        }
        int ttl = (int) client.options().defaultTtl().toSeconds();
        String idem = UUID.randomUUID().toString();
        JsonNode acq;
        try {
          acq =
              scope == ApiScope.GLOBAL
                  ? client
                      .rest()
                      .acquireJobGlobal(tenantId, jobId, holderLabel, null, ttl, Map.of(), idem)
                  : client.rest().acquireJob(tenantId, jobId, holderLabel, null, ttl, Map.of(), idem);
        } catch (IllegalStateException e) {
          if (e.getMessage() != null
              && (e.getMessage().contains("409") || e.getMessage().contains("JOB_EXISTS"))) {
            return Optional.empty();
          }
          throw e;
        }
        if (!acq.path("acquired").asBoolean(true) || !acq.hasNonNull("token")) {
          return Optional.empty();
        }
        String token = acq.get("token").asText();
        try {
          work.run();
        } finally {
          try {
            if (scope == ApiScope.GLOBAL) {
              client.rest().releaseJobGlobal(tenantId, jobId, token);
            } else {
              client.rest().releaseJob(tenantId, jobId, token);
            }
          } catch (Exception ignored) {
            // best-effort release
          }
        }
        return Optional.of(null);
      }
    }
  }
}
