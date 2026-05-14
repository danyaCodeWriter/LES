package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.policy.LeaseTtlBounds;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.model.CoordinatorRecord;
import com.acme.coordination.model.EpochRecord;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

/**
 * Regional and global coordinator election over etcd: {@link #acquire} uses a transaction that
 * succeeds only when the leader key is absent ({@code create_revision == 0}) and the coordination
 * epoch CAS matches. The leader record is stored with an etcd lease; when renews stop,
 * etcd drops the key and another instance can acquire (after any concurrent epoch races settle).
 */
@Service
public class CoordinatorService {

  private static final int MAX_EPOCH_CAS_RETRIES = 16;
  private static final SecureRandom RNG = new SecureRandom();

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final TenantKeyQuotaService keyQuota;

  public CoordinatorService(
      Client client, Keyspace keyspace, JsonCodec json, TenantKeyQuotaService keyQuota) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
    this.json = json;
    this.keyQuota = keyQuota;
  }

  public CompletableFuture<AcquireResult> acquire(
      String tenantId,
      String regionId,
      String coordinatorId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    LeaseTtlBounds.requireValid(ttlSeconds);
    return acquireWithEpochCas(
        tenantId,
        keyspace.leaderKey(tenantId, regionId),
        keyspace.coordinationEpochKey(tenantId, regionId),
        coordinatorId,
        instanceId,
        ttlSeconds,
        metadata,
        0);
  }

  /** Single coordinator for the tenant across all regions (etcd {@code .../global/leader}). */
  public CompletableFuture<AcquireResult> acquireGlobal(
      String tenantId,
      String coordinatorId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    LeaseTtlBounds.requireValid(ttlSeconds);
    return acquireWithEpochCas(
        tenantId,
        keyspace.globalLeaderKey(tenantId),
        keyspace.globalCoordinationEpochKey(tenantId),
        coordinatorId,
        instanceId,
        ttlSeconds,
        metadata,
        0);
  }

  private CompletableFuture<AcquireResult> acquireWithEpochCas(
      String tenantId,
      String leaderKey,
      String epochKey,
      String coordinatorId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata,
      int attempt
  ) {
    if (attempt > MAX_EPOCH_CAS_RETRIES) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Failed to acquire coordinator after concurrent epoch updates; retry"));
    }

    return kv.get(bs(epochKey)).thenCompose(epochResp -> {
      boolean epochExists = !epochResp.getKvs().isEmpty();
      long epochMod = epochExists ? epochResp.getKvs().getFirst().getModRevision() : 0;
      long epochVal = 0;
      if (epochExists) {
        epochVal = json.read(epochResp.getKvs().getFirst().getValue().getBytes(), EpochRecord.class).epoch();
      }
      long newEpoch = epochVal + 1;

      return kv.get(bs(leaderKey))
          .thenCompose(
              leaderResp -> {
                boolean leaderOccupied = !leaderResp.getKvs().isEmpty();
                CompletableFuture<Void> quotaStep = CompletableFuture.completedFuture(null);
                if (!leaderOccupied) {
                  int keysNeeded = epochExists ? 1 : 2;
                  quotaStep = keyQuota.assertHeadroom(tenantId, keysNeeded);
                }
                return quotaStep.thenCompose(
                    v ->
                        lease.grant(ttlSeconds)
                            .thenCompose(
                                grant -> {
                                  long leaseId = grant.getID();
                                  String leaderToken = newToken();
                                  CoordinatorRecord rec =
                                      new CoordinatorRecord(
                                          coordinatorId, instanceId, leaderToken, metadata, Instant.now(), newEpoch);
                                  EpochRecord epochRec = new EpochRecord(newEpoch);

                                  Cmp leaderCmp = new Cmp(bs(leaderKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0));
                                  Cmp epochCmp =
                                      epochExists
                                          ? new Cmp(bs(epochKey), Cmp.Op.EQUAL, CmpTarget.modRevision(epochMod))
                                          : new Cmp(bs(epochKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0));

                                  Txn txn =
                                      kv.txn()
                                          .If(leaderCmp, epochCmp)
                                          .Then(
                                              Op.put(bs(epochKey), bb(json.writeBytes(epochRec)), PutOption.DEFAULT),
                                              Op.put(
                                                  bs(leaderKey),
                                                  bb(json.writeBytes(rec)),
                                                  PutOption.newBuilder().withLeaseId(leaseId).build()))
                                          .Else(Op.get(bs(leaderKey), GetOption.DEFAULT));

                                  return txn.commit()
                                      .thenCompose(
                                          resp ->
                                              handleAcquireTxnResponse(
                                                  tenantId,
                                                  leaderKey,
                                                  epochKey,
                                                  coordinatorId,
                                                  instanceId,
                                                  ttlSeconds,
                                                  metadata,
                                                  attempt,
                                                  leaseId,
                                                  newEpoch,
                                                  leaderToken,
                                                  resp));
                                }));
              });
    });
  }

  private static String newToken() {
    byte[] b = new byte[24];
    RNG.nextBytes(b);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  private CompletableFuture<AcquireResult> handleAcquireTxnResponse(
      String tenantId,
      String leaderKey,
      String epochKey,
      String coordinatorId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata,
      int attempt,
      long leaseId,
      long newEpoch,
      String leaderToken,
      TxnResponse resp
  ) {
    if (resp.isSucceeded()) {
      return CompletableFuture.completedFuture(
          AcquireResult.acquired(leaseId, resp.getHeader().getRevision(), newEpoch, leaderToken));
    }

    lease.revoke(leaseId);

    var get = resp.getGetResponses().getFirst();
    if (!get.getKvs().isEmpty()) {
      CoordinatorRecord current = json.read(get.getKvs().getFirst().getValue().getBytes(), CoordinatorRecord.class);
      return CompletableFuture.completedFuture(AcquireResult.notAcquired(Optional.of(current)));
    }

    // Lost CAS on epoch while leader slot still free — retry.
    return acquireWithEpochCas(
        tenantId, leaderKey, epochKey, coordinatorId, instanceId, ttlSeconds, metadata, attempt + 1);
  }

  public CompletableFuture<Optional<CoordinatorState>> get(String tenantId, String regionId) {
    return getByLeaderKey(keyspace.leaderKey(tenantId, regionId));
  }

  public CompletableFuture<Optional<CoordinatorState>> getGlobal(String tenantId) {
    return getByLeaderKey(keyspace.globalLeaderKey(tenantId));
  }

  private CompletableFuture<Optional<CoordinatorState>> getByLeaderKey(String leaderKey) {
    return kv.get(bs(leaderKey)).thenApply(resp -> {
      if (resp.getKvs().isEmpty()) {
        return Optional.empty();
      }
      var kvp = resp.getKvs().getFirst();
      CoordinatorRecord rec = json.read(kvp.getValue().getBytes(), CoordinatorRecord.class);
      return Optional.of(new CoordinatorState(rec, kvp.getLease(), kvp.getModRevision()));
    });
  }

  public CompletableFuture<Boolean> renewIfHolder(String tenantId, String regionId, String token) {
    return renewIfHolderByLeaderKey(keyspace.leaderKey(tenantId, regionId), token);
  }

  public CompletableFuture<Boolean> renewGlobalIfHolder(String tenantId, String token) {
    return renewIfHolderByLeaderKey(keyspace.globalLeaderKey(tenantId), token);
  }

  private CompletableFuture<Boolean> renewIfHolderByLeaderKey(String leaderKey, String token) {
    if (token == null || token.isBlank()) {
      return CompletableFuture.completedFuture(false);
    }
    return kv.get(bs(leaderKey)).thenCompose(resp -> {
      if (resp.getKvs().isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      var kvp = resp.getKvs().getFirst();
      CoordinatorRecord rec = json.read(kvp.getValue().getBytes(), CoordinatorRecord.class);
      if (rec.token() == null || rec.token().isBlank() || !token.equals(rec.token())) {
        return CompletableFuture.completedFuture(false);
      }
      long leaseId = kvp.getLease();
      if (leaseId == 0) {
        return CompletableFuture.completedFuture(false);
      }
      return lease.keepAliveOnce(leaseId).thenApply(x -> true);
    });
  }

  public CompletableFuture<Map<String, CoordinatorState>> listCoordinatorsAllRegions(String tenantId) {
    String prefix = keyspace.regionsPrefix(tenantId);
    return kv.get(bs(prefix), GetOption.newBuilder().isPrefix(true).build()).thenApply(resp -> {
      Map<String, CoordinatorState> out = new LinkedHashMap<>();
      for (var kvp : resp.getKvs()) {
        String key = keyspace.keyUtf8(kvp);
        keyspace.parseRegionFromLeaderKey(key, tenantId).ifPresent(region -> {
          CoordinatorRecord rec = json.read(kvp.getValue().getBytes(), CoordinatorRecord.class);
          out.put(region, new CoordinatorState(rec, kvp.getLease(), kvp.getModRevision()));
        });
      }
      return out;
    });
  }

  public record AcquireResult(
      boolean acquired,
      Long leaseId,
      Long revision,
      Long fenceEpoch,
      Optional<CoordinatorRecord> currentHolder,
      String token
  ) {
    public static AcquireResult acquired(long leaseId, long revision, long fenceEpoch, String token) {
      return new AcquireResult(true, leaseId, revision, fenceEpoch, Optional.empty(), token);
    }

    public static AcquireResult notAcquired(Optional<CoordinatorRecord> current) {
      return new AcquireResult(false, null, null, current.map(CoordinatorRecord::fenceEpoch).orElse(null), current, null);
    }
  }

  public record CoordinatorState(CoordinatorRecord record, long leaseId, long modRevision) {}
}
