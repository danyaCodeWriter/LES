package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.model.JobRecord;
import com.acme.coordination.policy.LeaseTtlBounds;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.security.SecureRandom;
import org.springframework.stereotype.Service;

@Service
public class JobLockService {

  private static final SecureRandom RNG = new SecureRandom();

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final TenantKeyQuotaService keyQuota;

  public JobLockService(Client client, Keyspace keyspace, JsonCodec json, TenantKeyQuotaService keyQuota) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
    this.json = json;
    this.keyQuota = keyQuota;
  }

  public CompletableFuture<AcquireJobResult> acquire(
      String tenantId,
      String regionId,
      String jobId,
      String holder,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return acquireAtKey(tenantId, keyspace.jobKey(tenantId, regionId, jobId), jobId, holder, ttlSeconds, metadata);
  }

  public CompletableFuture<AcquireJobResult> acquireGlobal(
      String tenantId,
      String jobId,
      String holder,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return acquireAtKey(tenantId, keyspace.globalJobKey(tenantId, jobId), jobId, holder, ttlSeconds, metadata);
  }

  private CompletableFuture<AcquireJobResult> acquireAtKey(
      String tenantId,
      String key,
      String jobId,
      String holder,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    LeaseTtlBounds.requireValid(ttlSeconds);
    return kv.get(bs(key))
        .thenCompose(
            pre -> {
              CompletableFuture<Void> quota =
                  pre.getKvs().isEmpty()
                      ? keyQuota.assertHeadroom(tenantId, 1)
                      : CompletableFuture.completedFuture(null);
              return quota.thenCompose(
                  v ->
                      lease.grant(ttlSeconds)
                          .thenCompose(
                              grant -> {
                                long leaseId = grant.getID();
                                String lockToken = newToken();
                                JobRecord record = new JobRecord(jobId, holder, lockToken, metadata, Instant.now());

                                Txn txn =
                                    kv.txn()
                                        .If(new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                                        .Then(
                                            Op.put(
                                                bs(key),
                                                bb(json.writeBytes(record)),
                                                PutOption.newBuilder().withLeaseId(leaseId).build()))
                                        .Else(Op.get(bs(key), GetOption.DEFAULT));

                                return txn.commit()
                                    .thenCompose(
                                        resp -> {
                                          if (resp.isSucceeded()) {
                                            return CompletableFuture.completedFuture(
                                                AcquireJobResult.acquired(
                                                    leaseId, resp.getHeader().getRevision(), lockToken));
                                          }
                                          lease.revoke(leaseId);
                                          var get = resp.getGetResponses().getFirst();
                                          if (get.getKvs().isEmpty()) {
                                            return CompletableFuture.completedFuture(
                                                AcquireJobResult.notAcquired(Optional.empty()));
                                          }
                                          JobRecord existing =
                                              json.read(get.getKvs().getFirst().getValue().getBytes(), JobRecord.class);
                                          return CompletableFuture.completedFuture(
                                              AcquireJobResult.notAcquired(Optional.of(existing)));
                                        });
                              }));
            });
  }

  public CompletableFuture<Boolean> releaseIfHolder(String tenantId, String regionId, String jobId, String holder) {
    return releaseAtKey(keyspace.jobKey(tenantId, regionId, jobId), null, holder);
  }

  public CompletableFuture<Boolean> releaseGlobalIfHolder(String tenantId, String jobId, String holder) {
    return releaseAtKey(keyspace.globalJobKey(tenantId, jobId), null, holder);
  }

  /** Preferred: {@code token} from acquire; {@code legacyHolder} when gRPC client still sends holder only (old locks without token in etcd). */
  public CompletableFuture<Boolean> releaseIfCredential(
      String tenantId,
      String regionId,
      String jobId,
      String token,
      String legacyHolder
  ) {
    return releaseAtKey(keyspace.jobKey(tenantId, regionId, jobId), token, legacyHolder);
  }

  public CompletableFuture<Boolean> releaseGlobalIfCredential(String tenantId, String jobId, String token, String legacyHolder) {
    return releaseAtKey(keyspace.globalJobKey(tenantId, jobId), token, legacyHolder);
  }

  public CompletableFuture<Boolean> renewIfHolder(String tenantId, String regionId, String jobId, String holder) {
    return renewAtKey(keyspace.jobKey(tenantId, regionId, jobId), null, holder);
  }

  public CompletableFuture<Boolean> renewGlobalIfHolder(String tenantId, String jobId, String holder) {
    return renewAtKey(keyspace.globalJobKey(tenantId, jobId), null, holder);
  }

  public CompletableFuture<Boolean> renewIfCredential(
      String tenantId,
      String regionId,
      String jobId,
      String token,
      String legacyHolder
  ) {
    return renewAtKey(keyspace.jobKey(tenantId, regionId, jobId), token, legacyHolder);
  }

  public CompletableFuture<Boolean> renewGlobalIfCredential(String tenantId, String jobId, String token, String legacyHolder) {
    return renewAtKey(keyspace.globalJobKey(tenantId, jobId), token, legacyHolder);
  }

  private CompletableFuture<Boolean> releaseAtKey(String key, String token, String legacyHolder) {
    return kv.get(bs(key)).thenCompose(resp -> {
      if (resp.getKvs().isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      var kvp = resp.getKvs().getFirst();
      JobRecord rec = json.read(kvp.getValue().getBytes(), JobRecord.class);
      if (!possessionMatches(rec, token, legacyHolder)) {
        return CompletableFuture.completedFuture(false);
      }
      return kv.delete(bs(key)).thenApply(del -> del.getDeleted() > 0);
    });
  }

  private CompletableFuture<Boolean> renewAtKey(String key, String token, String legacyHolder) {
    return kv.get(bs(key)).thenCompose(resp -> {
      if (resp.getKvs().isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      var kvp = resp.getKvs().getFirst();
      JobRecord rec = json.read(kvp.getValue().getBytes(), JobRecord.class);
      if (!possessionMatches(rec, token, legacyHolder)) {
        return CompletableFuture.completedFuture(false);
      }
      long leaseId = kvp.getLease();
      if (leaseId == 0) {
        return CompletableFuture.completedFuture(false);
      }
      return lease.keepAliveOnce(leaseId).thenApply(x -> true);
    });
  }

  /**
   * If the stored record has a {@link JobRecord#token()}, only {@code token} must match. Otherwise (legacy JSON
   * without token) {@code legacyHolder} must match {@link JobRecord#holder()}.
   */
  static boolean possessionMatches(JobRecord rec, String token, String legacyHolder) {
    String rt = rec.token();
    if (rt != null && !rt.isBlank()) {
      return token != null && !token.isBlank() && token.equals(rt);
    }
    return legacyHolder != null && !legacyHolder.isBlank() && legacyHolder.equals(rec.holder());
  }

  public CompletableFuture<Optional<JobState>> get(String tenantId, String regionId, String jobId) {
    return getAtKey(keyspace.jobKey(tenantId, regionId, jobId));
  }

  public CompletableFuture<Optional<JobState>> getGlobal(String tenantId, String jobId) {
    return getAtKey(keyspace.globalJobKey(tenantId, jobId));
  }

  private CompletableFuture<Optional<JobState>> getAtKey(String key) {
    return kv.get(bs(key)).thenApply(resp -> {
      if (resp.getKvs().isEmpty()) {
        return Optional.empty();
      }
      var kvp = resp.getKvs().getFirst();
      JobRecord rec = json.read(kvp.getValue().getBytes(), JobRecord.class);
      return Optional.of(new JobState(rec, kvp.getLease(), kvp.getModRevision()));
    });
  }

  /** All job locks in the region whose {@link JobRecord#holder()} equals {@code holderWorkerId} (prefix scan). */
  public CompletableFuture<List<JobState>> listJobsForHolder(String tenantId, String regionId, String holderWorkerId) {
    return listJobsForHolderAtPrefix(keyspace.jobsPrefix(tenantId, regionId), holderWorkerId);
  }

  /** Global job pool: jobs held by {@code holderWorkerId}. */
  public CompletableFuture<List<JobState>> listGlobalJobsForHolder(String tenantId, String holderWorkerId) {
    return listJobsForHolderAtPrefix(keyspace.globalJobsPrefix(tenantId), holderWorkerId);
  }

  private CompletableFuture<List<JobState>> listJobsForHolderAtPrefix(String prefix, String holderWorkerId) {
    return kv.get(bs(prefix), GetOption.newBuilder().isPrefix(true).build())
        .thenApply(
            resp -> {
              List<JobState> out = new ArrayList<>();
              for (var kvp : resp.getKvs()) {
                JobRecord rec = json.read(kvp.getValue().getBytes(), JobRecord.class);
                if (holderWorkerId.equals(rec.holder())) {
                  out.add(new JobState(rec, kvp.getLease(), kvp.getModRevision()));
                }
              }
              return out;
            });
  }

  private static String newToken() {
    byte[] bytes = new byte[24];
    RNG.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  public record AcquireJobResult(
      boolean acquired,
      Long leaseId,
      Long revision,
      String token,
      Optional<JobRecord> currentHolder
  ) {
    public static AcquireJobResult acquired(long leaseId, long revision, String token) {
      return new AcquireJobResult(true, leaseId, revision, token, Optional.empty());
    }

    public static AcquireJobResult notAcquired(Optional<JobRecord> current) {
      return new AcquireJobResult(false, null, null, null, current);
    }
  }

  public record JobState(JobRecord record, long leaseId, long modRevision) {}
}
