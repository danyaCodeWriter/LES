package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.policy.LeaseTtlBounds;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.model.WorkerRecord;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

@Service
public class WorkerRegistryService {

  private static final SecureRandom RNG = new SecureRandom();

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final TenantKeyQuotaService keyQuota;

  public WorkerRegistryService(Client client, Keyspace keyspace, JsonCodec json, TenantKeyQuotaService keyQuota) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
    this.json = json;
    this.keyQuota = keyQuota;
  }

  public CompletableFuture<RegisterResult> register(
      String tenantId,
      String regionId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return registerAtKey(tenantId, keyspace.workerKey(tenantId, regionId, workerId), workerId, instanceId, ttlSeconds, metadata);
  }

  /** Shared worker pool for the tenant (etcd {@code .../global/workers/...}). */
  public CompletableFuture<RegisterResult> registerGlobal(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return registerAtKey(tenantId, keyspace.globalWorkerKey(tenantId, workerId), workerId, instanceId, ttlSeconds, metadata);
  }

  private CompletableFuture<RegisterResult> registerAtKey(
      String tenantId,
      String key,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    LeaseTtlBounds.requireValid(ttlSeconds);
    String token = newToken();

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
                                WorkerRecord record =
                                    new WorkerRecord(workerId, instanceId, token, metadata, Instant.now());

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
                                                new RegisterResult(true, token, leaseId, Optional.empty()));
                                          }
                                          lease.revoke(leaseId);
                                          var get = resp.getGetResponses().getFirst();
                                          if (get.getKvs().isEmpty()) {
                                            return CompletableFuture.completedFuture(
                                                new RegisterResult(false, null, null, Optional.empty()));
                                          }
                                          WorkerRecord existing =
                                              json.read(get.getKvs().getFirst().getValue().getBytes(), WorkerRecord.class);
                                          return CompletableFuture.completedFuture(
                                              new RegisterResult(false, null, null, Optional.of(existing)));
                                        });
                              }));
            });
  }

  public CompletableFuture<Optional<WorkerRecord>> getWorker(String tenantId, String regionId, String workerId) {
    return getWorkerAtKey(keyspace.workerKey(tenantId, regionId, workerId));
  }

  public CompletableFuture<Optional<WorkerRecord>> getWorkerGlobal(String tenantId, String workerId) {
    return getWorkerAtKey(keyspace.globalWorkerKey(tenantId, workerId));
  }

  private CompletableFuture<Optional<WorkerRecord>> getWorkerAtKey(String key) {
    return kv.get(bs(key))
        .thenApply(
            resp -> {
              if (resp.getKvs().isEmpty()) {
                return Optional.empty();
              }
              return Optional.of(json.read(resp.getKvs().getFirst().getValue().getBytes(), WorkerRecord.class));
            });
  }

  public CompletableFuture<Boolean> heartbeat(String tenantId, String regionId, String workerId, String token) {
    return heartbeatAtKey(keyspace.workerKey(tenantId, regionId, workerId), token);
  }

  public CompletableFuture<Boolean> heartbeatGlobal(String tenantId, String workerId, String token) {
    return heartbeatAtKey(keyspace.globalWorkerKey(tenantId, workerId), token);
  }

  private CompletableFuture<Boolean> heartbeatAtKey(String key, String token) {
    return kv.get(bs(key)).thenCompose(resp -> {
      if (resp.getKvs().isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      var kvp = resp.getKvs().getFirst();
      WorkerRecord rec = json.read(kvp.getValue().getBytes(), WorkerRecord.class);
      if (!token.equals(rec.token())) {
        return CompletableFuture.completedFuture(false);
      }
      long leaseId = kvp.getLease();
      if (leaseId == 0) {
        return CompletableFuture.completedFuture(false);
      }
      return lease.keepAliveOnce(leaseId).thenApply(x -> true);
    });
  }

  public CompletableFuture<Boolean> deregisterIfTokenMatches(String tenantId, String regionId, String workerId, String token) {
    return deregisterAtKey(keyspace.workerKey(tenantId, regionId, workerId), token);
  }

  public CompletableFuture<Boolean> deregisterGlobalIfTokenMatches(String tenantId, String workerId, String token) {
    return deregisterAtKey(keyspace.globalWorkerKey(tenantId, workerId), token);
  }

  private CompletableFuture<Boolean> deregisterAtKey(String key, String token) {
    return kv.get(bs(key)).thenCompose(resp -> {
      if (resp.getKvs().isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      var kvp = resp.getKvs().getFirst();
      WorkerRecord rec = json.read(kvp.getValue().getBytes(), WorkerRecord.class);
      if (!token.equals(rec.token())) {
        return CompletableFuture.completedFuture(false);
      }
      return kv.delete(bs(key)).thenApply(del -> del.getDeleted() > 0);
    });
  }

  public CompletableFuture<List<WorkerState>> listWorkers(String tenantId, String regionId) {
    return listWorkersAtPrefix(keyspace.workersPrefix(tenantId, regionId));
  }

  public CompletableFuture<List<WorkerState>> listWorkersGlobal(String tenantId) {
    return listWorkersAtPrefix(keyspace.globalWorkersPrefix(tenantId));
  }

  /** Sorted worker ids (stable input for consistent-hash range computation). */
  public CompletableFuture<List<String>> listWorkerIds(String tenantId, String regionId) {
    return listWorkers(tenantId, regionId).thenApply(WorkerRegistryService::sortedWorkerIds);
  }

  public CompletableFuture<List<String>> listWorkerIdsGlobal(String tenantId) {
    return listWorkersGlobal(tenantId).thenApply(WorkerRegistryService::sortedWorkerIds);
  }

  private static List<String> sortedWorkerIds(List<WorkerState> states) {
    return states.stream()
        .map(s -> s.record().workerId())
        .sorted(Comparator.naturalOrder())
        .toList();
  }

  private CompletableFuture<List<WorkerState>> listWorkersAtPrefix(String prefix) {
    return kv.get(bs(prefix), GetOption.newBuilder().isPrefix(true).build())
        .thenApply(resp -> resp.getKvs().stream().map(kvp -> {
          WorkerRecord record = json.read(kvp.getValue().getBytes(), WorkerRecord.class);
          return new WorkerState(record, kvp.getLease(), kvp.getModRevision());
        }).toList());
  }

  private static String newToken() {
    byte[] bytes = new byte[24];
    RNG.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  public record RegisterResult(
      boolean created,
      String token,
      Long leaseId,
      Optional<WorkerRecord> alreadyRegistered
  ) {}

  public record WorkerState(WorkerRecord record, long leaseId, long modRevision) {}
}
