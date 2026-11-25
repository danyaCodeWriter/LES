package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.etcd.Keyspace;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.options.GetOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

/**
 * Force-clears regional lease keys (leader, workers, jobs) under a tenant region namespace.
 * Keeps {@code coordinationEpoch} so fencing stays monotonic. Does not touch GLOBAL or RSS.
 */
@Service
public class DrainService {

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;

  public DrainService(Client client, Keyspace keyspace) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
  }

  public CompletableFuture<DrainReport> drainRegional(String tenantId, String regionId) {
    String regionRoot = keyspace.regionRoot(tenantId, regionId) + "/";
    String epochKey = keyspace.coordinationEpochKey(tenantId, regionId);
    return kv.get(bs(regionRoot), GetOption.newBuilder().isPrefix(true).build())
        .thenCompose(
            resp -> {
              List<CompletableFuture<Void>> revokes = new ArrayList<>();
              List<String> deletedKeys = new ArrayList<>();
              List<CompletableFuture<?>> deletes = new ArrayList<>();
              for (var kvp : resp.getKvs()) {
                String key = keyspace.keyUtf8(kvp);
                if (key.equals(epochKey)) {
                  continue;
                }
                long leaseId = kvp.getLease();
                if (leaseId != 0) {
                  revokes.add(
                      lease
                          .revoke(leaseId)
                          .handle((r, e) -> null)
                          .thenApply(x -> null));
                }
                deletedKeys.add(key);
                deletes.add(kv.delete(bs(key)).handle((r, e) -> null));
              }
              CompletableFuture<Void> allRevoke =
                  revokes.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.allOf(revokes.toArray(CompletableFuture[]::new));
              CompletableFuture<Void> allDelete =
                  deletes.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.allOf(deletes.toArray(CompletableFuture[]::new));
              return allRevoke
                  .thenCompose(v -> allDelete)
                  .thenApply(
                      v ->
                          new DrainReport(
                              tenantId, regionId, deletedKeys.size(), List.copyOf(deletedKeys)));
            });
  }

  /** Optional: wipe global leader/workers/jobs (not KV tickets). Keeps global coordinationEpoch. */
  public CompletableFuture<DrainReport> drainGlobalLeases(String tenantId) {
    String globalPrefix = keyspace.globalSubtreePrefix(tenantId);
    String epochKey = keyspace.globalCoordinationEpochKey(tenantId);
    String kvPrefix = keyspace.globalKvPrefix(tenantId);
    return kv.get(bs(globalPrefix), GetOption.newBuilder().isPrefix(true).build())
        .thenCompose(
            resp -> {
              List<CompletableFuture<Void>> revokes = new ArrayList<>();
              List<String> deletedKeys = new ArrayList<>();
              List<CompletableFuture<?>> deletes = new ArrayList<>();
              for (var kvp : resp.getKvs()) {
                String key = keyspace.keyUtf8(kvp);
                if (key.equals(epochKey) || key.startsWith(kvPrefix)) {
                  continue;
                }
                long leaseId = kvp.getLease();
                if (leaseId != 0) {
                  revokes.add(lease.revoke(leaseId).handle((r, e) -> null).thenApply(x -> null));
                }
                deletedKeys.add(key);
                deletes.add(kv.delete(bs(key)).handle((r, e) -> null));
              }
              CompletableFuture<Void> allRevoke =
                  revokes.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.allOf(revokes.toArray(CompletableFuture[]::new));
              CompletableFuture<Void> allDelete =
                  deletes.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.allOf(deletes.toArray(CompletableFuture[]::new));
              return allRevoke
                  .thenCompose(v -> allDelete)
                  .thenApply(
                      v ->
                          new DrainReport(
                              tenantId, "global", deletedKeys.size(), List.copyOf(deletedKeys)));
            });
  }

  public record DrainReport(String tenantId, String scope, int deletedCount, List<String> deletedKeys) {
    public Map<String, Object> toMap() {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("tenantId", tenantId);
      m.put("scope", scope);
      m.put("deletedCount", deletedCount);
      m.put("deletedKeys", deletedKeys);
      return m;
    }
  }
}
