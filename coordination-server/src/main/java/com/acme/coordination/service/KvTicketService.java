package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.api.error.ResourceConflictException;
import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.model.KvTicketRecord;
import com.acme.coordination.policy.LeaseTtlBounds;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;

/**
 * Simple tenant-global KV under etcd {@code .../tenants/{tenant}/global/kv/...} with per-key access token (ticket).
 */
@Service
public class KvTicketService {

  private static final SecureRandom RNG = new SecureRandom();

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final TenantKeyQuotaService keyQuota;

  public KvTicketService(Client client, Keyspace keyspace, JsonCodec json, TenantKeyQuotaService keyQuota) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
    this.json = json;
    this.keyQuota = keyQuota;
  }

  public static String extractAccessToken(ServerHttpRequest req) {
    String auth = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (auth != null) {
      String a = auth.trim();
      if (a.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
        return a.substring("Bearer ".length()).trim();
      }
    }
    String x = req.getHeaders().getFirst("X-Kv-Access-Token");
    return x == null ? "" : x.trim();
  }

  /**
   * Create (no access token header) or replace (Authorization Bearer / X-Kv-Access-Token = ticket from create).
   */
  public CompletableFuture<PutResponse> put(
      String tenantId, String keyId, JsonNode value, Integer ttlSeconds, String accessTokenForReplace
  ) {
    if (value == null || value.isNull() || value.isMissingNode()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(HttpStatus.BAD_REQUEST, "KV_VALUE_REQUIRED", "JSON body must include a non-null \"value\"."));
    }
    String key = keyspace.globalKvKey(tenantId, keyId);
    if (accessTokenForReplace != null && !accessTokenForReplace.isBlank()) {
      return replace(key, keyId, value, ttlSeconds, accessTokenForReplace);
    }
    validateKvTtl(ttlSeconds);
    return create(tenantId, key, keyId, value, ttlSeconds);
  }

  public CompletableFuture<GetResponse> get(String tenantId, String keyId, String accessToken) {
    if (accessToken == null || accessToken.isBlank()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(
              HttpStatus.UNAUTHORIZED,
              "KV_ACCESS_TOKEN_REQUIRED",
              "Send Authorization: Bearer <ticket from create> or X-Kv-Access-Token."));
    }
    String key = keyspace.globalKvKey(tenantId, keyId);
    return kv.get(bs(key))
        .thenApply(
            resp -> {
              if (resp.getKvs().isEmpty()) {
                throw new ParticipantException(HttpStatus.NOT_FOUND, "KV_NOT_FOUND", "No such key in this tenant.");
              }
              var kvp = resp.getKvs().getFirst();
              KvTicketRecord rec = json.read(kvp.getValue().getBytes(), KvTicketRecord.class);
              if (!accessToken.equals(rec.accessToken())) {
                throw new ParticipantException(
                    HttpStatus.FORBIDDEN, "KV_ACCESS_FORBIDDEN", "Access token does not match this key.");
              }
              return new GetResponse(rec.value(), kvp.getLease(), kvp.getModRevision());
            });
  }

  public CompletableFuture<Map<String, Object>> delete(String tenantId, String keyId, String accessToken) {
    if (accessToken == null || accessToken.isBlank()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(
              HttpStatus.UNAUTHORIZED,
              "KV_ACCESS_TOKEN_REQUIRED",
              "Send Authorization: Bearer <ticket from create> or X-Kv-Access-Token."));
    }
    String key = keyspace.globalKvKey(tenantId, keyId);
    return kv.get(bs(key))
        .thenCompose(
            resp -> {
              if (resp.getKvs().isEmpty()) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(HttpStatus.NOT_FOUND, "KV_NOT_FOUND", "No such key in this tenant."));
              }
              var kvp = resp.getKvs().getFirst();
              KvTicketRecord rec = json.read(kvp.getValue().getBytes(), KvTicketRecord.class);
              if (!accessToken.equals(rec.accessToken())) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(
                        HttpStatus.FORBIDDEN, "KV_ACCESS_FORBIDDEN", "Access token does not match this key."));
              }
              return kv.delete(bs(key)).thenApply(del -> Map.<String, Object>of("deleted", del.getDeleted() > 0));
            });
  }

  private CompletableFuture<PutResponse> create(String tenantId, String etcdKey, String keyId, JsonNode value, Integer ttlSeconds) {
    return keyQuota
        .assertHeadroom(tenantId, 1)
        .thenCompose(
            v -> {
              String access = newAccessToken();
              KvTicketRecord rec = new KvTicketRecord(access, value);
              byte[] bytes = json.writeBytes(rec);
              if (ttlSeconds == null) {
                Txn txn =
                    kv.txn()
                        .If(new Cmp(bs(etcdKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                        .Then(Op.put(bs(etcdKey), bb(bytes), PutOption.DEFAULT))
                        .Else(Op.get(bs(etcdKey), GetOption.DEFAULT));
                return txn.commit()
                    .thenApply(
                        resp -> {
                          if (resp.isSucceeded()) {
                            return new PutResponse(true, keyId, access, 0L, resp.getHeader().getRevision());
                          }
                          throw new ResourceConflictException(
                              "KV_EXISTS",
                              "Key already exists; send the ticket in Authorization to update.",
                              Map.of("keyId", keyId));
                        });
              }
              return lease.grant(ttlSeconds)
                  .thenCompose(
                      grant -> {
                        long leaseId = grant.getID();
                        PutOption po = PutOption.newBuilder().withLeaseId(leaseId).build();
                        Txn txn =
                            kv.txn()
                                .If(new Cmp(bs(etcdKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                                .Then(Op.put(bs(etcdKey), bb(bytes), po))
                                .Else(Op.get(bs(etcdKey), GetOption.DEFAULT));
                        return txn.commit()
                            .thenCompose(
                                resp -> {
                                  if (resp.isSucceeded()) {
                                    return CompletableFuture.completedFuture(
                                        new PutResponse(true, keyId, access, leaseId, resp.getHeader().getRevision()));
                                  }
                                  return lease.revoke(leaseId)
                                      .thenApply(
                                          x -> {
                                            throw new ResourceConflictException(
                                                "KV_EXISTS",
                                                "Key already exists; send the ticket in Authorization to update.",
                                                Map.of("keyId", keyId));
                                          });
                                });
                      });
            });
  }

  private CompletableFuture<PutResponse> replace(
      String etcdKey, String keyId, JsonNode newValue, Integer ttlSeconds, String accessToken
  ) {
    validateKvTtl(ttlSeconds);
    return kv.get(bs(etcdKey))
        .thenCompose(
            resp -> {
              if (resp.getKvs().isEmpty()) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(HttpStatus.NOT_FOUND, "KV_NOT_FOUND", "No such key in this tenant."));
              }
              var kvp = resp.getKvs().getFirst();
              KvTicketRecord old;
              try {
                old = json.read(kvp.getValue().getBytes(), KvTicketRecord.class);
              } catch (Exception e) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(
                        HttpStatus.BAD_REQUEST, "KV_CORRUPT", "Stored value is not a valid KV ticket record."));
              }
              if (!accessToken.equals(old.accessToken())) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(
                        HttpStatus.FORBIDDEN, "KV_ACCESS_FORBIDDEN", "Access token does not match this key."));
              }
              long mod = kvp.getModRevision();
              long oldLease = kvp.getLease();
              KvTicketRecord neu = new KvTicketRecord(old.accessToken(), newValue);
              byte[] bytes = json.writeBytes(neu);

              if (ttlSeconds == null) {
                PutOption po =
                    oldLease > 0
                        ? PutOption.newBuilder().withLeaseId(oldLease).build()
                        : PutOption.DEFAULT;
                return commitCas(etcdKey, mod, bytes, po)
                    .thenApply(
                        txr -> {
                          if (!txr.isSucceeded()) {
                            throw new ResourceConflictException(
                                "KV_REVISION_CONFLICT",
                                "Key was modified concurrently; re-read and retry.",
                                Map.of("keyId", keyId));
                          }
                          long outLease = oldLease > 0 ? oldLease : 0L;
                          return new PutResponse(false, keyId, null, outLease, txr.getHeader().getRevision());
                        });
              }

              return lease.grant(ttlSeconds)
                  .thenCompose(
                      gr -> {
                        long newLease = gr.getID();
                        PutOption po = PutOption.newBuilder().withLeaseId(newLease).build();
                        return commitCas(etcdKey, mod, bytes, po)
                            .thenCompose(
                                txr -> {
                                  if (!txr.isSucceeded()) {
                                    return lease.revoke(newLease)
                                        .thenApply(
                                            v -> {
                                              throw new ResourceConflictException(
                                                  "KV_REVISION_CONFLICT",
                                                  "Key was modified concurrently; re-read and retry.",
                                                  Map.of("keyId", keyId));
                                            });
                                  }
                                  if (oldLease > 0) {
                                    return lease
                                        .revoke(oldLease)
                                        .thenApply(
                                            v ->
                                                new PutResponse(
                                                    false, keyId, null, newLease, txr.getHeader().getRevision()));
                                  }
                                  return CompletableFuture.completedFuture(
                                      new PutResponse(false, keyId, null, newLease, txr.getHeader().getRevision()));
                                });
                      });
            });
  }

  private CompletableFuture<TxnResponse> commitCas(
      String etcdKey, long expectedModRevision, byte[] bytes, PutOption putOption
  ) {
    Txn txn =
        kv.txn()
            .If(new Cmp(bs(etcdKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedModRevision)))
            .Then(Op.put(bs(etcdKey), bb(bytes), putOption))
            .Else(Op.get(bs(etcdKey), GetOption.DEFAULT));
    return txn.commit();
  }

  private static void validateKvTtl(Integer ttlSeconds) {
    if (ttlSeconds == null) {
      return;
    }
    try {
      LeaseTtlBounds.requireValid(ttlSeconds);
    } catch (IllegalArgumentException e) {
      throw new ParticipantException(HttpStatus.BAD_REQUEST, "KV_TTL_INVALID", e.getMessage());
    }
  }

  private static String newAccessToken() {
    byte[] b = new byte[24];
    RNG.nextBytes(b);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  /** {@code created true} on first write — {@code accessToken} is non-null only then. */
  public record PutResponse(boolean created, String keyId, String accessToken, long leaseId, long modRevision) {}

  public record GetResponse(JsonNode value, long leaseId, long modRevision) {}
}
