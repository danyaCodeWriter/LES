package com.acme.coordination.idempotency;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.etcd.Keyspace;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class IdempotencyService {

  private static final int MIN_TTL_SEC = 60;
  private static final int MAX_TTL_SEC = 604_800; // 7d

  private final KV kv;
  private final Lease lease;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final CoordinationProperties.Idempotency cfg;

  /** Spring bean: несколько конструкторов — явно указываем, какой использовать для DI. */
  @Autowired
  public IdempotencyService(
      Client client,
      Keyspace keyspace,
      JsonCodec json,
      CoordinationProperties props
  ) {
    this(client, keyspace, json, props.idempotency());
  }

  /** Для тестов и явной конфигурации без полного {@link CoordinationProperties}. */
  public IdempotencyService(
      Client client,
      Keyspace keyspace,
      JsonCodec json,
      CoordinationProperties.Idempotency idempotencyConfig
  ) {
    this.kv = client.getKVClient();
    this.lease = client.getLeaseClient();
    this.keyspace = keyspace;
    this.json = json;
    this.cfg = idempotencyConfig;
  }

  public CoordinationProperties.Idempotency config() {
    return cfg;
  }

  public static String fingerprint(
      String method,
      String path,
      String idempotencyHeader,
      byte[] body,
      String kvAccessTokenOrEmpty
  ) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(method.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) '\n');
      sha.update(path.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) '\n');
      sha.update(idempotencyHeader.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) '\n');
      sha.update(body);
      sha.update((byte) '\n');
      sha.update(kvAccessTokenOrEmpty.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(sha.digest());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public CompletableFuture<Optional<IdempotencyPayload>> get(String tenantId, String fingerprintHex) {
    String key = keyspace.idempotencyKey(tenantId, fingerprintHex);
    return kv.get(bs(key))
        .thenApply(
            resp -> {
              if (resp.getKvs().isEmpty()) {
                return Optional.empty();
              }
              byte[] val = resp.getKvs().getFirst().getValue().getBytes();
              return Optional.of(json.read(val, IdempotencyPayload.class));
            });
  }

  public CompletableFuture<Void> store(String tenantId, String fingerprintHex, int httpStatus, byte[] bodyUtf8) {
    int ttl = Math.clamp(cfg.ttlSeconds(), MIN_TTL_SEC, MAX_TTL_SEC);
    IdempotencyPayload payload = IdempotencyPayload.of(httpStatus, bodyUtf8);
    byte[] bytes = json.writeBytes(payload);
    String key = keyspace.idempotencyKey(tenantId, fingerprintHex);
    return lease.grant(ttl)
        .thenCompose(
            g -> {
              long lid = g.getID();
              return kv.put(bs(key), bb(bytes), PutOption.newBuilder().withLeaseId(lid).build());
            })
        .thenApply(r -> null);
  }
}
