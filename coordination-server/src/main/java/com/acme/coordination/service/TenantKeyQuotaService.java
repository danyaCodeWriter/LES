package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.etcd.Keyspace;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.GetOption;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Лимит числа ключей coordination на tenant (подсчёт под префиксами {@code .../regions/} и {@code .../global/};
 * ключи {@code .../idempotency/} не входят в квоту).
 */
@Service
public class TenantKeyQuotaService {

  private final KV kv;
  private final Keyspace keyspace;
  private final CoordinationProperties props;

  public TenantKeyQuotaService(Client client, Keyspace keyspace, CoordinationProperties props) {
    this.kv = client.getKVClient();
    this.keyspace = keyspace;
    this.props = props;
  }

  /** Суммарное число ключей coordination для tenant (regions + global). */
  public CompletableFuture<Long> countCoordinationKeys(String tenantId) {
    GetOption countOnly = GetOption.newBuilder().isPrefix(true).withCountOnly(true).build();
    CompletableFuture<Long> r =
        kv.get(bs(keyspace.regionsPrefix(tenantId)), countOnly).thenApply(io.etcd.jetcd.kv.GetResponse::getCount);
    CompletableFuture<Long> g =
        kv.get(bs(keyspace.globalSubtreePrefix(tenantId)), countOnly).thenApply(io.etcd.jetcd.kv.GetResponse::getCount);
    return r.thenCombine(g, Long::sum);
  }

  /**
   * Перед созданием {@code keysToAdd} новых ключей: если {@code coordination.limits.max-keys-per-tenant} &gt; 0 и
   * текущее число + {@code keysToAdd} превышает лимит — {@link ParticipantException} с **429**.
   */
  public CompletableFuture<Void> assertHeadroom(String tenantId, int keysToAdd) {
    int limit = props.limits().maxKeysPerTenant();
    if (limit <= 0 || keysToAdd <= 0) {
      return CompletableFuture.completedFuture(null);
    }
    return countCoordinationKeys(tenantId)
        .thenCompose(
            current -> {
              if (current + keysToAdd > limit) {
                return CompletableFuture.failedFuture(
                    new ParticipantException(
                        HttpStatus.TOO_MANY_REQUESTS,
                        "TENANT_KEY_LIMIT_EXCEEDED",
                        "Coordination key count for tenant is "
                            + current
                            + "; adding "
                            + keysToAdd
                            + " would exceed limit "
                            + limit
                            + " (coordination.limits.max-keys-per-tenant)."));
              }
              return CompletableFuture.completedFuture(null);
            });
  }
}
