package com.acme.coordination.service;

import static com.acme.coordination.etcd.EtcdBytes.bb;
import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.deployment.DeploymentRole;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.region.ActiveRegionState;
import com.acme.coordination.region.InstanceHomeRegion;
import com.acme.coordination.security.SreTokenVerifier;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Active region + POV status in etcd for <strong>this</strong> LES home region.
 * Key: {@code /coord/v1/cluster/regions/{home}/activeRegion}.
 */
@Service
public class ActiveRegionService {

  private final KV kv;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final SreTokenVerifier sre;
  private final InstanceHomeRegion instanceHome;

  public ActiveRegionService(
      Client client,
      Keyspace keyspace,
      JsonCodec json,
      SreTokenVerifier sre,
      InstanceHomeRegion instanceHome) {
    this.kv = client.getKVClient();
    this.keyspace = keyspace;
    this.json = json;
    this.sre = sre;
    this.instanceHome = instanceHome;
  }

  private String homeKey() {
    return keyspace.clusterActiveRegionKey(instanceHome.get());
  }

  public CompletableFuture<Optional<ActiveRegionState>> get() {
    String key = homeKey();
    return kv.get(bs(key))
        .thenApply(
            (GetResponse resp) -> {
              if (resp.getKvs().isEmpty()) {
                return Optional.empty();
              }
              byte[] raw = resp.getKvs().get(0).getValue().getBytes();
              return Optional.of(normalizeRead(json.read(raw, ActiveRegionState.class)));
            });
  }

  /**
   * Persist activeRegion id and/or POV status for this LES home key.
   * Null/blank keeps previous (or defaults).
   */
  public CompletableFuture<ActiveRegionState> set(String activeRegionId, String status) {
    String key = homeKey();
    return get()
        .thenCompose(
            opt -> {
              try {
                String rid =
                    (activeRegionId != null && !activeRegionId.isBlank())
                        ? normalizeRegionId(activeRegionId)
                        : opt.map(ActiveRegionState::regionId)
                            .filter(s -> s != null && !s.isBlank())
                            .orElse("r1");
                DeploymentRole role;
                if (status != null && !status.isBlank()) {
                  role = DeploymentRole.parse(status);
                } else {
                  role = opt.map(ActiveRegionState::deploymentRole).orElse(DeploymentRole.ACTIVE);
                }
                ActiveRegionState state = ActiveRegionState.of(rid, role);
                return kv.put(bs(key), bb(json.writeBytes(state))).thenApply(ignored -> state);
              } catch (IllegalArgumentException | ParticipantException e) {
                if (e instanceof ParticipantException pe) {
                  return CompletableFuture.failedFuture(pe);
                }
                return CompletableFuture.failedFuture(
                    new ParticipantException(
                        HttpStatus.BAD_REQUEST, "ACTIVE_REGION_STATUS_INVALID", e.getMessage()));
              }
            });
  }

  public CompletableFuture<ActiveRegionState> setWithSre(
      String activeRegionId, String status, String sreCredential) {
    try {
      sre.assertSre(sreCredential);
    } catch (ParticipantException e) {
      return CompletableFuture.failedFuture(e);
    }
    return set(activeRegionId, status);
  }

  public static String normalizeRegionId(String regionId) {
    if (regionId == null || regionId.isBlank()) {
      throw new ParticipantException(
          HttpStatus.BAD_REQUEST, "ACTIVE_REGION_REQUIRED", "region must be a non-empty string.");
    }
    String v = regionId.trim();
    if (v.contains("/") || v.contains("\u0000")) {
      throw new ParticipantException(
          HttpStatus.BAD_REQUEST, "ACTIVE_REGION_INVALID", "region must not contain '/' or NUL.");
    }
    return v;
  }

  private static ActiveRegionState normalizeRead(ActiveRegionState s) {
    if (s == null) {
      return ActiveRegionState.of("r1", DeploymentRole.ACTIVE);
    }
    String rid = s.regionId() == null || s.regionId().isBlank() ? "r1" : s.regionId().trim();
    DeploymentRole role = s.deploymentRole();
    return new ActiveRegionState(rid, role.name(), s.updatedAtUnixMillis());
  }
}
