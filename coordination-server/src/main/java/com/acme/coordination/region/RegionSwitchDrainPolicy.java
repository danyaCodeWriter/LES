package com.acme.coordination.region;

import static com.acme.coordination.etcd.EtcdBytes.bs;

import com.acme.coordination.deployment.DeploymentRegistrationPolicy;
import com.acme.coordination.etcd.Keyspace;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.GetOption;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * After the logical active region id changes (e.g. r1→r2), regional coordinator/worker/job mutations
 * stay disabled until etcd no longer has lease-backed keys under {@code .../regions/{oldId}/} except
 * {@code coordinationEpoch} (durable, no lease). With {@link com.acme.coordination.deployment.DeploymentRole#SWITCH},
 * the same keys are deleted proactively on each poll (operator must ensure old holders are stopped).
 */
@Component
public class RegionSwitchDrainPolicy {

  private static final Logger log = LoggerFactory.getLogger(RegionSwitchDrainPolicy.class);

  private final KV kv;
  private final Keyspace keyspace;
  private final DeploymentRegistrationPolicy deployment;
  private final AtomicReference<String> drainingRegionId = new AtomicReference<>(null);
  private final AtomicBoolean regionalWritesAllowed = new AtomicBoolean(true);

  public RegionSwitchDrainPolicy(Client client, Keyspace keyspace, DeploymentRegistrationPolicy deployment) {
    this.kv = client.getKVClient();
    this.keyspace = keyspace;
    this.deployment = deployment;
  }

  /**
   * Call when resolved active region id changes. Starts draining {@code previousRegionId} until
   * leader/workers/jobs keys are gone.
   */
  public void onActiveRegionIdChanged(String previousRegionId, String nextRegionId) {
    if (previousRegionId == null || nextRegionId == null || previousRegionId.equals(nextRegionId)) {
      return;
    }
    drainingRegionId.set(previousRegionId);
    regionalWritesAllowed.set(false);
    log.info("active region switch {} -> {}; regional mutations blocked until previous segment is empty of leader/workers/jobs",
        previousRegionId, nextRegionId);
    refreshDrainedStateBlocking();
  }

  /**
   * ACTIVE/SWITCH: false while previous region segment still has leader/workers/jobs keys.
   * STANDBY: always true (standby blocks regional separately).
   */
  public boolean regionalWritesAllowed() {
    if (!deployment.participatesInRegionalDrain()) {
      return true;
    }
    return regionalWritesAllowed.get();
  }

  public Optional<String> drainingPreviousRegionId() {
    return Optional.ofNullable(drainingRegionId.get());
  }

  public boolean regionalWritesBlocked() {
    return deployment.participatesInRegionalDrain() && !regionalWritesAllowed.get();
  }

  @Scheduled(fixedDelayString = "${coordination.region-drain.poll-ms:2000}")
  public void scheduledPoll() {
    if (drainingRegionId.get() != null) {
      refreshDrainedStateBlocking();
    }
  }

  private void refreshDrainedStateBlocking() {
    String region = drainingRegionId.get();
    if (region == null) {
      regionalWritesAllowed.set(true);
      return;
    }
    try {
      if (deployment.isSwitch()) {
        purgeDrainingLeaseKeysBlocking(region);
      }
      boolean drained = etcdPreviousRegionDrained(region);
      if (drained) {
        drainingRegionId.set(null);
        regionalWritesAllowed.set(true);
        log.info("region {} drained (no leader/workers/jobs keys) — regional writes enabled", region);
      } else {
        regionalWritesAllowed.set(false);
      }
    } catch (Exception e) {
      log.warn("etcd drain check failed for region {}", region, e);
      regionalWritesAllowed.set(false);
    }
  }

  /**
   * При {@link com.acme.coordination.deployment.DeploymentRole#SWITCH} удаляем lease-ключи старого сегмента
   * (leader, workers, jobs), не трогая {@code coordinationEpoch}. Безопасно только если процессы, державшие
   * lease, уже остановлены — иначе возможна неконсистентность у клиентов.
   */
  private void purgeDrainingLeaseKeysBlocking(String previousRegionId) throws Exception {
    String needle = "/regions/" + previousRegionId + "/";
    var resp = kv.get(bs(keyspace.allTenantsScanPrefix()), GetOption.newBuilder().isPrefix(true).build())
        .get(5, TimeUnit.SECONDS);
    int deleted = 0;
    for (var kvp : resp.getKvs()) {
      String k = kvp.getKey().toString(StandardCharsets.UTF_8);
      if (!k.contains(needle)) {
        continue;
      }
      if (k.endsWith("/coordinationEpoch")) {
        continue;
      }
      kv.delete(bs(k)).get(5, TimeUnit.SECONDS);
      deleted++;
    }
    if (deleted > 0) {
      log.info("SWITCH purge: deleted {} keys under draining region {}", deleted, previousRegionId);
    }
  }

  private boolean etcdPreviousRegionDrained(String previousRegionId) throws Exception {
    String needle = "/regions/" + previousRegionId + "/";
    var resp = kv.get(bs(keyspace.allTenantsScanPrefix()), GetOption.newBuilder().isPrefix(true).build())
        .get(5, TimeUnit.SECONDS);
    for (var kvp : resp.getKvs()) {
      String k = kvp.getKey().toString(StandardCharsets.UTF_8);
      if (!k.contains(needle)) {
        continue;
      }
      if (k.endsWith("/coordinationEpoch")) {
        continue;
      }
      return false;
    }
    return true;
  }
}
