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
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * After logical active region id changes, regional mutations stay disabled until old segment is empty
 * (except coordinationEpoch). With cluster status {@code SWITCH}, lease keys are purged on each poll.
 */
@Component
public class RegionSwitchDrainPolicy {

  private static final Logger log = LoggerFactory.getLogger(RegionSwitchDrainPolicy.class);

  private final KV kv;
  private final Keyspace keyspace;
  private final DeploymentRegistrationPolicy deployment;
  private final ActiveRegionMonitor activeRegionMonitor;
  private final AtomicReference<String> drainingRegionId = new AtomicReference<>(null);
  private final AtomicBoolean regionalWritesAllowed = new AtomicBoolean(true);

  public RegionSwitchDrainPolicy(
      Client client,
      Keyspace keyspace,
      DeploymentRegistrationPolicy deployment,
      @Lazy ActiveRegionMonitor activeRegionMonitor) {
    this.kv = client.getKVClient();
    this.keyspace = keyspace;
    this.deployment = deployment;
    this.activeRegionMonitor = activeRegionMonitor;
  }

  public void onActiveRegionIdChanged(String previousRegionId, String nextRegionId) {
    if (previousRegionId == null || nextRegionId == null || previousRegionId.equals(nextRegionId)) {
      return;
    }
    drainingRegionId.set(previousRegionId);
    regionalWritesAllowed.set(false);
    log.info(
        "active region switch {} -> {}; regional mutations blocked until previous segment is empty of leader/workers/jobs",
        previousRegionId,
        nextRegionId);
    refreshDrainedStateBlocking();
  }

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
      boolean wasBlocked = !regionalWritesAllowed.getAndSet(true);
      if (wasBlocked) {
        activeRegionMonitor.emitDeploymentSnapshotForSse();
      }
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
        activeRegionMonitor.emitDeploymentSnapshotForSse();
      } else {
        regionalWritesAllowed.set(false);
      }
    } catch (Exception e) {
      log.warn("etcd drain check failed for region {}", region, e);
      regionalWritesAllowed.set(false);
    }
  }

  private void purgeDrainingLeaseKeysBlocking(String previousRegionId) throws Exception {
    String needle = "/regions/" + previousRegionId + "/";
    var resp =
        kv.get(bs(keyspace.allTenantsScanPrefix()), GetOption.newBuilder().isPrefix(true).build())
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
    var resp =
        kv.get(bs(keyspace.allTenantsScanPrefix()), GetOption.newBuilder().isPrefix(true).build())
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
