package com.acme.coordination.region;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.deployment.DeploymentRole;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.rss.RssRegionSyncService;
import com.acme.coordination.service.ActiveRegionService;
import com.acme.coordination.watch.EtcdWatchService;
import com.acme.coordination.watch.WatchEventDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Logical region id + deployment status (ACTIVE/STANDBY/SWITCH) from cluster etcd key.
 * Bootstrap: RSS (when {@code coordination.rss.required=true}) or yaml {@code coordination.active-region.*}.
 */
@Component
public class ActiveRegionMonitor {

  private static final Logger log = LoggerFactory.getLogger(ActiveRegionMonitor.class);

  private final ActiveRegionService activeRegionService;
  private final CoordinationProperties props;
  private final RssRegionSyncService rssSync;
  private final EtcdWatchService watch;
  private final Keyspace keyspace;
  private final JsonCodec json;
  private final ObjectMapper objectMapper;
  private final RegionSwitchDrainPolicy drainPolicy;
  private final AtomicReference<ActiveRegionSnapshot> current =
      new AtomicReference<>(new ActiveRegionSnapshot("r1", DeploymentRole.ACTIVE, "initial"));
  private final Sinks.Many<String> changes = Sinks.many().multicast().onBackpressureBuffer();
  private volatile Disposable watchDisposable;

  public ActiveRegionMonitor(
      ActiveRegionService activeRegionService,
      CoordinationProperties props,
      RssRegionSyncService rssSync,
      EtcdWatchService watch,
      Keyspace keyspace,
      JsonCodec json,
      ObjectMapper objectMapper,
      RegionSwitchDrainPolicy drainPolicy
  ) {
    this.activeRegionService = activeRegionService;
    this.props = props;
    this.rssSync = rssSync;
    this.watch = watch;
    this.keyspace = keyspace;
    this.json = json;
    this.objectMapper = objectMapper;
    this.drainPolicy = drainPolicy;
  }

  @PostConstruct
  void start() {
    seedBootstrapIntoEtcd();
    refreshFromEtcd();
    watchDisposable =
        watch
            .watchKey(keyspace.clusterActiveRegionKey())
            .subscribe(this::onWatchEvent, e -> log.warn("active-region etcd watch failed", e));
    if (rssSync.rssRequired()) {
      rssSync.startBackgroundSync();
    }
  }

  @PreDestroy
  void stop() {
    Disposable d = watchDisposable;
    if (d != null) {
      d.dispose();
    }
  }

  public String activeRegionId() {
    return current.get().regionId();
  }

  public ActiveRegionSnapshot snapshot() {
    return current.get();
  }

  public Flux<String> activeRegionEvents() {
    return changes.asFlux().startWith(serialize(snapshot()));
  }

  @Scheduled(fixedDelayString = "${coordination.active-region.poll-interval-ms:5000}")
  public void tick() {
    refreshFromEtcd();
  }

  public void emitDeploymentSnapshotForSse() {
    try {
      changes.tryEmitNext(serialize(snapshot()));
    } catch (Exception e) {
      log.warn("emit deployment snapshot for SSE failed", e);
    }
  }

  private void seedBootstrapIntoEtcd() {
    try {
      if (rssSync.rssRequired()) {
        rssSync.awaitInitialState();
      } else {
        rssSync.applyYamlBootstrapToEtcd();
      }
    } catch (RuntimeException e) {
      if (rssSync.rssRequired()) {
        throw e;
      }
      log.error("failed to seed active region into etcd; continuing with local fallback", e);
    }
  }

  private void onWatchEvent(WatchEventDto ev) {
    if (ev == null) {
      return;
    }
    String type = ev.type() == null ? "" : ev.type();
    if ("DELETE".equalsIgnoreCase(type)) {
      applySnapshot(bootstrapSnapshot("etcd-missing"));
      return;
    }
    String valueJson = ev.valueJson();
    if (valueJson == null || valueJson.isBlank()) {
      refreshFromEtcd();
      return;
    }
    try {
      ActiveRegionState state =
          json.read(valueJson.getBytes(java.nio.charset.StandardCharsets.UTF_8), ActiveRegionState.class);
      applySnapshot(
          new ActiveRegionSnapshot(normalizeRegionId(state.regionId()), state.deploymentRole(), "etcd"));
    } catch (Exception e) {
      log.warn("active-region watch payload parse failed; falling back to get", e);
      refreshFromEtcd();
    }
  }

  private void refreshFromEtcd() {
    try {
      var opt = activeRegionService.get().join();
      if (opt.isEmpty()) {
        applySnapshot(bootstrapSnapshot("etcd-missing"));
      } else {
        ActiveRegionState st = opt.get();
        applySnapshot(new ActiveRegionSnapshot(normalizeRegionId(st.regionId()), st.deploymentRole(), "etcd"));
      }
    } catch (Exception e) {
      log.warn("active-region etcd get failed; keeping {}", current.get().regionId(), e);
    }
  }

  private ActiveRegionSnapshot bootstrapSnapshot(String source) {
    return new ActiveRegionSnapshot(
        normalizeRegionId(props.activeRegion().bootstrapRegionId()),
        props.activeRegion().bootstrapStatus(),
        source);
  }

  private void applySnapshot(ActiveRegionSnapshot next) {
    ActiveRegionSnapshot prev = current.get();
    if (prev.regionId().equals(next.regionId())
        && prev.status() == next.status()
        && prev.source().equals(next.source())) {
      return;
    }
    if (!prev.regionId().equals(next.regionId())) {
      drainPolicy.onActiveRegionIdChanged(prev.regionId(), next.regionId());
    }
    current.set(next);
    try {
      changes.tryEmitNext(serialize(next));
    } catch (Exception e) {
      log.warn("emit active-region event failed", e);
    }
    log.info(
        "active region updated: region={} status={} (source={})",
        next.regionId(),
        next.status(),
        next.source());
  }

  private static String normalizeRegionId(String regionId) {
    if (regionId == null || regionId.isBlank()) {
      return "r1";
    }
    return regionId.trim();
  }

  private String serialize(ActiveRegionSnapshot s) {
    try {
      java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
      m.put("region", s.regionId());
      m.put("status", s.status().wire());
      m.put("activeRegion", s.regionId());
      m.put("source", s.source());
      m.put("regionalWritesBlocked", drainPolicy.regionalWritesBlocked());
      m.put("drainingPreviousRegion", drainPolicy.drainingPreviousRegionId().orElse(null));
      return objectMapper.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
