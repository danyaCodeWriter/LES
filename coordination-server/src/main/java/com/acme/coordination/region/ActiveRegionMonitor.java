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
 * Per-home POV of active region + status from etcd key
 * {@code /coord/v1/cluster/regions/{home}/activeRegion}.
 *
 * <p>Bootstrap:
 * <ul>
 *   <li>{@code rss.required=false} — yaml {@code active-region.*} (defaults r1/r1/ACTIVE).
 *   <li>{@code rss.required=true} without yaml starters — block on first RSS {@code state}.
 *   <li>{@code rss.required=true} with yaml starters ({@code home} set) — yaml then background RSS.
 * </ul>
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
  private final InstanceHomeRegion instanceHome;
  private final AtomicReference<ActiveRegionSnapshot> current =
      new AtomicReference<>(new ActiveRegionSnapshot("r1", DeploymentRole.ACTIVE, "initial"));
  private final Sinks.Many<String> changes = Sinks.many().multicast().onBackpressureBuffer();
  private volatile Disposable watchDisposable;
  private volatile String watchedHome;

  public ActiveRegionMonitor(
      ActiveRegionService activeRegionService,
      CoordinationProperties props,
      RssRegionSyncService rssSync,
      EtcdWatchService watch,
      Keyspace keyspace,
      JsonCodec json,
      ObjectMapper objectMapper,
      RegionSwitchDrainPolicy drainPolicy,
      InstanceHomeRegion instanceHome
  ) {
    this.activeRegionService = activeRegionService;
    this.props = props;
    this.rssSync = rssSync;
    this.watch = watch;
    this.keyspace = keyspace;
    this.json = json;
    this.objectMapper = objectMapper;
    this.drainPolicy = drainPolicy;
    this.instanceHome = instanceHome;
  }

  @PostConstruct
  void start() {
    seedBootstrapIntoEtcd();
    refreshFromEtcd();
    ensureWatchForCurrentHome();
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
    watchDisposable = null;
    watchedHome = null;
  }

  /** Rebind etcd watch when RSS changes LES home (per-home key). */
  @org.springframework.context.event.EventListener
  public void onHomeRegionChanged(HomeRegionChangedEvent event) {
    log.info(
        "home region event: {} -> {}; rebinding etcd watch to per-home key",
        event.previousHome(),
        event.home());
    ensureWatchForCurrentHome();
    refreshFromEtcd();
  }

  public String activeRegionId() {
    return current.get().regionId();
  }

  /** Home region of this LES JVM (RSS {@code state.region} or yaml seed). */
  public String instanceHomeRegion() {
    return instanceHome.get();
  }

  public ActiveRegionSnapshot snapshot() {
    return current.get();
  }

  /**
   * Role of this LES for gates: status from <strong>this home's</strong> etcd key
   * (RSS POV for {@link InstanceHomeRegion#get()}).
   */
  public DeploymentRole effectiveRole() {
    return current.get().status();
  }

  public Flux<String> activeRegionEvents() {
    return changes.asFlux().startWith(serialize(snapshot()));
  }

  @Scheduled(fixedDelayString = "${coordination.active-region.poll-interval-ms:5000}")
  public void tick() {
    ensureWatchForCurrentHome();
    refreshFromEtcd();
  }

  private synchronized void ensureWatchForCurrentHome() {
    String home;
    try {
      home = instanceHome.get();
    } catch (IllegalStateException e) {
      log.warn("skip active-region watch: {}", e.getMessage());
      return;
    }
    if (home.equals(watchedHome) && watchDisposable != null && !watchDisposable.isDisposed()) {
      return;
    }
    Disposable prev = watchDisposable;
    if (prev != null) {
      prev.dispose();
    }
    String key = keyspace.clusterActiveRegionKey(home);
    watchedHome = home;
    watchDisposable =
        watch
            .watchKey(key)
            .subscribe(this::onWatchEvent, e -> log.warn("active-region etcd watch failed key={}", key, e));
    log.info("watching per-home active-region key={}", key);
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
        if (props.hasYamlActiveRegionStarters()) {
          // Starters in yaml: operate on them and wait for RSS in background.
          rssSync.applyYamlBootstrapToEtcd();
          log.info(
              "RSS required but yaml starters present (home={}): started on yaml, waiting for RSS",
              instanceHome.get());
        } else {
          // No starters: first home/activeRegion/status must come from RSS or we do not start.
          rssSync.awaitInitialState();
        }
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
      m.put("region", instanceHome.get());
      m.put("activeRegion", s.regionId());
      m.put("status", effectiveRole().wire());
      m.put("source", s.source());
      m.put("regionalWritesBlocked", drainPolicy.regionalWritesBlocked());
      m.put("drainingPreviousRegion", drainPolicy.drainingPreviousRegionId().orElse(null));
      return objectMapper.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
