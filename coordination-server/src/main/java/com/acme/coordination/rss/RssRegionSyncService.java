package com.acme.coordination.rss;

import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.deployment.DeploymentRole;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.region.InstanceHomeRegion;
import com.acme.coordination.service.ActiveRegionService;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Syncs this home's active-region etcd key with RSS when {@code coordination.rss.required=true}.
 *
 * <ul>
 *   <li>No yaml starters → block until first RSS {@code state} (fail startup if unavailable).
 *   <li>Yaml starters ({@code active-region.home} set) → seed etcd from yaml, start, wait for RSS in
 *       background.
 *   <li>{@code rss.required=false} → yaml only (see {@link #applyYamlBootstrapToEtcd()}).
 * </ul>
 *
 * <p>After each RSS write: refresh {@link ActiveRegionMonitor} so deployment gates (stand_in/switch →
 * no regional mutations) and SSE {@code stream/active-region} update immediately.
 */
@Component
public class RssRegionSyncService {

  private static final Logger log = LoggerFactory.getLogger(RssRegionSyncService.class);

  private final CoordinationProperties props;
  private final RssClient rssClient;
  private final ActiveRegionService activeRegionService;
  private final InstanceHomeRegion instanceHome;
  private final ObjectProvider<ActiveRegionMonitor> activeRegionMonitor;
  private final AtomicReference<Disposable> stream = new AtomicReference<>();

  public RssRegionSyncService(
      CoordinationProperties props,
      RssClient rssClient,
      ActiveRegionService activeRegionService,
      InstanceHomeRegion instanceHome,
      ObjectProvider<ActiveRegionMonitor> activeRegionMonitor) {
    this.props = props;
    this.rssClient = rssClient;
    this.activeRegionService = activeRegionService;
    this.instanceHome = instanceHome;
    this.activeRegionMonitor = activeRegionMonitor;
  }

  public boolean rssRequired() {
    return props.rss().required();
  }

  /**
   * Blocking bootstrap from RSS (first {@code state}). Used when RSS is required and yaml has no
   * starters — process must not become operational without these values.
   */
  public RssMappedState awaitInitialState() {
    CoordinationProperties.Rss rss = props.rss();
    if (!rss.isConfigured()) {
      throw new IllegalStateException(
          "coordination.rss.required=true but base-url / recovery-group are not configured");
    }
    String seed = instanceHome.configuredSeed();
    if (seed.isBlank()) {
      throw new IllegalStateException(
          "coordination.rss.required=true without yaml starters needs a query seed for the first "
              + "SSE ?region= (set coordination.rss.home-region); after connect, home/activeRegion/"
              + "status come from RSS state. Or set coordination.active-region.home to start on yaml "
              + "and wait for RSS in background.");
    }
    Duration timeout = rss.connectTimeout();
    log.info(
        "RSS required (no yaml starters): waiting up to {} for initial state from {} (rg={}, queryHome={})",
        timeout,
        rss.baseUrl(),
        rss.recoveryGroup(),
        seed);
    try {
      RssMappedState first = rssClient.firstState().block(timeout);
      if (first == null) {
        throw new IllegalStateException("RSS returned no state event within " + timeout);
      }
      applyToEtcd(first, "rss-bootstrap");
      return first;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to bootstrap from RSS (required=true, no yaml starters): " + e.getMessage(), e);
    }
  }

  /** Keep applying live RSS state events to etcd (non-blocking). */
  public void startBackgroundSync() {
    if (!props.rss().required()) {
      return;
    }
    Disposable prev = stream.getAndSet(null);
    if (prev != null) {
      prev.dispose();
    }
    Disposable d =
        rssClient
            .stateUpdates()
            .subscribe(
                s -> {
                  try {
                    applyToEtcd(s, "rss");
                  } catch (Exception e) {
                    log.warn("failed to apply RSS state to etcd: {}", e.toString());
                  }
                },
                err -> log.error("RSS background sync terminated: {}", err.toString()));
    stream.set(d);
  }

  /**
   * Write yaml {@code active-region.*} into etcd (defaults: value=r1, status=ACTIVE; home via
   * {@link InstanceHomeRegion}).
   */
  public void applyYamlBootstrapToEtcd() {
    String region = props.activeRegion().bootstrapRegionId();
    DeploymentRole status = props.activeRegion().bootstrapStatus();
    activeRegionService.set(region, status.name()).join();
    log.info(
        "seeded home={} active-region from yaml: activeRegion={} status={} (rss.required={})",
        instanceHome.get(),
        region,
        status,
        props.rss().required());
  }

  private void applyToEtcd(RssMappedState mapped, String source) {
    if (mapped.homeRegion() != null && !mapped.homeRegion().isBlank()) {
      instanceHome.applyFromRss(mapped.homeRegion());
    }
    activeRegionService.set(mapped.regionId(), mapped.status().name()).join();
    // Same as SRE PUT: do not wait for etcd watch — gates + SSE must see stand_in/switch now.
    ActiveRegionMonitor monitor = activeRegionMonitor.getIfAvailable();
    if (monitor != null) {
      monitor.tick();
    }
    log.info(
        "applied active-region from {}: home={} activeRegion={} status={} revision={}",
        source,
        instanceHome.get(),
        mapped.regionId(),
        mapped.status(),
        mapped.revision());
  }

  @PreDestroy
  void stop() {
    Disposable d = stream.getAndSet(null);
    if (d != null) {
      d.dispose();
    }
  }
}
