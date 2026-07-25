package com.acme.coordination.rss;

import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.deployment.DeploymentRole;
import com.acme.coordination.service.ActiveRegionService;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Bootstraps and keeps cluster active-region (etcd) in sync with RSS when
 * {@code coordination.rss.required=true}. When required=false, yaml
 * {@code coordination.active-region.*} is used by {@link com.acme.coordination.region.ActiveRegionMonitor}.
 */
@Component
public class RssRegionSyncService {

  private static final Logger log = LoggerFactory.getLogger(RssRegionSyncService.class);

  private final CoordinationProperties props;
  private final RssClient rssClient;
  private final ActiveRegionService activeRegionService;
  private final AtomicReference<Disposable> stream = new AtomicReference<>();

  public RssRegionSyncService(
      CoordinationProperties props, RssClient rssClient, ActiveRegionService activeRegionService) {
    this.props = props;
    this.rssClient = rssClient;
    this.activeRegionService = activeRegionService;
  }

  public boolean rssRequired() {
    return props.rss().required();
  }

  /**
   * Blocking bootstrap from RSS (first {@code state} event). Throws if RSS is required and unavailable.
   */
  public RssMappedState awaitInitialState() {
    CoordinationProperties.Rss rss = props.rss();
    if (!rss.isConfigured()) {
      throw new IllegalStateException(
          "coordination.rss.required=true but base-url / recovery-group are not configured");
    }
    Duration timeout = rss.connectTimeout();
    log.info(
        "RSS required: waiting up to {} for initial state from {} (rg={}, homeRegion={})",
        timeout,
        rss.baseUrl(),
        rss.recoveryGroup(),
        rss.homeRegion());
    try {
      RssMappedState first = rssClient.firstState().block(timeout);
      if (first == null) {
        throw new IllegalStateException("RSS returned no state event within " + timeout);
      }
      applyToEtcd(first, "rss-bootstrap");
      return first;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to bootstrap active-region from RSS (coordination.rss.required=true): " + e.getMessage(),
          e);
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

  public void applyYamlBootstrapToEtcd() {
    String region = props.activeRegion().bootstrapRegionId();
    DeploymentRole status = props.activeRegion().bootstrapStatus();
    activeRegionService.set(region, status.name()).join();
    log.info(
        "seeded cluster active-region from yaml: region={} status={} (rss.required=false)",
        region,
        status);
  }

  private void applyToEtcd(RssMappedState mapped, String source) {
    activeRegionService.set(mapped.regionId(), mapped.status().name()).join();
    log.info(
        "applied active-region from {}: region={} status={} revision={}",
        source,
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
