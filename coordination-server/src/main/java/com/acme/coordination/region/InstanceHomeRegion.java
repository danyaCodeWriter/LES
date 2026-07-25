package com.acme.coordination.region;

import com.acme.coordination.config.CoordinationProperties;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Home region of this LES JVM.
 *
 * <ul>
 *   <li>{@code rss.required=false}: yaml {@code active-region.home}, else default {@code r1}.
 *   <li>{@code rss.required=true} + yaml starters ({@code active-region.home} set): yaml until RSS
 *       {@code state.region} arrives.
 *   <li>{@code rss.required=true} without yaml starters: only RSS {@code state.region}; until then
 *       the process must not become operational (bootstrap blocks / fails). Query seed for the first
 *       {@code ?region=} may be {@code rss.home-region}.
 * </ul>
 */
@Component
public class InstanceHomeRegion {

  private static final Logger log = LoggerFactory.getLogger(InstanceHomeRegion.class);
  private static final String DEFAULT_HOME = "r1";

  private final CoordinationProperties props;
  private final ApplicationEventPublisher events;
  private final AtomicReference<String> fromRss = new AtomicReference<>();

  public InstanceHomeRegion(CoordinationProperties props, ApplicationEventPublisher events) {
    this.props = props;
    this.events = events;
  }

  /**
   * Config seed for etcd key / first RSS {@code ?region=}.
   * When RSS is off and nothing configured → {@code r1}.
   * When RSS is on without yaml home → {@code rss.home-region} only (no default {@code r1}).
   */
  public String configuredSeed() {
    String home = props.activeRegion().home();
    if (home != null && !home.isBlank()) {
      return home.trim();
    }
    String rssHome = props.rss().homeRegion();
    if (rssHome != null && !rssHome.isBlank()) {
      return rssHome.trim();
    }
    if (!props.rss().required()) {
      return DEFAULT_HOME;
    }
    return "";
  }

  /**
   * Resolved home: RSS echo if applied, else config seed.
   *
   * @throws IllegalStateException if unknown (RSS required, no seed, no RSS yet)
   */
  public String get() {
    String rss = fromRss.get();
    if (rss != null && !rss.isBlank()) {
      return rss;
    }
    String seed = configuredSeed();
    if (!seed.isBlank()) {
      return seed;
    }
    throw new IllegalStateException(
        "LES home region is unknown: set coordination.active-region.home (yaml starters), "
            + "coordination.rss.home-region (RSS query seed), or wait for RSS state.region");
  }

  /** Apply home from RSS {@code StateEvent.region} (may run on every state update). */
  public void applyFromRss(String region) {
    if (region == null || region.isBlank()) {
      return;
    }
    String next = region.trim();
    String prev = fromRss.getAndSet(next);
    if (prev == null) {
      log.info("LES home region set from RSS: {}", next);
      events.publishEvent(new HomeRegionChangedEvent(next, null));
    } else if (!prev.equals(next)) {
      log.warn("LES home region changed from RSS: {} -> {}", prev, next);
      events.publishEvent(new HomeRegionChangedEvent(next, prev));
    }
  }

  public boolean hasRssHome() {
    String r = fromRss.get();
    return r != null && !r.isBlank();
  }
}
