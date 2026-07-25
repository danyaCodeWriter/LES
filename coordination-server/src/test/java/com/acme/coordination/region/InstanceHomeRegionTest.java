package com.acme.coordination.region;

import com.acme.coordination.config.CoordinationProperties;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InstanceHomeRegionTest {

  private static final AtomicReference<HomeRegionChangedEvent> LAST = new AtomicReference<>();

  private static CoordinationProperties props(boolean rssRequired, String home, String rssHome) {
    return new CoordinationProperties(
        CoordinationProperties.Etcd.plain(
            List.of("http://127.0.0.1:2379"), "/coord/v1/tenants", Duration.ofSeconds(1), Duration.ofSeconds(1)),
        "",
        new CoordinationProperties.ActiveRegion(home, "r1", "ACTIVE", 5000L),
        new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
        new CoordinationProperties.Sre(""),
        new CoordinationProperties.Limits(0),
        new CoordinationProperties.Rss(rssRequired, "", "", rssHome, Duration.ofSeconds(5)));
  }

  private static InstanceHomeRegion home(boolean rssRequired, String home, String rssHome) {
    LAST.set(null);
    return new InstanceHomeRegion(
        props(rssRequired, home, rssHome),
        event -> {
          if (event instanceof HomeRegionChangedEvent h) {
            LAST.set(h);
          }
        });
  }

  @Test
  void yamlModeUsesConfiguredHome() {
    var h = home(false, "r2", "");
    Assertions.assertEquals("r2", h.configuredSeed());
    Assertions.assertEquals("r2", h.get());
  }

  @Test
  void yamlModeBlankHomeDefaultsToR1() {
    var h = home(false, "", "");
    Assertions.assertEquals("r1", h.configuredSeed());
    Assertions.assertEquals("r1", h.get());
  }

  @Test
  void rssWithYamlStartersUsesHomeUntilRss() {
    var p = props(true, "r1", "");
    Assertions.assertTrue(p.hasYamlActiveRegionStarters());
    var h = home(true, "r1", "");
    Assertions.assertEquals("r1", h.get());
    h.applyFromRss("r2");
    Assertions.assertEquals("r2", h.get());
    Assertions.assertEquals("r2", LAST.get().home());
    Assertions.assertNull(LAST.get().previousHome());
  }

  @Test
  void rssHomeChangePublishesPrevious() {
    var h = home(true, "r1", "");
    h.applyFromRss("r1");
    h.applyFromRss("r2");
    Assertions.assertEquals("r2", LAST.get().home());
    Assertions.assertEquals("r1", LAST.get().previousHome());
  }

  @Test
  void rssWithoutYamlStartersNeedsQuerySeedOrRss() {
    var p = props(true, "", "seed");
    Assertions.assertFalse(p.hasYamlActiveRegionStarters());
    var h = home(true, "", "seed");
    Assertions.assertEquals("seed", h.configuredSeed());
    Assertions.assertEquals("seed", h.get());
    h.applyFromRss("r1");
    Assertions.assertEquals("r1", h.get());
  }

  @Test
  void rssStrictFailsWithoutSeedUntilRssApplies() {
    var p = props(true, "", "");
    Assertions.assertFalse(p.hasYamlActiveRegionStarters());
    var h = home(true, "", "");
    Assertions.assertEquals("", h.configuredSeed());
    Assertions.assertThrows(IllegalStateException.class, h::get);
    h.applyFromRss("r1");
    Assertions.assertEquals("r1", h.get());
  }

  @Test
  void defaultPropsMatchApplicationYamlIntent() {
    var p = props(false, "r1", "");
    Assertions.assertFalse(p.rss().required());
    Assertions.assertEquals("r1", p.activeRegion().home());
    Assertions.assertEquals("r1", p.activeRegion().bootstrapRegionId());
    Assertions.assertEquals("ACTIVE", p.activeRegion().bootstrapStatus().name());
    Assertions.assertTrue(p.hasYamlActiveRegionStarters());
  }
}
