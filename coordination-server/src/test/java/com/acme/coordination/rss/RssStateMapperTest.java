package com.acme.coordination.rss;

import com.acme.coordination.deployment.DeploymentRole;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RssStateMapperTest {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

  @Test
  void mapsActiveAndStandIn() {
    var active =
        RssStateMapper.fromStateEvent(
                new RssStateEvent("r1", "r1", "active", false, 1L, 10L))
            .orElseThrow();
    Assertions.assertEquals("r1", active.regionId());
    Assertions.assertEquals(DeploymentRole.ACTIVE, active.status());

    var standIn =
        RssStateMapper.fromStateEvent(
                new RssStateEvent("r1", "r2", "stand_in", true, 2L, 11L))
            .orElseThrow();
    Assertions.assertEquals("r2", standIn.regionId());
    Assertions.assertEquals(DeploymentRole.STANDBY, standIn.status());

    var sw =
        RssStateMapper.fromStateEvent(
                new RssStateEvent("r1", "r2", "switch", true, 3L, 12L))
            .orElseThrow();
    Assertions.assertEquals(DeploymentRole.SWITCH, sw.status());
  }

  @Test
  void parseStateDataJson() {
    String json =
        "{\"region\":\"r1\",\"activeRegion\":\"eu\",\"status\":\"active\",\"regionalWritesBlocked\":false,\"epoch\":1,\"revision\":5}";
    var mapped = RssStateMapper.parseStateData(mapper, json).orElseThrow();
    Assertions.assertEquals("eu", mapped.regionId());
    Assertions.assertEquals(DeploymentRole.ACTIVE, mapped.status());
    Assertions.assertEquals(5L, mapped.revision());
  }

  @Test
  void rejectsMissingActiveRegion() {
    Assertions.assertTrue(
        RssStateMapper.fromStateEvent(new RssStateEvent("r1", "", "active", false, 1L, 1L)).isEmpty());
  }
}
