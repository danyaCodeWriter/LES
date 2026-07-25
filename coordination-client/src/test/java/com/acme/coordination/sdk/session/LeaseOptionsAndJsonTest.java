package com.acme.coordination.sdk.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LeaseOptionsAndJsonTest {

  private final ObjectMapper json = new ObjectMapper();

  @Test
  void defaultRenewEveryIsTtlOverThree() {
    LeaseOptions o = LeaseOptions.ofTtlSeconds(30);
    Assertions.assertEquals(Duration.ofSeconds(10), o.effectiveRenewEvery());
  }

  @Test
  void requireTokenAndAcquired() {
    ObjectNode ok = json.createObjectNode();
    ok.put("acquired", true);
    ok.put("token", "t1");
    ok.put("fenceEpoch", 9);
    Assertions.assertTrue(JsonLeaseResponses.acquiredFlag(ok));
    Assertions.assertEquals("t1", JsonLeaseResponses.requireToken(ok, "job"));
    Assertions.assertEquals(9L, JsonLeaseResponses.fenceEpoch(ok));

    ObjectNode created = json.createObjectNode();
    created.put("created", true);
    created.put("token", "wt");
    Assertions.assertTrue(JsonLeaseResponses.acquiredFlag(created));

    ObjectNode miss = json.createObjectNode();
    miss.put("acquired", false);
    Assertions.assertFalse(JsonLeaseResponses.acquiredFlag(miss));
    Assertions.assertThrows(
        LeaseNotAcquiredException.class, () -> JsonLeaseResponses.requireToken(miss, "job"));
  }
}
