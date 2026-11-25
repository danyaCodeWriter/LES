package com.acme.coordination.sdk;

import java.net.ConnectException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RestCallPolicyTest {

  @Test
  void retriesThenSucceeds() throws Exception {
    RestCallPolicy p = new RestCallPolicy(3, 10, java.time.Duration.ofSeconds(1));
    int[] n = {0};
    String out =
        p.call(
            () -> {
              n[0]++;
              if (n[0] < 3) {
                throw new ConnectException("down");
              }
              return "ok";
            });
    Assertions.assertEquals("ok", out);
    Assertions.assertEquals(3, n[0]);
  }

  @Test
  void opensCircuitAfterThreshold() {
    RestCallPolicy p = new RestCallPolicy(0, 2, java.time.Duration.ofSeconds(30));
    Assertions.assertThrows(
        ConnectException.class,
        () ->
            p.call(
                () -> {
                  throw new ConnectException("x");
                }));
    Assertions.assertThrows(
        ConnectException.class,
        () ->
            p.call(
                () -> {
                  throw new ConnectException("x");
                }));
    Assertions.assertEquals(RestCallPolicy.State.OPEN, p.state());
    Assertions.assertThrows(IllegalStateException.class, () -> p.call(() -> "nope"));
  }
}
