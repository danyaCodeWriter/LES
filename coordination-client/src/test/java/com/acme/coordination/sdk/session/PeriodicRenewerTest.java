package com.acme.coordination.sdk.session;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PeriodicRenewerTest {

  @Test
  void renewsUntilStop() throws Exception {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      AtomicInteger n = new AtomicInteger();
      CountDownLatch atLeastTwo = new CountDownLatch(2);
      PeriodicRenewer r =
          new PeriodicRenewer(
              scheduler,
              () -> {
                n.incrementAndGet();
                atLeastTwo.countDown();
              },
              Duration.ofMillis(50),
              "test",
              e -> {});
      r.start();
      Assertions.assertTrue(atLeastTwo.await(2, TimeUnit.SECONDS));
      r.stopQuietly();
      int afterStop = n.get();
      Thread.sleep(150);
      Assertions.assertEquals(afterStop, n.get());
      Assertions.assertFalse(r.isHeld());
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void renewFailureFiresOnLost() throws Exception {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      CountDownLatch lost = new CountDownLatch(1);
      PeriodicRenewer r =
          new PeriodicRenewer(
              scheduler,
              () -> {
                throw new IllegalStateException("boom");
              },
              Duration.ofMillis(40),
              "fail-test",
              e -> {
                Assertions.assertEquals(LeaseLostReason.RENEW_FAILED, e.reason());
                lost.countDown();
              });
      r.start();
      Assertions.assertTrue(lost.await(2, TimeUnit.SECONDS));
      Assertions.assertFalse(r.isHeld());
    } finally {
      scheduler.shutdownNow();
    }
  }
}
