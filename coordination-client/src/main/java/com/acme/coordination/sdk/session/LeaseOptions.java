package com.acme.coordination.sdk.session;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Options for auto-renew / heartbeat loops.
 *
 * @param ttl lease TTL requested on acquire/register (server: 10…300 s for regional)
 * @param renewEvery how often to renew; {@code null} → {@code ttl/3} (min 1s)
 * @param onLost called once when the lease is no longer held (renew fail or stop after loss)
 */
public record LeaseOptions(Duration ttl, Duration renewEvery, Consumer<LeaseLostEvent> onLost) {

  public LeaseOptions {
    Objects.requireNonNull(ttl, "ttl");
    if (ttl.isNegative() || ttl.isZero()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
    if (renewEvery != null && (renewEvery.isNegative() || renewEvery.isZero())) {
      throw new IllegalArgumentException("renewEvery must be positive when set");
    }
  }

  public static LeaseOptions ofTtlSeconds(int ttlSeconds) {
    return new LeaseOptions(Duration.ofSeconds(ttlSeconds), null, ignored -> {});
  }

  public LeaseOptions withOnLost(Consumer<LeaseLostEvent> onLost) {
    return new LeaseOptions(ttl, renewEvery, onLost == null ? ignored -> {} : onLost);
  }

  public LeaseOptions withRenewEvery(Duration renewEvery) {
    return new LeaseOptions(ttl, renewEvery, onLost);
  }

  public int ttlSeconds() {
    long s = ttl.getSeconds();
    if (s > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) Math.max(1, s);
  }

  /** Effective renew period (default ttl/3, at least 1 second). */
  public Duration effectiveRenewEvery() {
    if (renewEvery != null) {
      return renewEvery;
    }
    long sec = Math.max(1, ttl.getSeconds() / 3);
    return Duration.ofSeconds(sec);
  }
}
