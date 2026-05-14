package com.acme.coordination.policy;

/** Lease TTL for coordinator / worker / job: etcd keepalive cadence must fit within this window. */
public final class LeaseTtlBounds {

  public static final int MIN_SECONDS = 10;
  public static final int MAX_SECONDS = 300;

  private LeaseTtlBounds() {}

  public static void requireValid(int ttlSeconds) {
    if (ttlSeconds < MIN_SECONDS || ttlSeconds > MAX_SECONDS) {
      throw new IllegalArgumentException(
          "ttlSeconds must be between " + MIN_SECONDS + " and " + MAX_SECONDS + " inclusive, got " + ttlSeconds);
    }
  }
}
