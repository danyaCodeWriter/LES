package com.acme.coordination.sdk;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Lightweight retry / circuit hygiene for REST calls — no external resilience library.
 * Opens after consecutive failures; half-opens after cool-down.
 */
public final class RestCallPolicy {

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int maxRetries;
  private final int failureThreshold;
  private final Duration openFor;
  private final Object lock = new Object();
  private State state = State.CLOSED;
  private int consecutiveFailures;
  private long openUntilNanos;

  public RestCallPolicy() {
    this(2, 5, Duration.ofSeconds(5));
  }

  public RestCallPolicy(int maxRetries, int failureThreshold, Duration openFor) {
    this.maxRetries = Math.max(0, maxRetries);
    this.failureThreshold = Math.max(1, failureThreshold);
    this.openFor = openFor == null ? Duration.ofSeconds(5) : openFor;
  }

  public State state() {
    synchronized (lock) {
      maybeHalfOpen();
      return state;
    }
  }

  public <T> T call(SupplierWithException<T> action) throws Exception {
    synchronized (lock) {
      maybeHalfOpen();
      if (state == State.OPEN) {
        throw new IllegalStateException("Les REST circuit open — cool-down in progress");
      }
    }
    Exception last = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        T result = action.get();
        onSuccess();
        return result;
      } catch (Exception e) {
        last = e;
        if (!isRetryable(e) || attempt == maxRetries) {
          onFailure();
          throw e;
        }
        Thread.sleep(backoffMs(attempt));
      }
    }
    onFailure();
    throw last != null ? last : new IllegalStateException("retry exhausted");
  }

  private void onSuccess() {
    synchronized (lock) {
      consecutiveFailures = 0;
      state = State.CLOSED;
    }
  }

  private void onFailure() {
    synchronized (lock) {
      consecutiveFailures++;
      if (consecutiveFailures >= failureThreshold) {
        state = State.OPEN;
        openUntilNanos = System.nanoTime() + openFor.toNanos();
      }
    }
  }

  private void maybeHalfOpen() {
    if (state == State.OPEN && System.nanoTime() >= openUntilNanos) {
      state = State.HALF_OPEN;
    }
  }

  static boolean isRetryable(Exception e) {
    if (e instanceof ConnectException || e instanceof HttpTimeoutException || e instanceof IOException) {
      return true;
    }
    String m = e.getMessage();
    if (m == null) {
      return false;
    }
    return m.contains("HTTP 502") || m.contains("HTTP 503") || m.contains("HTTP 504");
  }

  private static long backoffMs(int attempt) {
    long base = 50L << attempt;
    return base + ThreadLocalRandom.current().nextLong(0, 40);
  }

  @FunctionalInterface
  public interface SupplierWithException<T> {
    T get() throws Exception;
  }
}
