package com.acme.coordination.sdk.session;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules a renew/heartbeat runnable until {@link #stop()} or the first failure.
 */
final class PeriodicRenewer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PeriodicRenewer.class);

  @FunctionalInterface
  interface RenewAction {
    void renew() throws Exception;
  }

  private final ScheduledExecutorService scheduler;
  private final RenewAction action;
  private final Duration every;
  private final String label;
  private final AtomicBoolean held = new AtomicBoolean(true);
  private final AtomicBoolean lostFired = new AtomicBoolean(false);
  private final AtomicReference<Consumer<LeaseLostEvent>> onLost;
  private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>();

  PeriodicRenewer(
      ScheduledExecutorService scheduler,
      RenewAction action,
      Duration every,
      String label,
      Consumer<LeaseLostEvent> onLost) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.action = Objects.requireNonNull(action);
    this.every = Objects.requireNonNull(every);
    this.label = Objects.requireNonNull(label);
    this.onLost = new AtomicReference<>(onLost == null ? e -> {} : onLost);
  }

  void start() {
    long ms = Math.max(200L, every.toMillis());
    ScheduledFuture<?> f =
        scheduler.scheduleWithFixedDelay(this::tick, ms, ms, TimeUnit.MILLISECONDS);
    future.set(f);
  }

  boolean isHeld() {
    return held.get();
  }

  private void tick() {
    if (!held.get()) {
      return;
    }
    try {
      action.renew();
    } catch (Exception e) {
      log.warn("lease renew failed ({}): {}", label, e.toString());
      held.set(false);
      cancelFuture();
      JsonLeaseResponses.fireOnce(lostFired, onLost, LeaseLostEvent.renewFailed(e, label));
    }
  }

  /** Stop renew loop without firing lost. */
  void stopQuietly() {
    held.set(false);
    cancelFuture();
  }

  /** Notify {@link LeaseLostReason#STOPPED} once (idempotent with renew-failed). */
  void notifyStopped() {
    JsonLeaseResponses.fireOnce(lostFired, onLost, LeaseLostEvent.stopped(label));
  }

  /** Stop renew and notify {@link LeaseLostReason#STOPPED} once. */
  void stopAndNotify() {
    stopQuietly();
    notifyStopped();
  }

  private void cancelFuture() {
    ScheduledFuture<?> f = future.getAndSet(null);
    if (f != null) {
      f.cancel(false);
    }
  }

  @Override
  public void close() {
    stopQuietly();
  }
}
