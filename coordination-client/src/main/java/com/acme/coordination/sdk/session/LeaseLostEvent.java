package com.acme.coordination.sdk.session;

/**
 * Fired when a {@link CoordinatorLease}, {@link WorkerSession}, or {@link JobLease} is no longer held.
 *
 * @param reason why
 * @param cause optional underlying error (renew HTTP failure, etc.)
 * @param detail human-readable context (ids)
 */
public record LeaseLostEvent(LeaseLostReason reason, Throwable cause, String detail) {

  public static LeaseLostEvent stopped(String detail) {
    return new LeaseLostEvent(LeaseLostReason.STOPPED, null, detail);
  }

  public static LeaseLostEvent renewFailed(Throwable cause, String detail) {
    return new LeaseLostEvent(LeaseLostReason.RENEW_FAILED, cause, detail);
  }

  public static LeaseLostEvent notAcquired(String detail) {
    return new LeaseLostEvent(LeaseLostReason.NOT_ACQUIRED, null, detail);
  }
}
