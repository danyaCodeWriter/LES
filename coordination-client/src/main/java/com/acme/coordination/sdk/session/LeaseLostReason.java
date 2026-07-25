package com.acme.coordination.sdk.session;

/** Why a lease/session stopped being held. */
public enum LeaseLostReason {
  /** {@link AutoCloseable#close()} or explicit stop. */
  STOPPED,
  /** Renew / heartbeat failed (lease likely expired or token invalid). */
  RENEW_FAILED,
  /** Acquire/register did not obtain the lease (conflict / not acquired). */
  NOT_ACQUIRED
}
