package com.acme.coordination.sdk.session;

/** Thrown when acquire/register did not obtain ownership (HTTP conflict or {@code acquired=false}). */
public class LeaseNotAcquiredException extends RuntimeException {

  public LeaseNotAcquiredException(String message) {
    super(message);
  }

  public LeaseNotAcquiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
