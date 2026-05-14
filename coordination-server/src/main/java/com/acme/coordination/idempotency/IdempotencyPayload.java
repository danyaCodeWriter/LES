package com.acme.coordination.idempotency;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** JSON value under {@code .../tenants/{tenant}/idempotency/{fp}}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IdempotencyPayload(int httpStatus, String bodyBase64) {

  public static IdempotencyPayload of(int httpStatus, byte[] bodyUtf8) {
    return new IdempotencyPayload(httpStatus, java.util.Base64.getEncoder().encodeToString(bodyUtf8));
  }

  public byte[] bodyBytes() {
    return java.util.Base64.getDecoder().decode(bodyBase64);
  }
}
