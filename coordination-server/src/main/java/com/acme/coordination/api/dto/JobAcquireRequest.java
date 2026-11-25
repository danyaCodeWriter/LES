package com.acme.coordination.api.dto;

import com.acme.coordination.policy.LeaseTtlBounds;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * Job acquire body. {@code participantToken} is optional: when blank/absent, acquire is
 * <strong>freestanding</strong> (holder is a client label only). When set, holder must be a live
 * coordinator or registered worker with matching token.
 */
public record JobAcquireRequest(
    @NotBlank String holder,
    @Min(LeaseTtlBounds.MIN_SECONDS) @Max(LeaseTtlBounds.MAX_SECONDS) int ttlSeconds,
    Map<String, String> metadata,
    /** Optional. Coordinator/worker token; omit for freestanding lock. */
    String participantToken
) {
  public boolean hasParticipant() {
    return participantToken != null && !participantToken.isBlank();
  }
}
