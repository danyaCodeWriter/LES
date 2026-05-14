package com.acme.coordination.api.dto;

import com.acme.coordination.policy.LeaseTtlBounds;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record JobAcquireRequest(
    @NotBlank String holder,
    @Min(LeaseTtlBounds.MIN_SECONDS) @Max(LeaseTtlBounds.MAX_SECONDS) int ttlSeconds,
    Map<String, String> metadata,
    /** Coordinator acquire token or worker register token for {@code holder}. */
    @NotBlank String participantToken
) {}

