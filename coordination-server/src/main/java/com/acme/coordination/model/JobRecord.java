package com.acme.coordination.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Map;

/**
 * Job lock value in etcd. {@code holder} identifies the logical process; {@code token} is issued on
 * acquire and required for {@code :renew} / {@code :release} (same possession model as workers / coordinator).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRecord(
    String jobId,
    String holder,
    String token,
    Map<String, String> metadata,
    Instant issuedAt
) {}
