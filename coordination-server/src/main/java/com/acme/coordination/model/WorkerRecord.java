package com.acme.coordination.model;

import java.time.Instant;
import java.util.Map;

public record WorkerRecord(
    String workerId,
    String instanceId,
    String token,
    Map<String, String> metadata,
    Instant issuedAt
) {}

