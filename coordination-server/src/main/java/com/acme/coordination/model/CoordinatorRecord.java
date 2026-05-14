package com.acme.coordination.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Map;

/**
 * Coordinator value under {@code .../leader}. {@code fenceEpoch} is bumped atomically with each new leader
 * (see {@link com.acme.coordination.service.CoordinatorService}) for fencing writers.
 *
 * <p>{@code coordinatorId} is the stable logical id of the coordinator role (like {@code workerId} for workers);
 * {@code instanceId} identifies the process holding the lease; {@code token} is issued on acquire and required for renew
 * (same possession model as {@link com.acme.coordination.model.WorkerRecord}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoordinatorRecord(
    String coordinatorId,
    String instanceId,
    String token,
    Map<String, String> metadata,
    Instant issuedAt,
    long fenceEpoch
) {}
