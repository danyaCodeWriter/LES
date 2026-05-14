package com.acme.coordination.api.dto;

import jakarta.validation.constraints.NotBlank;

public record WorkerHeartbeatRequest(
    @NotBlank String token
) {}

