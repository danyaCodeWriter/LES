package com.acme.coordination.api.dto;

import jakarta.validation.constraints.NotBlank;

/** Тело {@code POST .../jobs/{jobId}:renew}, {@code :release} и global-аналогов (как coordinator renew). */
public record JobTokenRequest(@NotBlank String token) {}
