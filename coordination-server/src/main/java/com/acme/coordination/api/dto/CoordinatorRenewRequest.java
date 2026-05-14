package com.acme.coordination.api.dto;

import jakarta.validation.constraints.NotBlank;

/** Тело {@code POST .../coordinator/renew} и {@code .../global/coordinator/renew} (как heartbeat воркера). */
public record CoordinatorRenewRequest(@NotBlank String token) {}
