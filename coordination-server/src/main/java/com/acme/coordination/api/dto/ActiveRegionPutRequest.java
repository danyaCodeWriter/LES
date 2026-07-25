package com.acme.coordination.api.dto;

/** Body for {@code PUT /v1/active-region}. */
public record ActiveRegionPutRequest(String region, String status) {}
