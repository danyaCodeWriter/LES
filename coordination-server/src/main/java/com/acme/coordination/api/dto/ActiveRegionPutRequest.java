package com.acme.coordination.api.dto;

/**
 * Body for {@code PUT /v1/active-region}.
 *
 * @param region cluster {@code activeRegion} to set (not instance home)
 * @param status deployment status from this LES home's POV
 */
public record ActiveRegionPutRequest(String region, String status) {}
