package com.acme.coordination.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Body for {@code PUT /v1/tenants/{tenant}/global/kv/{keyId}}. */
public record KvPutRequest(JsonNode value, Integer ttlSeconds) {}
