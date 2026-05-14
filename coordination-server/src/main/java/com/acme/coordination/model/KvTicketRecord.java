package com.acme.coordination.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Stored value for {@code .../global/kv/{keyId}}. {@code accessToken} is returned once on create; required for
 * read/delete/update.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KvTicketRecord(String accessToken, JsonNode value) {}
