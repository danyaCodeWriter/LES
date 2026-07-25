package com.acme.coordination.rss;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Payload of an RSS SSE {@code state} event (see openapi/rss-api.yaml). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RssStateEvent(
    String region,
    String activeRegion,
    String status,
    Boolean regionalWritesBlocked,
    Long epoch,
    Long revision
) {}
