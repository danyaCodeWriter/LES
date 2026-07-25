package com.acme.coordination.region;

/** Fired when LES home becomes known or changes (typically from RSS {@code state.region}). */
public record HomeRegionChangedEvent(String home, String previousHome) {}
