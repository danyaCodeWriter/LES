package com.acme.coordination.watch;

public record WatchEventDto(
    String type,
    String key,
    long modRevision,
    long leaseId,
    String valueJson
) {}

