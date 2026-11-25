package com.acme.coordination.sdk.les;

import java.time.Duration;

public record LesOptions(String sreToken, String homeRegion, Duration defaultTtl) {}
