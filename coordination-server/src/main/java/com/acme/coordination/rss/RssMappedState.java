package com.acme.coordination.rss;

import com.acme.coordination.deployment.DeploymentRole;

/** Mapped RSS state → our cluster active-region fields. */
public record RssMappedState(String regionId, DeploymentRole status, long revision) {}
