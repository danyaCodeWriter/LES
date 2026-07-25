package com.acme.coordination.rss;

import com.acme.coordination.deployment.DeploymentRole;

/**
 * Mapped RSS state.
 *
 * @param homeRegion {@code StateEvent.region} — home of this LES (etcd key suffix)
 * @param regionId {@code StateEvent.activeRegion} — who serves the RG
 * @param status POV status for {@code homeRegion}
 * @param revision RSS revision
 */
public record RssMappedState(String homeRegion, String regionId, DeploymentRole status, long revision) {}
