package com.acme.coordination.region;

import com.acme.coordination.deployment.DeploymentRole;

public record ActiveRegionSnapshot(String regionId, DeploymentRole status, String source) {

  public ActiveRegionSnapshot {
    if (regionId == null || regionId.isBlank()) {
      regionId = "r1";
    }
    if (status == null) {
      status = DeploymentRole.ACTIVE;
    }
    if (source == null || source.isBlank()) {
      source = "initial";
    }
  }
}
