package com.acme.coordination.region;

import com.acme.coordination.deployment.DeploymentRole;

/** Persisted value of the cluster-wide active-region etcd key. */
public record ActiveRegionState(String regionId, String status, long updatedAtUnixMillis) {

  public DeploymentRole deploymentRole() {
    try {
      return DeploymentRole.parse(status);
    } catch (IllegalArgumentException e) {
      return DeploymentRole.ACTIVE;
    }
  }

  public static ActiveRegionState of(String regionId, DeploymentRole role) {
    return new ActiveRegionState(
        regionId == null || regionId.isBlank() ? "r1" : regionId.trim(),
        role == null ? DeploymentRole.ACTIVE.name() : role.name(),
        System.currentTimeMillis());
  }
}
