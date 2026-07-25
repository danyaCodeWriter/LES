package com.acme.coordination.deployment;

/** Current cluster deployment role (from etcd / ActiveRegionMonitor). */
@FunctionalInterface
public interface DeploymentRoleSource {
  DeploymentRole currentRole();
}
