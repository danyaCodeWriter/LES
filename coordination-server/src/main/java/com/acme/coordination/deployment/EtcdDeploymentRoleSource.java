package com.acme.coordination.deployment;

import com.acme.coordination.region.ActiveRegionMonitor;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/** Deployment role from cluster etcd state (via {@link ActiveRegionMonitor}), not env/file. */
@Component
public class EtcdDeploymentRoleSource implements DeploymentRoleSource {

  private final ActiveRegionMonitor activeRegion;

  public EtcdDeploymentRoleSource(@Lazy ActiveRegionMonitor activeRegion) {
    this.activeRegion = activeRegion;
  }

  @Override
  public DeploymentRole currentRole() {
    return activeRegion.snapshot().status();
  }
}
