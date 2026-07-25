package com.acme.coordination.deployment;

import com.acme.coordination.region.ActiveRegionMonitor;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Role from this LES home's etcd key ({@code .../cluster/regions/{home}/activeRegion}),
 * which holds RSS POV status for that home.
 */
@Component
public class EtcdDeploymentRoleSource implements DeploymentRoleSource {

  private final ActiveRegionMonitor activeRegion;

  public EtcdDeploymentRoleSource(@Lazy ActiveRegionMonitor activeRegion) {
    this.activeRegion = activeRegion;
  }

  @Override
  public DeploymentRole currentRole() {
    return activeRegion.effectiveRole();
  }
}
