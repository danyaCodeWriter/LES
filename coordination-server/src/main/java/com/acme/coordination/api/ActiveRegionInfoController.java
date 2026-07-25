package com.acme.coordination.api;

import com.acme.coordination.deployment.DeploymentRegistrationPolicy;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.region.RegionSwitchDrainPolicy;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/tenants/{tenantId}")
public class ActiveRegionInfoController {

  private final ActiveRegionMonitor activeRegion;
  private final RegionSwitchDrainPolicy drainPolicy;
  private final DeploymentRegistrationPolicy deployment;

  public ActiveRegionInfoController(
      ActiveRegionMonitor activeRegion,
      RegionSwitchDrainPolicy drainPolicy,
      DeploymentRegistrationPolicy deployment
  ) {
    this.activeRegion = activeRegion;
    this.drainPolicy = drainPolicy;
    this.deployment = deployment;
  }

  @GetMapping("/active-region")
  public Map<String, Object> activeRegion(@PathVariable String tenantId) {
    var s = activeRegion.snapshot();
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("tenantId", tenantId);
    m.put("region", s.regionId());
    m.put("status", deployment.instanceStatusWire());
    m.put("activeRegion", s.regionId());
    m.put("source", s.source());
    m.put("regionalWritesBlocked", drainPolicy.regionalWritesBlocked());
    m.put("drainingPreviousRegion", drainPolicy.drainingPreviousRegionId().orElse(null));
    return m;
  }
}
