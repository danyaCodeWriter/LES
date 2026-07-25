package com.acme.coordination.api;

import com.acme.coordination.api.dto.ActiveRegionPutRequest;
import com.acme.coordination.deployment.DeploymentRegistrationPolicy;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.region.RegionSwitchDrainPolicy;
import com.acme.coordination.security.SreTokenVerifier;
import com.acme.coordination.service.ActiveRegionService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/active-region")
public class ClusterActiveRegionController {

  private final ActiveRegionMonitor activeRegion;
  private final ActiveRegionService activeRegionService;
  private final RegionSwitchDrainPolicy drainPolicy;
  private final DeploymentRegistrationPolicy deployment;
  private final SreTokenVerifier sre;

  public ClusterActiveRegionController(
      ActiveRegionMonitor activeRegion,
      ActiveRegionService activeRegionService,
      RegionSwitchDrainPolicy drainPolicy,
      DeploymentRegistrationPolicy deployment,
      SreTokenVerifier sre
  ) {
    this.activeRegion = activeRegion;
    this.activeRegionService = activeRegionService;
    this.drainPolicy = drainPolicy;
    this.deployment = deployment;
    this.sre = sre;
  }

  @GetMapping
  public Map<String, Object> get() {
    return toInfo(null);
  }

  @PutMapping
  public CompletableFuture<Map<String, Object>> put(
      @RequestBody ActiveRegionPutRequest body, ServerHttpRequest request
  ) {
    sre.assertSreFromRequest(request);
    String region = body == null ? null : body.region();
    String status = body == null ? null : body.status();
    return activeRegionService
        .set(region, status)
        .thenApply(
            state -> {
              activeRegion.tick();
              return toInfo(null);
            });
  }

  Map<String, Object> toInfo(String tenantId) {
    var s = activeRegion.snapshot();
    Map<String, Object> m = new LinkedHashMap<>();
    if (tenantId != null) {
      m.put("tenantId", tenantId);
    }
    // region = where THIS LES instance is deployed; activeRegion = who serves the cluster
    m.put("region", activeRegion.instanceHomeRegion());
    m.put("activeRegion", s.regionId());
    m.put("status", deployment.instanceStatusWire());
    m.put("source", s.source());
    m.put("regionalWritesBlocked", drainPolicy.regionalWritesBlocked());
    m.put("drainingPreviousRegion", drainPolicy.drainingPreviousRegionId().orElse(null));
    return m;
  }
}
