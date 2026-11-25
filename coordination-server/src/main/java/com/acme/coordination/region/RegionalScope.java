package com.acme.coordination.region;

import com.acme.coordination.config.CoordinationProperties;
import org.springframework.stereotype.Component;

/**
 * Static etcd region segment for this LES process ({@code coordination.region} / {@code COORDINATION_REGION}).
 * Clients choose regional short-path vs global APIs; LES does not track an active region.
 */
@Component
public class RegionalScope {

  private final String regionId;

  public RegionalScope(CoordinationProperties props) {
    String region = props.region();
    if (region == null || region.isBlank()) {
      region = "r1";
    }
    this.regionId = region.trim();
  }

  public String regionId() {
    return regionId;
  }
}
