package com.acme.coordination.rss;

import com.acme.coordination.deployment.DeploymentRole;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;

final class RssStateMapper {

  private RssStateMapper() {}

  static Optional<RssMappedState> fromStateEvent(RssStateEvent ev) {
    if (ev == null) {
      return Optional.empty();
    }
    String regionId = ev.activeRegion();
    if (regionId == null || regionId.isBlank()) {
      return Optional.empty();
    }
    DeploymentRole role;
    try {
      role = DeploymentRole.parse(ev.status());
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
    long rev = ev.revision() == null ? 0L : ev.revision();
    return Optional.of(new RssMappedState(regionId.trim(), role, rev));
  }

  static Optional<RssMappedState> parseStateData(ObjectMapper mapper, String dataJson) {
    if (dataJson == null || dataJson.isBlank()) {
      return Optional.empty();
    }
    try {
      return fromStateEvent(mapper.readValue(dataJson, RssStateEvent.class));
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
