package com.acme.coordination.region;

import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.deployment.DeploymentRegistrationPolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class ActiveRegionMonitor {

  private static final Logger log = LoggerFactory.getLogger(ActiveRegionMonitor.class);

  private final CoordinationProperties props;
  private final ObjectMapper objectMapper;
  private final RegionSwitchDrainPolicy drainPolicy;
  private final DeploymentRegistrationPolicy deployment;
  private final AtomicReference<ActiveRegionSnapshot> current = new AtomicReference<>(new ActiveRegionSnapshot("default", "initial"));
  private final Sinks.Many<String> changes = Sinks.many().multicast().onBackpressureBuffer();

  public ActiveRegionMonitor(
      CoordinationProperties props,
      ObjectMapper objectMapper,
      RegionSwitchDrainPolicy drainPolicy,
      DeploymentRegistrationPolicy deployment
  ) {
    this.props = props;
    this.objectMapper = objectMapper;
    this.drainPolicy = drainPolicy;
    this.deployment = deployment;
    Resolved r = resolve();
    String rid = normalizeRegionId(r.regionId());
    current.set(new ActiveRegionSnapshot(rid, r.source()));
  }

  /** Logical segment used for active-region etcd keys (short REST/gRPC paths; not exposed in URLs). */
  public String activeRegionId() {
    return current.get().regionId();
  }

  public ActiveRegionSnapshot snapshot() {
    return current.get();
  }

  /** SSE: JSON payload when active region (or its source) changes. */
  public Flux<String> activeRegionEvents() {
    return changes.asFlux()
        .startWith(serialize(snapshot()));
  }

  @Scheduled(fixedDelayString = "${coordination.active-region.poll-interval-ms:5000}")
  public void tick() {
    Resolved r = resolve();
    String rid = normalizeRegionId(r.regionId());
    ActiveRegionSnapshot next = new ActiveRegionSnapshot(rid, r.source());
    ActiveRegionSnapshot prev = current.get();
    if (prev.regionId().equals(next.regionId()) && prev.source().equals(next.source())) {
      return;
    }
    if (!prev.regionId().equals(next.regionId())) {
      drainPolicy.onActiveRegionIdChanged(prev.regionId(), next.regionId());
    }
    current.set(next);
    try {
      changes.tryEmitNext(serialize(next));
    } catch (Exception e) {
      log.warn("emit active-region event failed", e);
    }
    log.info("active region updated: {} (source={})", next.regionId(), next.source());
  }

  /**
   * Вызывается при смене {@link com.acme.coordination.deployment.DeploymentRole} (ACTIVE / STANDBY / SWITCH), чтобы
   * подписчики {@link #activeRegionEvents()} получили обновлённое поле {@code status} без смены logical region.
   */
  public void emitDeploymentSnapshotForSse() {
    try {
      changes.tryEmitNext(serialize(snapshot()));
    } catch (Exception e) {
      log.warn("emit deployment snapshot for SSE failed", e);
    }
  }

  private static String normalizeRegionId(String regionId) {
    if (regionId == null || regionId.isBlank()) {
      return "default";
    }
    return regionId.trim();
  }

  private String serialize(ActiveRegionSnapshot s) {
    try {
      java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
      m.put("region", s.regionId());
      m.put("status", deployment.instanceStatusWire());
      m.put("activeRegion", s.regionId());
      m.put("source", s.source());
      m.put("regionalWritesBlocked", drainPolicy.regionalWritesBlocked());
      m.put("drainingPreviousRegion", drainPolicy.drainingPreviousRegionId().orElse(null));
      return objectMapper.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private Resolved resolve() {
    CoordinationProperties.ActiveRegion ar = props.activeRegion();
    if (ar == null) {
      return new Resolved("default", "none");
    }
    if (ar.filePath() != null && !ar.filePath().isBlank()) {
      try {
        Path p = Path.of(ar.filePath().trim());
        if (Files.isReadable(p)) {
          String content = Files.readString(p, StandardCharsets.UTF_8).trim();
          if (!content.isEmpty()) {
            String firstLine = content.contains("\n") ? content.substring(0, content.indexOf('\n')).trim() : content;
            if (!firstLine.isEmpty()) {
              return new Resolved(firstLine, "file");
            }
          }
        }
      } catch (IOException e) {
        log.debug("active region file read failed: {}", ar.filePath(), e);
      }
    }
    String envVar = (ar.envVar() == null || ar.envVar().isBlank()) ? "COORDINATION_ACTIVE_REGION" : ar.envVar().trim();
    String fromEnv = System.getenv(envVar);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return new Resolved(fromEnv.trim(), "env:" + envVar);
    }
    if (ar.value() != null && !ar.value().isBlank()) {
      return new Resolved(ar.value().trim(), "config");
    }
    return new Resolved("default", "fallback");
  }

  private record Resolved(String regionId, String source) {}
}
