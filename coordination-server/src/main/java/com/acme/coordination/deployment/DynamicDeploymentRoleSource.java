package com.acme.coordination.deployment;

import com.acme.coordination.region.ActiveRegionMonitor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Поллинг роли раз в {@code coordination.active-region.poll-interval-ms}: файл (если задан) → env → статический
 * {@code coordination.deployment.role}. При смене роли обновляются фильтры REST/gRPC и при наличии
 * подписчиков — событие SSE {@code stream/active-region}.
 */
@Component
public class DynamicDeploymentRoleSource implements DeploymentRoleSource {

  private static final Logger log = LoggerFactory.getLogger(DynamicDeploymentRoleSource.class);

  private final CoordinationDeploymentProperties props;
  private final ObjectProvider<ActiveRegionMonitor> activeRegionMonitor;
  private final AtomicReference<DeploymentRole> role = new AtomicReference<>(DeploymentRole.ACTIVE);
  private final AtomicReference<String> source = new AtomicReference<>("initial");

  public DynamicDeploymentRoleSource(
      CoordinationDeploymentProperties props,
      ObjectProvider<ActiveRegionMonitor> activeRegionMonitor
  ) {
    this.props = props;
    this.activeRegionMonitor = activeRegionMonitor;
    Resolved r = resolve();
    this.role.set(r.role());
    this.source.set(r.source());
  }

  @Override
  public DeploymentRole currentRole() {
    return role.get();
  }

  @Scheduled(fixedDelayString = "${coordination.active-region.poll-interval-ms:5000}")
  public void tick() {
    Resolved r = resolve();
    DeploymentRole prev = role.get();
    String prevSrc = source.get();
    if (prev == r.role() && prevSrc.equals(r.source())) {
      return;
    }
    if (prev != r.role()) {
      log.info("deployment role updated: {} -> {} (source={})", prev, r.role(), r.source());
      role.set(r.role());
      source.set(r.source());
      activeRegionMonitor.ifAvailable(ActiveRegionMonitor::emitDeploymentSnapshotForSse);
    } else {
      source.set(r.source());
    }
  }

  private Resolved resolve() {
    if (props.getRoleFile() != null && !props.getRoleFile().isBlank()) {
      try {
        Path p = Path.of(props.getRoleFile().trim());
        if (Files.isReadable(p)) {
          String content = Files.readString(p, StandardCharsets.UTF_8).trim();
          if (!content.isEmpty()) {
            String firstLine = content.contains("\n") ? content.substring(0, content.indexOf('\n')).trim() : content;
            Optional<DeploymentRole> parsed = parseRoleToken(firstLine);
            if (parsed.isPresent()) {
              return new Resolved(parsed.get(), "file");
            }
            log.warn("deployment role file {} has invalid first line: {}", p, firstLine);
          }
        }
      } catch (IOException e) {
        log.debug("deployment role file read failed: {}", props.getRoleFile(), e);
      }
    }
    String envVar = props.getRoleEnvVar();
    String fromEnv = System.getenv(envVar);
    if (fromEnv != null && !fromEnv.isBlank()) {
      Optional<DeploymentRole> parsed = parseRoleToken(fromEnv.trim());
      if (parsed.isPresent()) {
        return new Resolved(parsed.get(), "env:" + envVar);
      }
      log.warn("deployment role env {} has invalid value: {}", envVar, fromEnv.trim());
    }
    return new Resolved(props.getRole(), "config");
  }

  static Optional<DeploymentRole> parseRoleToken(String raw) {
    if (raw == null) {
      return Optional.empty();
    }
    String t = raw.trim();
    if (t.isEmpty()) {
      return Optional.empty();
    }
    String u = t.toUpperCase(Locale.ROOT).replace('-', '_');
    if ("ACTIVE".equals(u)) {
      return Optional.of(DeploymentRole.ACTIVE);
    }
    if ("STANDBY".equals(u) || "STAND_IN".equals(u) || "STANDIN".equals(u)) {
      return Optional.of(DeploymentRole.STANDBY);
    }
    if ("SWITCH".equals(u) || "SWITCH_OVER".equals(u) || "SWITCHOVER".equals(u)) {
      return Optional.of(DeploymentRole.SWITCH);
    }
    return Optional.empty();
  }

  private record Resolved(DeploymentRole role, String source) {}
}
