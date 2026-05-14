package com.acme.coordination.deployment;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "coordination.deployment")
public class CoordinationDeploymentProperties {

  /**
   * {@code ACTIVE} — полный REST/gRPC по regional и short-path (active region).
   * {@code STANDBY} — тот же etcd, но regional/active-pocket <strong>мутации</strong> отклоняются; {@code /global/...} всегда.
   * {@code SWITCH} — regional мутации для клиентов отключены (как standby), но инстанс участвует в {@code REGION_DRAIN}
   * и при смене logical region может ускоренно удалять старые lease-ключи (см. README).
   * Env: {@code COORDINATION_DEPLOYMENT_ROLE=STANDBY} (synonym stand-in passive site) или {@code SWITCH}.
   * <p>При включённом {@link DynamicDeploymentRoleSource} роль переопределяется поллингом: файл (если задан) →
   * {@link #getRoleEnvVar()} → это поле как запасной вариант.
   */
  private DeploymentRole role = DeploymentRole.ACTIVE;

  /**
   * Если непусто — первая непустая строка файла задаёт роль (как {@code coordination.active-region.file-path}).
   * Имеет приоритет над env и над статическим {@link #getRole()}.
   */
  private String roleFile = "";

  /** Имя переменной окружения для поллинга роли (по умолчанию {@code COORDINATION_DEPLOYMENT_ROLE}). */
  private String roleEnvVar = "COORDINATION_DEPLOYMENT_ROLE";

  public DeploymentRole getRole() {
    return role;
  }

  public void setRole(DeploymentRole role) {
    this.role = role != null ? role : DeploymentRole.ACTIVE;
  }

  public String getRoleFile() {
    return roleFile;
  }

  public void setRoleFile(String roleFile) {
    this.roleFile = roleFile != null ? roleFile : "";
  }

  public String getRoleEnvVar() {
    return roleEnvVar;
  }

  public void setRoleEnvVar(String roleEnvVar) {
    this.roleEnvVar =
        (roleEnvVar == null || roleEnvVar.isBlank()) ? "COORDINATION_DEPLOYMENT_ROLE" : roleEnvVar.trim();
  }
}
