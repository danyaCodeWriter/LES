package com.acme.coordination.deployment;

/**
 * Текущая {@link DeploymentRole} инстанса; может меняться во время работы JVM (поллинг файла / env).
 */
@FunctionalInterface
public interface DeploymentRoleSource {

  DeploymentRole currentRole();
}
