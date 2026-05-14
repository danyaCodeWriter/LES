package com.acme.coordination.deployment;

/**
 * Роль инстанса в multi-DC: только {@link #ACTIVE} принимает regional / active-pocket мутации;
 * {@link #STANDBY} (stand-in / пассивный ЦОД) — только global и часть read-only диагностики;
 * {@link #SWITCH} — переход: regional для клиентов закрыт (как standby), но инстанс участвует в {@code REGION_DRAIN}
 * и при смене logical region может ускоренно удалять старые lease-ключи coordinator/worker/job (см. {@code RegionSwitchDrainPolicy}).
 */
public enum DeploymentRole {
  ACTIVE,
  STANDBY,
  SWITCH
}
