package com.acme.coordination.deployment;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DynamicDeploymentRoleSourceTest {

  @Test
  void parseRoleToken_acceptsSynonyms() {
    Assertions.assertEquals(Optional.of(DeploymentRole.ACTIVE), DynamicDeploymentRoleSource.parseRoleToken("active"));
    Assertions.assertEquals(Optional.of(DeploymentRole.STANDBY), DynamicDeploymentRoleSource.parseRoleToken("STANDBY"));
    Assertions.assertEquals(Optional.of(DeploymentRole.STANDBY), DynamicDeploymentRoleSource.parseRoleToken("stand-in"));
    Assertions.assertEquals(Optional.of(DeploymentRole.STANDBY), DynamicDeploymentRoleSource.parseRoleToken("stand_in"));
    Assertions.assertEquals(Optional.of(DeploymentRole.SWITCH), DynamicDeploymentRoleSource.parseRoleToken("SWITCH"));
    Assertions.assertEquals(Optional.of(DeploymentRole.SWITCH), DynamicDeploymentRoleSource.parseRoleToken("switch-over"));
    Assertions.assertEquals(Optional.of(DeploymentRole.SWITCH), DynamicDeploymentRoleSource.parseRoleToken("switch_over"));
    Assertions.assertTrue(DynamicDeploymentRoleSource.parseRoleToken("unknown").isEmpty());
  }
}
