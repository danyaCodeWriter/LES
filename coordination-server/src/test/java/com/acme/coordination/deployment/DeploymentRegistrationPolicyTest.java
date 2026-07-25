package com.acme.coordination.deployment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;

class DeploymentRegistrationPolicyTest {

  @Test
  void activeNeverBlocks() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.ACTIVE);
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/coordinator/acquire", HttpMethod.POST));
    Assertions.assertFalse(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireCoordinator"));
    Assertions.assertEquals("active", policy.instanceStatusWire());
  }

  @Test
  void standbyBlocksRegionalPost() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/coordinator/acquire", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/workers/register", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/stream/keys", HttpMethod.GET));
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/global/coordinator/acquire", HttpMethod.POST));
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/active-region", HttpMethod.GET));
    Assertions.assertEquals("stand_in", policy.instanceStatusWire());
  }

  @Test
  void switchBlocksLikeStandbyAndParticipatesInDrain() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.SWITCH);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/coordinator/acquire", HttpMethod.POST));
    Assertions.assertTrue(policy.participatesInRegionalDrain());
    Assertions.assertEquals("switch", policy.instanceStatusWire());
  }

  @Test
  void standbyGrpcBlocksRegionalMutations() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireCoordinator"));
    Assertions.assertFalse(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireGlobalCoordinator"));
  }
}
