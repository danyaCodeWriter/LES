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
  }

  @Test
  void standbyBlocksRegionalPost() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/coordinator/acquire", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/workers/register", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/jobs/x:acquire", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/jobs/x:renew", HttpMethod.POST));
  }

  @Test
  void standbyAllowsGlobalPost() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/global/coordinator/acquire", HttpMethod.POST));
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/global/workers/register", HttpMethod.POST));
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/global/jobs/x:renew", HttpMethod.POST));
  }

  @Test
  void standbyAllowsDiagnosticsGet() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/active-region", HttpMethod.GET));
    Assertions.assertFalse(policy.shouldBlockRest("/v1/tenants/t/stream/active-region", HttpMethod.GET));
  }

  @Test
  void standbyBlocksWorkerJobStreams() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/stream/workers", HttpMethod.GET));
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/stream/jobs", HttpMethod.GET));
  }

  @Test
  void standbyBlocksTenantKeysStream() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/stream/keys", HttpMethod.GET));
  }

  @Test
  void standbyGrpcBlocksRegionalMutations() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertTrue(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireCoordinator"));
    Assertions.assertTrue(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/RenewJob"));
    Assertions.assertFalse(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/GetCoordinator"));
    Assertions.assertFalse(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireGlobalCoordinator"));
    Assertions.assertFalse(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/RenewGlobalJob"));
  }

  @Test
  void switchBlocksRegionalLikeStandby() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.SWITCH);
    Assertions.assertTrue(policy.shouldBlockRest("/v1/tenants/t/coordinator/acquire", HttpMethod.POST));
    Assertions.assertTrue(policy.shouldBlockGrpcMutation("coordination.v1.Coordination/AcquireCoordinator"));
    Assertions.assertTrue(policy.participatesInRegionalDrain());
    Assertions.assertEquals("switch", policy.instanceStatusWire());
  }

  @Test
  void standbyDoesNotParticipateInRegionalDrain() {
    var policy = new DeploymentRegistrationPolicy(() -> DeploymentRole.STANDBY);
    Assertions.assertFalse(policy.participatesInRegionalDrain());
  }

  @Test
  void normalizePathTrimsQueryAndSlash() {
    Assertions.assertEquals("/v1/tenants/t", DeploymentRegistrationPolicy.normalizePath("/v1/tenants/t/?x=1"));
    Assertions.assertEquals("/v1/tenants/t", DeploymentRegistrationPolicy.normalizePath("/v1/tenants/t/"));
  }
}
