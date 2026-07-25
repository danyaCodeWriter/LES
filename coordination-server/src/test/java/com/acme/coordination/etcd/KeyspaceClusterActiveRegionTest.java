package com.acme.coordination.etcd;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeyspaceClusterActiveRegionTest {

  @Test
  void clusterActiveRegionKey_fromTenantsRoot() {
    Assertions.assertEquals(
        "/coord/v1/cluster/activeRegion", new Keyspace("/coord/v1/tenants").clusterActiveRegionKey());
  }

  @Test
  void clusterActiveRegionKey_fallbackParent() {
    Assertions.assertEquals("/foo/cluster/activeRegion", new Keyspace("/foo/bar").clusterActiveRegionKey());
  }
}
