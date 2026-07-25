package com.acme.coordination.etcd;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeyspaceClusterActiveRegionTest {

  @Test
  void clusterActiveRegionKey_perHomeFromTenantsRoot() {
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    Assertions.assertEquals(
        "/coord/v1/cluster/regions/r1/activeRegion", ks.clusterActiveRegionKey("r1"));
    Assertions.assertEquals(
        "/coord/v1/cluster/regions/r2/activeRegion", ks.clusterActiveRegionKey("r2"));
  }

  @Test
  void clusterActiveRegionKey_fallbackParent() {
    Assertions.assertEquals(
        "/foo/cluster/regions/r1/activeRegion", new Keyspace("/foo/bar").clusterActiveRegionKey("r1"));
  }
}
