package com.acme.coordination.sdk.les;

import com.acme.coordination.sdk.session.ApiScope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExclusiveLockFreestandingTest {

  @Test
  void apiScopeDocumentsRegionalVsGlobal() {
    Assertions.assertEquals(2, ApiScope.values().length);
  }
}
