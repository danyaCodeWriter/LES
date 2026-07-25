package com.acme.coordination.security;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.config.CoordinationProperties;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SreTokenVerifierTest {

  private static SreTokenVerifier verifier(String token) {
    var props =
        new CoordinationProperties(
            CoordinationProperties.Etcd.plain(List.of("http://127.0.0.1:2379"), "/coord", Duration.ofSeconds(1), Duration.ofSeconds(1)),
            "",
            new CoordinationProperties.ActiveRegion("r1", "r1", "ACTIVE", 5000L),
            new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
            new CoordinationProperties.Sre(token == null ? "" : token),
            new CoordinationProperties.Limits(10_000),
            null);
    return new SreTokenVerifier(props);
  }

  @Test
  void matchesConfiguredToken() {
    Assertions.assertTrue(verifier("secret").matches("secret"));
    Assertions.assertFalse(verifier("secret").matches("other"));
    Assertions.assertFalse(verifier("").matches("secret"));
  }

  @Test
  void assertSre_rejectsMissingAndWrong() {
    Assertions.assertThrows(ParticipantException.class, () -> verifier("").assertSre("x"));
    Assertions.assertThrows(ParticipantException.class, () -> verifier("secret").assertSre(""));
    Assertions.assertThrows(ParticipantException.class, () -> verifier("secret").assertSre("nope"));
    Assertions.assertDoesNotThrow(() -> verifier("secret").assertSre("secret"));
  }
}
