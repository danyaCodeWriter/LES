package com.acme.coordination;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.acme.coordination.api.DrainController;
import com.acme.coordination.api.error.ResourceConflictExceptionHandler;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.security.SreTokenVerifier;
import com.acme.coordination.service.DrainService;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * HTTP auth contract for Drain (no etcd / full Spring). Full drain+etcd in {@link EtcdIntegrationTest}.
 */
class DrainAuthWebE2eTest {

  private static final String SRE = "e2e-sre-drain-token";

  private WebTestClient web;
  private DrainService drain;

  @BeforeEach
  void setUp() {
    drain = mock(DrainService.class);
    CoordinationProperties props =
        new CoordinationProperties(
            CoordinationProperties.Etcd.plain(
                List.of("http://127.0.0.1:2379"),
                "/coord/v1/tenants",
                Duration.ofSeconds(3),
                Duration.ofSeconds(3)),
            "",
            "r1",
            new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
            new CoordinationProperties.Sre(SRE),
            new CoordinationProperties.Limits(10_000));
    web =
        WebTestClient.bindToController(new DrainController(drain, new SreTokenVerifier(props)))
            .controllerAdvice(new ResourceConflictExceptionHandler())
            .build();
  }

  @Test
  void drainRequiresSreToken() {
    web.post()
        .uri("/v1/tenants/e2e-drain/regions/r1:drain")
        .exchange()
        .expectStatus()
        .isUnauthorized();

    web.post()
        .uri("/v1/tenants/e2e-drain/regions/r1:drain")
        .header("Authorization", "Bearer wrong")
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  @Test
  void drainWithSreSucceeds() {
    when(drain.drainRegional(anyString(), anyString()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new DrainService.DrainReport("e2e-drain", "r1", 2, List.of("a", "b"))));

    web.post()
        .uri("/v1/tenants/e2e-drain/regions/r1:drain")
        .header("Authorization", "Bearer " + SRE)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.deletedCount")
        .isEqualTo(2)
        .jsonPath("$.scope")
        .isEqualTo("r1");
  }
}
