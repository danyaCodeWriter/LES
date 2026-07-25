package com.acme.coordination.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.model.CoordinatorRecord;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.security.SreTokenVerifier;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

@ExtendWith(MockitoExtension.class)
class CoordinatorReadVerificationServiceSreStreamTest {

  @Mock private CoordinatorService coordinators;
  @Mock private ActiveRegionMonitor activeRegion;

  private static CoordinationProperties props(String sreToken) {
    return new CoordinationProperties(
        CoordinationProperties.Etcd.plain(List.of("http://127.0.0.1:2379"), "/coord", Duration.ofSeconds(1), Duration.ofSeconds(1)),
        "",
        new CoordinationProperties.ActiveRegion("r1", "ACTIVE", 5000L),
        new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
        new CoordinationProperties.Sre(sreToken == null ? "" : sreToken),
        new CoordinationProperties.Limits(10_000),
        null);
  }

  private static CoordinatorReadVerificationService svc(
      CoordinatorService coordinators, ActiveRegionMonitor activeRegion, String sreToken) {
    return new CoordinatorReadVerificationService(coordinators, activeRegion, new SreTokenVerifier(props(sreToken)));
  }

  @Test
  void regionalStream_sreTokenDoesNotTouchCoordinatorService() {
    var service = svc(coordinators, activeRegion, "sre-secret");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/stream")
            .header(HttpHeaders.AUTHORIZATION, "Bearer sre-secret")
            .build();
    assertDoesNotThrow(() -> service.verifyRegionalStream(req, "t").join());
    verify(coordinators, never()).get(anyString(), anyString());
  }

  @Test
  void regionalStream_xCoordinatorTokenAcceptsSre() {
    var service = svc(coordinators, activeRegion, "x");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/stream").header("X-Coordinator-Token", "x").build();
    assertDoesNotThrow(() -> service.verifyRegionalStream(req, "t").join());
    verify(coordinators, never()).get(anyString(), anyString());
  }

  @Test
  void globalStream_sreTokenDoesNotTouchGlobalCoordinator() {
    var service = svc(coordinators, activeRegion, "sre-secret");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/gstream")
            .header(HttpHeaders.AUTHORIZATION, "Bearer sre-secret")
            .build();
    assertDoesNotThrow(() -> service.verifyGlobalStream(req, "t").join());
    verify(coordinators, never()).getGlobal(anyString());
  }

  @Test
  void regionalStream_wrongSreFallsBackToCoordinatorPath() {
    when(activeRegion.activeRegionId()).thenReturn("r1");
    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    var service = svc(coordinators, activeRegion, "sre-secret");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/stream")
            .header(HttpHeaders.AUTHORIZATION, "Bearer not-sre")
            .build();
    assertThrows(ExecutionException.class, () -> service.verifyRegionalStream(req, "t").get());
    verify(coordinators).get("t", "r1");
  }

  @Test
  void regionalStream_disabledSreRequiresLeaderToken() {
    when(activeRegion.activeRegionId()).thenReturn("r1");
    CoordinatorService.CoordinatorState leader =
        new CoordinatorService.CoordinatorState(
            new CoordinatorRecord("c", "i", "ctok", Map.of(), Instant.now(), 1L), 1L, 2L);
    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.of(leader)));
    var service = svc(coordinators, activeRegion, "");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/stream")
            .header(HttpHeaders.AUTHORIZATION, "Bearer ctok")
            .build();
    assertDoesNotThrow(() -> service.verifyRegionalStream(req, "t").join());
  }

  @Test
  void regularRegionalGet_doesNotAcceptSreToken() {
    when(activeRegion.activeRegionId()).thenReturn("r1");
    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    var service = svc(coordinators, activeRegion, "sre-secret");
    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/workers")
            .header(HttpHeaders.AUTHORIZATION, "Bearer sre-secret")
            .build();
    assertThrows(ExecutionException.class, () -> service.verifyRegional(req, "t").get());
  }
}
