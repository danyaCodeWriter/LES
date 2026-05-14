package com.acme.coordination.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.model.CoordinatorRecord;
import com.acme.coordination.model.JobRecord;
import com.acme.coordination.model.WorkerRecord;
import com.acme.coordination.region.ActiveRegionMonitor;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

@ExtendWith(MockitoExtension.class)
class JobLockReadVerificationServiceTest {

  @Mock private CoordinatorService coordinators;
  @Mock private ActiveRegionMonitor activeRegion;
  @Mock private JobLockService jobs;
  @Mock private WorkerRegistryService workers;

  @InjectMocks private JobLockReadVerificationService svc;

  @BeforeEach
  void region() {
    lenient().when(activeRegion.activeRegionId()).thenReturn("r1");
  }

  @Test
  void regionalJobGet_allowsCoordinatorToken() {
    CoordinatorService.CoordinatorState leader =
        new CoordinatorService.CoordinatorState(
            new CoordinatorRecord("c", "i", "ctok", Map.of(), Instant.now(), 1L), 1L, 2L);
    JobLockService.JobState job =
        new JobLockService.JobState(
            new JobRecord("j1", "w1", "jt", Map.of(), Instant.now()), 1L, 2L);

    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.of(leader)));
    when(jobs.get("t", "r1", "j1")).thenReturn(CompletableFuture.completedFuture(Optional.of(job)));

    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/v1/tenants/t/jobs/j1")
            .header(HttpHeaders.AUTHORIZATION, "Bearer ctok")
            .build();

    assertDoesNotThrow(() -> svc.readRegionalJob(req, "t", "j1").join());
  }

  @Test
  void regionalJobGet_allowsHolderWorkerRegisterToken() {
    CoordinatorService.CoordinatorState leader =
        new CoordinatorService.CoordinatorState(
            new CoordinatorRecord("c", "i", "ctok", Map.of(), Instant.now(), 1L), 1L, 2L);
    JobLockService.JobState job =
        new JobLockService.JobState(
            new JobRecord("j1", "w1", "jt", Map.of(), Instant.now()), 1L, 2L);
    WorkerRecord wrec = new WorkerRecord("w1", "inst", "wtok", Map.of(), Instant.now());

    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.of(leader)));
    when(jobs.get("t", "r1", "j1")).thenReturn(CompletableFuture.completedFuture(Optional.of(job)));
    when(workers.getWorker("t", "r1", "w1")).thenReturn(CompletableFuture.completedFuture(Optional.of(wrec)));

    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/v1/tenants/t/jobs/j1")
            .header(HttpHeaders.AUTHORIZATION, "Bearer wtok")
            .build();

    assertDoesNotThrow(() -> svc.readRegionalJob(req, "t", "j1").join());
  }

  @Test
  void regionalJobGet_rejectsWhenNoToken() {
    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(jobs.get("t", "r1", "j1")).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    ServerHttpRequest req = MockServerHttpRequest.get("http://localhost/v1/tenants/t/jobs/j1").build();

    ExecutionException ex =
        assertThrows(ExecutionException.class, () -> svc.readRegionalJob(req, "t", "j1").get());
    if (!(ex.getCause() instanceof ParticipantException)) {
      throw new AssertionError(ex.getCause());
    }
  }

  @Test
  void globalJobGet_allowsWorkerTokenViaXWorkerHeader() {
    CoordinatorService.CoordinatorState leader =
        new CoordinatorService.CoordinatorState(
            new CoordinatorRecord("c", "i", "ctok", Map.of(), Instant.now(), 1L), 1L, 2L);
    JobLockService.JobState job =
        new JobLockService.JobState(
            new JobRecord("j1", "w1", "jt", Map.of(), Instant.now()), 1L, 2L);
    WorkerRecord wrec = new WorkerRecord("w1", "inst", "wtok", Map.of(), Instant.now());

    when(coordinators.getGlobal("t")).thenReturn(CompletableFuture.completedFuture(Optional.of(leader)));
    when(jobs.getGlobal("t", "j1")).thenReturn(CompletableFuture.completedFuture(Optional.of(job)));
    when(workers.getWorkerGlobal("t", "w1")).thenReturn(CompletableFuture.completedFuture(Optional.of(wrec)));

    ServerHttpRequest req =
        MockServerHttpRequest.get("http://localhost/v1/tenants/t/global/jobs/j1")
            .header("X-Worker-Token", "wtok")
            .build();

    assertDoesNotThrow(() -> svc.readGlobalJob(req, "t", "j1").join());
  }

  @Test
  void grpcRegional_acceptsWorkerRegisterTokenField() {
    CoordinatorService.CoordinatorState leader =
        new CoordinatorService.CoordinatorState(
            new CoordinatorRecord("c", "i", "ctok", Map.of(), Instant.now(), 1L), 1L, 2L);
    JobLockService.JobState job =
        new JobLockService.JobState(
            new JobRecord("j1", "w1", "jt", Map.of(), Instant.now()), 1L, 2L);
    WorkerRecord wrec = new WorkerRecord("w1", "inst", "wtok", Map.of(), Instant.now());

    when(coordinators.get("t", "r1")).thenReturn(CompletableFuture.completedFuture(Optional.of(leader)));
    when(jobs.get("t", "r1", "j1")).thenReturn(CompletableFuture.completedFuture(Optional.of(job)));
    when(workers.getWorker("t", "r1", "w1")).thenReturn(CompletableFuture.completedFuture(Optional.of(wrec)));

    assertDoesNotThrow(() -> svc.readRegionalJobGrpc("t", "j1", "", "wtok").join());
  }
}
