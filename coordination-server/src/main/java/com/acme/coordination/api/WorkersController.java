package com.acme.coordination.api;

import com.acme.coordination.api.dto.WorkerHeartbeatRequest;
import com.acme.coordination.api.dto.WorkerRegisterRequest;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.WorkerReadVerificationService;
import com.acme.coordination.service.WorkerRegistryService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/tenants/{tenantId}/workers")
public class WorkersController {

  private final WorkerRegistryService workers;
  private final CoordinatorReadVerificationService readAuth;
  private final WorkerReadVerificationService workerRead;
  private final JobLockService jobs;
  private final ActiveRegionMonitor activeRegion;

  public WorkersController(
      WorkerRegistryService workers,
      CoordinatorReadVerificationService readAuth,
      WorkerReadVerificationService workerRead,
      JobLockService jobs,
      ActiveRegionMonitor activeRegion) {
    this.workers = workers;
    this.readAuth = readAuth;
    this.workerRead = workerRead;
    this.jobs = jobs;
    this.activeRegion = activeRegion;
  }

  /** Все {@code workerId} в текущем active-region пуле (удобно для {@code POST /v1/rings/assign}). */
  @GetMapping("/ids")
  public CompletableFuture<List<String>> listIds(@PathVariable String tenantId, ServerHttpRequest request) {
    return readAuth
        .verifyRegional(request, tenantId)
        .thenCompose(v -> workers.listWorkerIds(tenantId, activeRegion.activeRegionId()));
  }

  @GetMapping("/{workerId}/jobs")
  public CompletableFuture<List<JobLockService.JobState>> listJobsHeld(
      @PathVariable String tenantId,
      @PathVariable String workerId,
      ServerHttpRequest request
  ) {
    return workerRead
        .verifyRegional(request, tenantId, activeRegion.activeRegionId(), workerId)
        .thenCompose(
            v ->
                jobs
                    .listJobsForHolder(tenantId, activeRegion.activeRegionId(), workerId)
                    .thenApply(list -> list.stream().map(ReadResponseSanitizer::strip).toList()));
  }

  @PostMapping("/register")
  public CompletableFuture<WorkerRegistryService.RegisterResult> register(
      @PathVariable String tenantId,
      @Valid @RequestBody WorkerRegisterRequest req
  ) {
    Map<String, String> md = req.metadata() != null ? req.metadata() : Map.of();
    return workers
        .register(tenantId, activeRegion.activeRegionId(), req.workerId(), req.instanceId(), req.ttlSeconds(), md)
        .thenApply(ApiConflicts::requireNewWorker);
  }

  @PostMapping("/{workerId}/heartbeat")
  public CompletableFuture<Map<String, Object>> heartbeat(
      @PathVariable String tenantId,
      @PathVariable String workerId,
      @Valid @RequestBody WorkerHeartbeatRequest req
  ) {
    return workers.heartbeat(tenantId, activeRegion.activeRegionId(), workerId, req.token()).thenApply(ok -> Map.of("ok", ok));
  }

  @PostMapping("/{workerId}/deregister")
  public CompletableFuture<Map<String, Object>> deregister(
      @PathVariable String tenantId,
      @PathVariable String workerId,
      @Valid @RequestBody WorkerHeartbeatRequest req
  ) {
    return workers.deregisterIfTokenMatches(tenantId, activeRegion.activeRegionId(), workerId, req.token())
        .thenApply(ok -> Map.of("deleted", ok));
  }

  @GetMapping
  public CompletableFuture<List<WorkerRegistryService.WorkerState>> list(
      @PathVariable String tenantId, ServerHttpRequest request) {
    return readAuth
        .verifyRegional(request, tenantId)
        .thenCompose(
            v ->
                workers
                    .listWorkers(tenantId, activeRegion.activeRegionId())
                    .thenApply(ReadResponseSanitizer::stripWorkers));
  }
}
