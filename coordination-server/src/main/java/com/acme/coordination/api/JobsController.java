package com.acme.coordination.api;

import com.acme.coordination.api.dto.JobAcquireRequest;
import com.acme.coordination.api.dto.JobTokenRequest;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.service.JobLockReadVerificationService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.JobParticipantVerificationService;
import jakarta.validation.Valid;
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
@RequestMapping("/v1/tenants/{tenantId}/jobs")
public class JobsController {

  private final JobLockService jobs;
  private final JobParticipantVerificationService jobParticipants;
  private final JobLockReadVerificationService jobReadAuth;
  private final ActiveRegionMonitor activeRegion;

  public JobsController(
      JobLockService jobs,
      JobParticipantVerificationService jobParticipants,
      JobLockReadVerificationService jobReadAuth,
      ActiveRegionMonitor activeRegion) {
    this.jobs = jobs;
    this.jobParticipants = jobParticipants;
    this.jobReadAuth = jobReadAuth;
    this.activeRegion = activeRegion;
  }

  @PostMapping("/{jobId}:acquire")
  public CompletableFuture<JobLockService.AcquireJobResult> acquire(
      @PathVariable String tenantId,
      @PathVariable String jobId,
      @Valid @RequestBody JobAcquireRequest req
  ) {
    Map<String, String> md = req.metadata() != null ? req.metadata() : Map.of();
    return jobParticipants
        .verifyRegional(tenantId, activeRegion.activeRegionId(), req.holder(), req.participantToken())
        .thenCompose(
            v ->
                jobs.acquire(tenantId, activeRegion.activeRegionId(), jobId, req.holder(), req.ttlSeconds(), md))
        .thenApply(ApiConflicts::requireNewJobLock);
  }

  @PostMapping("/{jobId}:release")
  public CompletableFuture<Map<String, Object>> release(
      @PathVariable String tenantId,
      @PathVariable String jobId,
      @Valid @RequestBody JobTokenRequest req
  ) {
    return jobs
        .releaseIfCredential(tenantId, activeRegion.activeRegionId(), jobId, req.token(), null)
        .thenApply(ok -> Map.of("released", ok));
  }

  @PostMapping("/{jobId}:renew")
  public CompletableFuture<Map<String, Object>> renew(
      @PathVariable String tenantId,
      @PathVariable String jobId,
      @Valid @RequestBody JobTokenRequest req
  ) {
    return jobs
        .renewIfCredential(tenantId, activeRegion.activeRegionId(), jobId, req.token(), null)
        .thenApply(ok -> Map.of("renewed", ok));
  }

  @GetMapping("/{jobId}")
  public CompletableFuture<Object> get(
      @PathVariable String tenantId, @PathVariable String jobId, ServerHttpRequest request) {
    return jobReadAuth
        .readRegionalJob(request, tenantId, jobId)
        .thenApply(opt -> opt.<Object>map(ReadResponseSanitizer::strip).orElse(null));
  }
}
