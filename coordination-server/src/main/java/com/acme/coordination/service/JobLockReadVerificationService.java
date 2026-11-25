package com.acme.coordination.service;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.model.WorkerRecord;
import com.acme.coordination.region.RegionalScope;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;

/**
 * GET a single job lock: coordinator read token <strong>or</strong> the register token of the worker that holds the
 * lock ({@link JobLockService.JobState#record()} {@code holder}).
 */
@Service
public class JobLockReadVerificationService {

  private final CoordinatorService coordinators;
  private final RegionalScope regionalScope;
  private final JobLockService jobs;
  private final WorkerRegistryService workers;

  public JobLockReadVerificationService(
      CoordinatorService coordinators,
      RegionalScope regionalScope,
      JobLockService jobs,
      WorkerRegistryService workers) {
    this.coordinators = coordinators;
    this.regionalScope = regionalScope;
    this.jobs = jobs;
    this.workers = workers;
  }

  /** One etcd read for the job + authorization; response body uses this snapshot. */
  public CompletableFuture<Optional<JobLockService.JobState>> readRegionalJob(
      ServerHttpRequest request, String tenantId, String jobId) {
    List<String> coord = dedupeNonBlank(List.of(header(request, "X-Coordinator-Token"), bearer(request)));
    List<String> workerTok = dedupeNonBlank(List.of(header(request, "X-Worker-Token"), bearer(request)));
    return loadAndAuthorize(
        coordinators.get(tenantId, regionalScope.regionId()),
        jobs.get(tenantId, regionalScope.regionId(), jobId),
        tenantId,
        regionalScope.regionId(),
        false,
        coord,
        workerTok);
  }

  public CompletableFuture<Optional<JobLockService.JobState>> readGlobalJob(
      ServerHttpRequest request, String tenantId, String jobId) {
    List<String> coord = dedupeNonBlank(List.of(header(request, "X-Coordinator-Token"), bearer(request)));
    List<String> workerTok = dedupeNonBlank(List.of(header(request, "X-Worker-Token"), bearer(request)));
    return loadAndAuthorize(
        coordinators.getGlobal(tenantId),
        jobs.getGlobal(tenantId, jobId),
        tenantId,
        null,
        true,
        coord,
        workerTok);
  }

  public CompletableFuture<Optional<JobLockService.JobState>> readRegionalJobGrpc(
      String tenantId, String jobId, String coordinatorReadToken, String workerRegisterToken) {
    List<String> coord = dedupeNonBlank(List.of(trimToEmpty(coordinatorReadToken)));
    List<String> workerTok = dedupeNonBlank(List.of(trimToEmpty(workerRegisterToken)));
    return loadAndAuthorize(
        coordinators.get(tenantId, regionalScope.regionId()),
        jobs.get(tenantId, regionalScope.regionId(), jobId),
        tenantId,
        regionalScope.regionId(),
        false,
        coord,
        workerTok);
  }

  public CompletableFuture<Optional<JobLockService.JobState>> readGlobalJobGrpc(
      String tenantId, String jobId, String coordinatorReadToken, String workerRegisterToken) {
    List<String> coord = dedupeNonBlank(List.of(trimToEmpty(coordinatorReadToken)));
    List<String> workerTok = dedupeNonBlank(List.of(trimToEmpty(workerRegisterToken)));
    return loadAndAuthorize(
        coordinators.getGlobal(tenantId),
        jobs.getGlobal(tenantId, jobId),
        tenantId,
        null,
        true,
        coord,
        workerTok);
  }

  private CompletableFuture<Optional<JobLockService.JobState>> loadAndAuthorize(
      CompletableFuture<Optional<CoordinatorService.CoordinatorState>> leaderFut,
      CompletableFuture<Optional<JobLockService.JobState>> jobFut,
      String tenantId,
      String regionId,
      boolean global,
      List<String> coordinatorCandidates,
      List<String> workerCandidates
  ) {
    return leaderFut.thenCompose(
        leaderOpt ->
            jobFut.thenCompose(
                jobOpt -> {
                  if (coordinatorCandidates.isEmpty() && workerCandidates.isEmpty()) {
                    return CompletableFuture.failedFuture(jobReadTokenRequired());
                  }
                  for (String c : coordinatorCandidates) {
                    if (matchesLeader(leaderOpt, c)) {
                      return CompletableFuture.completedFuture(jobOpt);
                    }
                  }
                  return verifyHolderWorkerTokens(tenantId, regionId, global, jobOpt, workerCandidates)
                      .thenApply(v -> jobOpt);
                }));
  }

  private static boolean matchesLeader(Optional<CoordinatorService.CoordinatorState> leaderOpt, String secret) {
    if (secret == null || secret.isBlank()) {
      return false;
    }
    return leaderOpt
        .map(s -> s.record().token())
        .filter(t -> t != null && !t.isBlank() && t.equals(secret))
        .isPresent();
  }

  private CompletableFuture<Void> verifyHolderWorkerTokens(
      String tenantId,
      String regionId,
      boolean global,
      Optional<JobLockService.JobState> jobOpt,
      List<String> workerCandidates
  ) {
    if (workerCandidates.isEmpty()) {
      return CompletableFuture.failedFuture(jobReadForbidden());
    }
    if (jobOpt.isEmpty()) {
      return CompletableFuture.failedFuture(jobReadForbidden());
    }
    String holder = jobOpt.get().record().holder();
    if (holder == null || holder.isBlank()) {
      return CompletableFuture.failedFuture(jobReadForbidden());
    }
    CompletableFuture<Optional<WorkerRecord>> wf =
        global ? workers.getWorkerGlobal(tenantId, holder) : workers.getWorker(tenantId, regionId, holder);
    return wf.thenAccept(
        wOpt -> {
          String regTok = wOpt.map(WorkerRecord::token).filter(t -> t != null && !t.isBlank()).orElse("");
          boolean ok = workerCandidates.stream().anyMatch(s -> !s.isBlank() && s.equals(regTok));
          if (!ok) {
            throw jobReadForbidden();
          }
        });
  }

  private static ParticipantException jobReadTokenRequired() {
    return new ParticipantException(
        HttpStatus.UNAUTHORIZED,
        "JOB_READ_TOKEN_REQUIRED",
        "Send coordinator read token (Authorization: Bearer or X-Coordinator-Token) or job holder worker token (Authorization: Bearer or X-Worker-Token from POST .../workers/register in this scope).");
  }

  private static ParticipantException jobReadForbidden() {
    return new ParticipantException(
        HttpStatus.FORBIDDEN,
        "JOB_READ_FORBIDDEN",
        "Coordinator read token does not match the active leader, or worker token is not the register token of the job holder.");
  }

  private static String bearer(ServerHttpRequest req) {
    String auth = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (auth == null) {
      return "";
    }
    String a = auth.trim();
    if (a.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
      return a.substring("Bearer ".length()).trim();
    }
    return "";
  }

  private static String header(ServerHttpRequest req, String name) {
    String v = req.getHeaders().getFirst(name);
    return v == null ? "" : v.trim();
  }

  private static String trimToEmpty(String s) {
    return s == null ? "" : s.trim();
  }

  private static List<String> dedupeNonBlank(List<String> parts) {
    Set<String> seen = new LinkedHashSet<>();
    List<String> out = new ArrayList<>();
    for (String p : parts) {
      if (p == null || p.isBlank()) {
        continue;
      }
      if (seen.add(p)) {
        out.add(p);
      }
    }
    return out;
  }
}
