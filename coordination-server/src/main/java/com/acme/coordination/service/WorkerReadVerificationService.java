package com.acme.coordination.service;

import com.acme.coordination.api.error.ParticipantException;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;

/**
 * Proves ownership of a {@code workerId} via the same secret as {@code POST .../workers/register} (Bearer or
 * {@code X-Worker-Token}).
 */
@Service
public class WorkerReadVerificationService {

  private final WorkerRegistryService workers;

  public WorkerReadVerificationService(WorkerRegistryService workers) {
    this.workers = workers;
  }

  public static String extractWorkerToken(ServerHttpRequest req) {
    String auth = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (auth != null) {
      String a = auth.trim();
      if (a.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
        return a.substring("Bearer ".length()).trim();
      }
    }
    String x = req.getHeaders().getFirst("X-Worker-Token");
    return x == null ? "" : x.trim();
  }

  public CompletableFuture<Void> verifyRegional(
      ServerHttpRequest request, String tenantId, String regionId, String workerId
  ) {
    String tok = extractWorkerToken(request);
    if (tok.isBlank()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(
              HttpStatus.UNAUTHORIZED,
              "WORKER_TOKEN_REQUIRED",
              "Send Authorization: Bearer <worker register token> or X-Worker-Token."));
    }
    return workers
        .getWorker(tenantId, regionId, workerId)
        .thenAccept(
            wOpt -> {
              if (wOpt.isEmpty() || !tok.equals(wOpt.get().token())) {
                throw new ParticipantException(
                    HttpStatus.FORBIDDEN,
                    "WORKER_TOKEN_INVALID",
                    "workerId is unknown or token does not match the active registration for this region.");
              }
            });
  }

  public CompletableFuture<Void> verifyGlobal(ServerHttpRequest request, String tenantId, String workerId) {
    String tok = extractWorkerToken(request);
    if (tok.isBlank()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(
              HttpStatus.UNAUTHORIZED,
              "WORKER_TOKEN_REQUIRED",
              "Send Authorization: Bearer <worker register token> or X-Worker-Token."));
    }
    return workers
        .getWorkerGlobal(tenantId, workerId)
        .thenAccept(
            wOpt -> {
              if (wOpt.isEmpty() || !tok.equals(wOpt.get().token())) {
                throw new ParticipantException(
                    HttpStatus.FORBIDDEN,
                    "WORKER_TOKEN_INVALID",
                    "workerId is unknown or token does not match the active global registration.");
              }
            });
  }
}
