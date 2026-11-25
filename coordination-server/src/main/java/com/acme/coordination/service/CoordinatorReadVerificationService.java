package com.acme.coordination.service;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.region.RegionalScope;
import com.acme.coordination.security.SreTokenVerifier;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;

/**
 * Regional/global GET (and SSE for workers/jobs) require proof of the coordinator lease: same secret as
 * {@code coordinator/renew} — {@code Authorization: Bearer &lt;token&gt;} or header {@code X-Coordinator-Token}.
 * <p>Опционально для SSE {@code stream/workers|jobs|keys} (regional и global) настроен
 * {@link com.acme.coordination.config.CoordinationProperties.Sre#streamListenToken()}: тот же формат заголовков,
 * но значение статическое из конфигурации.
 * <p>GET a <strong>single</strong> job by id is handled by {@link JobLockReadVerificationService} and also accepts the
 * holder worker's register token.
 */
@Service
public class CoordinatorReadVerificationService {

  private final CoordinatorService coordinators;
  private final RegionalScope regionalScope;
  private final SreTokenVerifier sre;

  public CoordinatorReadVerificationService(
      CoordinatorService coordinators, RegionalScope regionalScope, SreTokenVerifier sre) {
    this.coordinators = coordinators;
    this.regionalScope = regionalScope;
    this.sre = sre;
  }

  public CompletableFuture<Void> verifyRegional(ServerHttpRequest request, String tenantId) {
    return verifyAgainstLeader(coordinators.get(tenantId, regionalScope.regionId()), extractCoordReadSecret(request));
  }

  public CompletableFuture<Void> verifyGlobal(ServerHttpRequest request, String tenantId) {
    return verifyAgainstLeader(coordinators.getGlobal(tenantId), extractCoordReadSecret(request));
  }

  /**
   * SSE {@code .../stream/workers|jobs} (this region): coordinator read token <strong>или</strong> настроенный
   * {@code coordination.sre.stream-listen-token} (если не пустой).
   */
  public CompletableFuture<Void> verifyRegionalStream(ServerHttpRequest request, String tenantId) {
    String secret = extractCoordReadSecret(request);
    if (sre.matches(secret)) {
      return CompletableFuture.completedFuture(null);
    }
    return verifyRegional(request, tenantId);
  }

  /**
   * SSE {@code .../global/stream/workers|jobs}: coordinator global read token <strong>или</strong> тот же SRE
   * {@code coordination.sre.stream-listen-token} (один секрет на оба контура).
   */
  public CompletableFuture<Void> verifyGlobalStream(ServerHttpRequest request, String tenantId) {
    String secret = extractCoordReadSecret(request);
    if (sre.matches(secret)) {
      return CompletableFuture.completedFuture(null);
    }
    return verifyGlobal(request, tenantId);
  }

  /**
   * SSE {@code .../stream/keys}: объединённый watch по {@code regions/} и {@code global/} tenant. Допускается только
   * токен **global** coordinator или SRE {@code coordination.sre.stream-listen-token} (не regional leader — иначе
   * утечка global-ключей через regional токен).
   */
  public CompletableFuture<Void> verifyTenantKeysStream(ServerHttpRequest request, String tenantId) {
    String secret = extractCoordReadSecret(request);
    if (sre.matches(secret)) {
      return CompletableFuture.completedFuture(null);
    }
    return verifyGlobal(request, tenantId);
  }

  public CompletableFuture<Void> verifyTenantKeysStreamGrpc(String tenantId, String accessCredential) {
    String secret = grpcReadSecret(accessCredential);
    if (sre.matches(secret)) {
      return CompletableFuture.completedFuture(null);
    }
    return verifyGlobalGrpc(tenantId, secret);
  }

  public CompletableFuture<Void> verifyRegionalGrpc(String tenantId, String coordinatorReadToken) {
    return verifyAgainstLeader(coordinators.get(tenantId, regionalScope.regionId()), coordinatorReadToken);
  }

  public CompletableFuture<Void> verifyGlobalGrpc(String tenantId, String coordinatorReadToken) {
    return verifyAgainstLeader(coordinators.getGlobal(tenantId), coordinatorReadToken);
  }

  private static CompletableFuture<Void> verifyAgainstLeader(
      CompletableFuture<Optional<CoordinatorService.CoordinatorState>> leaderFut, String token
  ) {
    if (token == null || token.isBlank()) {
      return CompletableFuture.failedFuture(
          new ParticipantException(
              HttpStatus.UNAUTHORIZED,
              "COORDINATOR_READ_TOKEN_REQUIRED",
              "Send Authorization: Bearer <coordinator acquire token> or X-Coordinator-Token (same as coordinator renew)."));
    }
    return leaderFut.thenCompose(
        opt -> {
          if (opt.isEmpty()) {
            return CompletableFuture.failedFuture(
                new ParticipantException(
                    HttpStatus.FORBIDDEN,
                    "COORDINATOR_READ_NO_LEADER",
                    "No active coordinator in this scope; nothing to authorize reads against."));
          }
          String leaderTok = opt.get().record().token();
          if (leaderTok == null || leaderTok.isBlank() || !leaderTok.equals(token)) {
            return CompletableFuture.failedFuture(
                new ParticipantException(
                    HttpStatus.FORBIDDEN,
                    "COORDINATOR_READ_FORBIDDEN",
                    "Coordinator read token does not match the active leader token."));
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  public static String extractCoordReadSecret(ServerHttpRequest req) {
    String auth = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (auth != null) {
      String a = auth.trim();
      if (a.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
        return a.substring("Bearer ".length()).trim();
      }
    }
    String x = req.getHeaders().getFirst("X-Coordinator-Token");
    return x == null ? "" : x.trim();
  }

  public static String grpcReadSecret(String coordinatorReadToken) {
    return coordinatorReadToken == null ? "" : coordinatorReadToken.trim();
  }
}
