package com.acme.coordination.api;

import com.acme.coordination.api.dto.CoordinatorAcquireRequest;
import com.acme.coordination.api.dto.CoordinatorRenewRequest;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import com.acme.coordination.service.CoordinatorService;
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

/**
 * Single coordinator lease for the tenant (cross-DC); etcd keys under {@code .../tenants/{id}/global/}.
 */
@RestController
@RequestMapping("/v1/tenants/{tenantId}/global/coordinator")
public class GlobalCoordinatorController {

  private final CoordinatorService coordinator;
  private final CoordinatorReadVerificationService readAuth;

  public GlobalCoordinatorController(CoordinatorService coordinator, CoordinatorReadVerificationService readAuth) {
    this.coordinator = coordinator;
    this.readAuth = readAuth;
  }

  @PostMapping("/acquire")
  public CompletableFuture<CoordinatorService.AcquireResult> acquire(
      @PathVariable String tenantId,
      @Valid @RequestBody CoordinatorAcquireRequest req
  ) {
    Map<String, String> md = req.metadata() != null ? req.metadata() : Map.of();
    return coordinator
        .acquireGlobal(tenantId, req.coordinatorId(), req.instanceId(), req.ttlSeconds(), md)
        .thenApply(ApiConflicts::requireNewCoordinator);
  }

  @GetMapping
  public CompletableFuture<Object> get(@PathVariable String tenantId, ServerHttpRequest request) {
    return readAuth
        .verifyGlobal(request, tenantId)
        .thenCompose(
            v -> coordinator.getGlobal(tenantId).thenApply(opt -> opt.<Object>map(ReadResponseSanitizer::strip).orElse(null)));
  }

  @PostMapping("/renew")
  public CompletableFuture<Map<String, Object>> renew(
      @PathVariable String tenantId,
      @Valid @RequestBody CoordinatorRenewRequest req
  ) {
    return coordinator.renewGlobalIfHolder(tenantId, req.token())
        .thenApply(ok -> Map.of("renewed", ok));
  }
}
