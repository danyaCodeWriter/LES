package com.acme.coordination.api;

import com.acme.coordination.api.dto.CoordinatorAcquireRequest;
import com.acme.coordination.api.dto.CoordinatorRenewRequest;
import com.acme.coordination.region.RegionalScope;
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
 * Regional coordinator for this LES ({@code coordination.region} via {@link RegionalScope}).
 * Cross-DC singleton: {@code /v1/tenants/{tenant}/global/coordinator}.
 */
@RestController
@RequestMapping("/v1/tenants/{tenantId}/coordinator")
public class CoordinatorController {

  private final CoordinatorService coordinator;
  private final CoordinatorReadVerificationService readAuth;
  private final RegionalScope regionalScope;

  public CoordinatorController(
      CoordinatorService coordinator, CoordinatorReadVerificationService readAuth, RegionalScope regionalScope) {
    this.coordinator = coordinator;
    this.readAuth = readAuth;
    this.regionalScope = regionalScope;
  }

  @PostMapping("/acquire")
  public CompletableFuture<CoordinatorService.AcquireResult> acquire(
      @PathVariable String tenantId,
      @Valid @RequestBody CoordinatorAcquireRequest req
  ) {
    Map<String, String> md = req.metadata() != null ? req.metadata() : Map.of();
    return coordinator
        .acquire(tenantId, regionalScope.regionId(), req.coordinatorId(), req.instanceId(), req.ttlSeconds(), md)
        .thenApply(ApiConflicts::requireNewCoordinator);
  }

  /** Current coordinator in this instance's configured region. */
  @GetMapping
  public CompletableFuture<Object> get(@PathVariable String tenantId, ServerHttpRequest request) {
    return readAuth
        .verifyRegional(request, tenantId)
        .thenCompose(
            v ->
                coordinator
                    .get(tenantId, regionalScope.regionId())
                    .thenApply(opt -> opt.<Object>map(ReadResponseSanitizer::strip).orElse(null)));
  }

  /** Same as {@code GET /coordinator} — explicit alias. */
  @GetMapping("/active")
  public CompletableFuture<Object> getActive(@PathVariable String tenantId, ServerHttpRequest request) {
    return get(tenantId, request);
  }

  @PostMapping("/renew")
  public CompletableFuture<Map<String, Object>> renew(
      @PathVariable String tenantId,
      @Valid @RequestBody CoordinatorRenewRequest req
  ) {
    return coordinator.renewIfHolder(tenantId, regionalScope.regionId(), req.token())
        .thenApply(ok -> Map.of("renewed", ok));
  }

  @PostMapping("/release")
  public CompletableFuture<Map<String, Object>> release(
      @PathVariable String tenantId, @Valid @RequestBody CoordinatorRenewRequest req) {
    return coordinator
        .releaseIfHolder(tenantId, regionalScope.regionId(), req.token())
        .thenApply(ok -> Map.of("released", ok));
  }
}
