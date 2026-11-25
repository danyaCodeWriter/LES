package com.acme.coordination.api;

import com.acme.coordination.security.SreTokenVerifier;
import com.acme.coordination.service.DrainService;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Force-clear regional (or global lease) keys. Auth: SRE stream-listen token. Les server does not
 * call RSS; clients trigger drain after an RSS active-region flip.
 */
@RestController
@RequestMapping("/v1/tenants/{tenantId}")
public class DrainController {

  private final DrainService drain;
  private final SreTokenVerifier sre;

  public DrainController(DrainService drain, SreTokenVerifier sre) {
    this.drain = drain;
    this.sre = sre;
  }

  @PostMapping("/regions/{regionId}:drain")
  public CompletableFuture<Map<String, Object>> drainRegional(
      @PathVariable String tenantId,
      @PathVariable String regionId,
      ServerHttpRequest request) {
    sre.assertSreFromRequest(request);
    return drain.drainRegional(tenantId, regionId).thenApply(DrainService.DrainReport::toMap);
  }

  @PostMapping("/global:drain")
  public CompletableFuture<Map<String, Object>> drainGlobal(
      @PathVariable String tenantId, ServerHttpRequest request) {
    sre.assertSreFromRequest(request);
    return drain.drainGlobalLeases(tenantId).thenApply(DrainService.DrainReport::toMap);
  }
}
