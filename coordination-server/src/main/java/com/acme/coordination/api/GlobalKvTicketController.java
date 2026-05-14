package com.acme.coordination.api;

import com.acme.coordination.api.dto.KvPutRequest;
import com.acme.coordination.service.KvTicketService;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/tenants/{tenantId}/global/kv")
public class GlobalKvTicketController {

  private final KvTicketService kv;

  public GlobalKvTicketController(KvTicketService kv) {
    this.kv = kv;
  }

  @PutMapping("/{keyId}")
  public CompletableFuture<ResponseEntity<KvTicketService.PutResponse>> put(
      @PathVariable String tenantId,
      @PathVariable String keyId,
      @Valid @RequestBody KvPutRequest body,
      ServerHttpRequest request
  ) {
    String access = KvTicketService.extractAccessToken(request);
    return kv
        .put(tenantId, keyId, body.value(), body.ttlSeconds(), access)
        .thenApply(
            pr ->
                pr.created()
                    ? ResponseEntity.status(HttpStatus.CREATED).body(pr)
                    : ResponseEntity.ok(pr));
  }

  @GetMapping("/{keyId}")
  public CompletableFuture<Map<String, Object>> get(
      @PathVariable String tenantId, @PathVariable String keyId, ServerHttpRequest request) {
    String access = KvTicketService.extractAccessToken(request);
    return kv.get(tenantId, keyId, access)
        .thenApply(
            g -> {
              LinkedHashMap<String, Object> m = new LinkedHashMap<>();
              m.put("value", g.value());
              m.put("leaseId", g.leaseId());
              m.put("modRevision", g.modRevision());
              return m;
            });
  }

  @DeleteMapping("/{keyId}")
  public CompletableFuture<Map<String, Object>> delete(
      @PathVariable String tenantId, @PathVariable String keyId, ServerHttpRequest request) {
    String access = KvTicketService.extractAccessToken(request);
    return kv.delete(tenantId, keyId, access);
  }
}
