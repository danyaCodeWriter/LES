package com.acme.coordination.api;

import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.region.RegionalScope;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import com.acme.coordination.watch.EtcdWatchService;
import com.acme.coordination.watch.WatchEventDto;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/v1/tenants/{tenantId}/stream")
public class StreamController {

  private final EtcdWatchService watch;
  private final Keyspace keyspace;
  private final RegionalScope regionalScope;
  private final CoordinatorReadVerificationService readAuth;

  public StreamController(
      EtcdWatchService watch,
      Keyspace keyspace,
      RegionalScope regionalScope,
      CoordinatorReadVerificationService readAuth) {
    this.watch = watch;
    this.keyspace = keyspace;
    this.regionalScope = regionalScope;
    this.readAuth = readAuth;
  }

  @GetMapping(value = "/workers", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> workers(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyRegionalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.workersPrefix(tenantId, regionalScope.regionId())));
  }

  @GetMapping(value = "/jobs", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> jobs(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyRegionalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.jobsPrefix(tenantId, regionalScope.regionId())));
  }

  /**
   * SSE — изменения всех coordination-ключей tenant (префиксы {@code .../regions/} и {@code .../global/}; без
   * {@code .../idempotency/}). Авторизация: global coordinator read token или SRE {@code coordination.sre.stream-listen-token}.
   */
  @GetMapping(value = "/keys", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> tenantKeys(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyTenantKeysStream(request, tenantId))
        .thenMany(
            Flux.merge(
                watch.watchPrefix(keyspace.regionsPrefix(tenantId)),
                watch.watchPrefix(keyspace.globalSubtreePrefix(tenantId))));
  }
}
