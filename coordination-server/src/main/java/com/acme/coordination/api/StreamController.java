package com.acme.coordination.api;

import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.region.ActiveRegionMonitor;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import com.acme.coordination.watch.EtcdWatchService;
import com.acme.coordination.watch.WatchEventDto;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.codec.ServerSentEvent;
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
  private final ActiveRegionMonitor activeRegion;
  private final CoordinatorReadVerificationService readAuth;

  public StreamController(
      EtcdWatchService watch,
      Keyspace keyspace,
      ActiveRegionMonitor activeRegion,
      CoordinatorReadVerificationService readAuth) {
    this.watch = watch;
    this.keyspace = keyspace;
    this.activeRegion = activeRegion;
    this.readAuth = readAuth;
  }

  @GetMapping(value = "/workers", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> workers(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyRegionalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.workersPrefix(tenantId, activeRegion.activeRegionId())));
  }

  @GetMapping(value = "/jobs", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> jobs(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyRegionalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.jobsPrefix(tenantId, activeRegion.activeRegionId())));
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

  /** When active region changes, clients should re-register coordinator/worker under the new region. */
  @GetMapping(value = "/active-region", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> activeRegion(@PathVariable String tenantId) {
    return activeRegion.activeRegionEvents()
        .map(data -> ServerSentEvent.<String>builder().data(data).build());
  }
}
