package com.acme.coordination.api;

import com.acme.coordination.etcd.Keyspace;
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
@RequestMapping("/v1/tenants/{tenantId}/global/stream")
public class GlobalStreamController {

  private final EtcdWatchService watch;
  private final Keyspace keyspace;
  private final CoordinatorReadVerificationService readAuth;

  public GlobalStreamController(
      EtcdWatchService watch, Keyspace keyspace, CoordinatorReadVerificationService readAuth) {
    this.watch = watch;
    this.keyspace = keyspace;
    this.readAuth = readAuth;
  }

  @GetMapping(value = "/workers", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> workers(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyGlobalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.globalWorkersPrefix(tenantId)));
  }

  @GetMapping(value = "/jobs", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<WatchEventDto> jobs(@PathVariable String tenantId, ServerHttpRequest request) {
    return Mono.fromCompletionStage(readAuth.verifyGlobalStream(request, tenantId))
        .thenMany(watch.watchPrefix(keyspace.globalJobsPrefix(tenantId)));
  }
}
