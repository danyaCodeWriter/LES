package com.acme.coordination.rss;

import com.acme.coordination.config.CoordinationProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Client for Region State Service SSE:
 * {@code GET /v1/recovery-groups/{recoveryGroup}/active-region?region={homeRegion}}.
 */
@Component
public class RssClient {

  private static final Logger log = LoggerFactory.getLogger(RssClient.class);

  private final CoordinationProperties props;
  private final ObjectMapper objectMapper;
  private final WebClient.Builder webClientBuilder;
  private final AtomicLong lastRevision = new AtomicLong(0);

  public RssClient(
      CoordinationProperties props, ObjectMapper objectMapper, WebClient.Builder webClientBuilder) {
    this.props = props;
    this.objectMapper = objectMapper;
    this.webClientBuilder = webClientBuilder;
  }

  /** First {@code state} event (single connection, no reconnect loop). */
  public Mono<RssMappedState> firstState() {
    return connectOnce()
        .next()
        .doOnNext(s -> lastRevision.updateAndGet(prev -> Math.max(prev, s.revision())));
  }

  /** Live stream of state events with reconnect/backoff. */
  public Flux<RssMappedState> stateUpdates() {
    CoordinationProperties.Rss rss = props.rss();
    if (!rss.isConfigured()) {
      return Flux.error(new IllegalStateException("coordination.rss.base-url and recovery-group must be set"));
    }
    return Flux.defer(this::connectOnce)
        .doOnNext(s -> lastRevision.updateAndGet(prev -> Math.max(prev, s.revision())))
        .retryWhen(
            Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(30))
                .doBeforeRetry(
                    sig ->
                        log.warn(
                            "RSS SSE reconnect after error: {}",
                            sig.failure() == null ? "?" : sig.failure().toString())));
  }

  private Flux<RssMappedState> connectOnce() {
    CoordinationProperties.Rss rss = props.rss();
    if (!rss.isConfigured()) {
      return Flux.error(new IllegalStateException("coordination.rss.base-url and recovery-group must be set"));
    }
    long fromRev = lastRevision.get();
    WebClient client =
        webClientBuilder
            .baseUrl(rss.baseUrl())
            .codecs(c -> c.defaultCodecs().maxInMemorySize(1024 * 1024))
            .build();

    return client
        .get()
        .uri(
            uriBuilder -> {
              var b =
                  uriBuilder
                      .path("/v1/recovery-groups/{recoveryGroup}/active-region")
                      .queryParam("region", rss.homeRegion());
              if (fromRev > 0) {
                b.queryParam("from_revision", fromRev);
              }
              return b.build(rss.recoveryGroup());
            })
        .accept(MediaType.TEXT_EVENT_STREAM)
        .retrieve()
        .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
        .concatMap(this::mapSse)
        .doOnSubscribe(
            s ->
                log.info(
                    "RSS SSE connecting {}/v1/recovery-groups/{}/active-region?region={} (from_revision={})",
                    rss.baseUrl(),
                    rss.recoveryGroup(),
                    rss.homeRegion(),
                    fromRev));
  }

  private Flux<RssMappedState> mapSse(ServerSentEvent<String> sse) {
    if (sse == null) {
      return Flux.empty();
    }
    String type = sse.event();
    if (type != null && !type.isBlank() && !"state".equalsIgnoreCase(type)) {
      if ("heartbeat".equalsIgnoreCase(type)) {
        log.debug("RSS heartbeat: {}", sse.data());
      } else {
        log.debug("RSS ignoring unknown SSE event type={}", type);
      }
      return Flux.empty();
    }
    return RssStateMapper.parseStateData(objectMapper, sse.data())
        .map(Flux::just)
        .orElseGet(Flux::empty);
  }
}
