package com.acme.coordination.idempotency;

import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.service.KvTicketService;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import org.reactivestreams.Publisher;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * При наличии {@code Idempotency-Key} / {@code X-Idempotency-Key} кеширует ответы выбранных POST/PUT в etcd (см.
 * {@link CoordinationProperties.Idempotency}). Повтор с тем же ключом, методом, path, телом и (для KV PUT) тем же
 * access-токеном возвращает тот же HTTP-статус и JSON-тело.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public class IdempotencyWebFilter implements WebFilter {

  private static final Pattern P_COORD = Pattern.compile("^/v1/tenants/([^/]+)/coordinator/acquire$");
  private static final Pattern P_WORKER = Pattern.compile("^/v1/tenants/([^/]+)/workers/register$");
  private static final Pattern P_JOB = Pattern.compile("^/v1/tenants/([^/]+)/jobs/[^/]+:acquire$");
  private static final Pattern P_GCOORD = Pattern.compile("^/v1/tenants/([^/]+)/global/coordinator/acquire$");
  private static final Pattern P_GWORKER = Pattern.compile("^/v1/tenants/([^/]+)/global/workers/register$");
  private static final Pattern P_GJOB = Pattern.compile("^/v1/tenants/([^/]+)/global/jobs/[^/]+:acquire$");
  private static final Pattern P_GKV = Pattern.compile("^/v1/tenants/([^/]+)/global/kv/[^/]+$");

  private final CoordinationProperties props;
  private final IdempotencyService service;

  public IdempotencyWebFilter(CoordinationProperties props, IdempotencyService service) {
    this.props = props;
    this.service = service;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    if (!props.idempotency().enabled()) {
      return chain.filter(exchange);
    }
    HttpMethod method = exchange.getRequest().getMethod();
    if (method == null) {
      return chain.filter(exchange);
    }
    String m = method.name().toUpperCase(Locale.ROOT);
    if (!"POST".equals(m) && !"PUT".equals(m)) {
      return chain.filter(exchange);
    }
    String path = normalizePath(exchange.getRequest().getURI());
    Optional<String> tenantOpt = tenantFromPath(method, path);
    if (tenantOpt.isEmpty()) {
      return chain.filter(exchange);
    }
    String idem = extractIdempotencyKey(exchange.getRequest());
    if (idem.isEmpty()) {
      return chain.filter(exchange);
    }
    int maxH = props.idempotency().maxHeaderLength();
    if (idem.length() > maxH) {
      return writeJsonError(
          exchange,
          HttpStatus.BAD_REQUEST,
          "IDEMPOTENCY_KEY_INVALID",
          "Idempotency-Key exceeds max length " + maxH);
    }
    int maxBody = props.idempotency().maxBodyBytes();
    String tenantId = tenantOpt.get();
    DataBufferFactory bufferFactory = exchange.getResponse().bufferFactory();

    Mono<byte[]> bodyMono =
        DataBufferUtils.join(exchange.getRequest().getBody())
            .map(IdempotencyWebFilter::drainBuffer)
            .switchIfEmpty(Mono.fromSupplier(() -> new byte[0]));

    return bodyMono.flatMap(
        bodyBytes -> {
          if (bodyBytes.length > maxBody) {
            ServerWebExchange replay =
                exchange.mutate().request(rebuildRequest(exchange.getRequest(), bufferFactory, bodyBytes)).build();
            return chain.filter(replay);
          }
          String kvPart = kvAccessFingerprintPart(method, path, exchange.getRequest());
          String fp = IdempotencyService.fingerprint(m, path, idem, bodyBytes, kvPart);
          ServerWebExchange withBody =
              exchange.mutate().request(rebuildRequest(exchange.getRequest(), bufferFactory, bodyBytes)).build();

          return Mono.fromFuture(service.get(tenantId, fp))
              .subscribeOn(Schedulers.boundedElastic())
              .flatMap(
                  opt -> {
                    if (opt.isPresent()) {
                      IdempotencyPayload c = opt.get();
                      withBody.getResponse().setStatusCode(HttpStatus.valueOf(c.httpStatus()));
                      withBody.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
                      byte[] out = c.bodyBytes();
                      withBody.getResponse().getHeaders().setContentLength(out.length);
                      return withBody.getResponse().writeWith(Mono.just(bufferFactory.wrap(out)));
                    }
                    ServerHttpResponseDecorator decorated =
                        new ServerHttpResponseDecorator(withBody.getResponse()) {
                          @Override
                          public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                            return DataBufferUtils.join(body)
                                .flatMap(
                                    buf -> {
                                      byte[] content = drainBuffer(buf);
                                      int code =
                                          getDelegate().getStatusCode() != null
                                              ? getDelegate().getStatusCode().value()
                                              : 200;
                                      Mono<Void> storeMono = Mono.empty();
                                      if (shouldCache(code)) {
                                        storeMono =
                                            Mono.fromFuture(
                                                    service.store(
                                                        tenantId,
                                                        fp,
                                                        code,
                                                        java.util.Arrays.copyOf(content, content.length)))
                                                .subscribeOn(Schedulers.boundedElastic())
                                                .onErrorResume(e -> Mono.empty())
                                                .then();
                                      }
                                      DataBuffer outBuf = bufferFactory.wrap(content);
                                      return storeMono.then(super.writeWith(Mono.just(outBuf)));
                                    });
                          }
                        };
                    return chain.filter(withBody.mutate().response(decorated).build());
                  });
        });
  }

  private static Mono<Void> writeJsonError(
      ServerWebExchange exchange, HttpStatus status, String code, String message
  ) {
    exchange.getResponse().setStatusCode(status);
    exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
    String json =
        "{\"error\":\""
            + code
            + "\",\"message\":\""
            + message.replace("\\", "\\\\").replace("\"", "\\\"")
            + "\"}";
    byte[] b = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponse().getHeaders().setContentLength(b.length);
    return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(b)));
  }

  private static boolean shouldCache(int code) {
    return code == 200 || code == 201 || code == 409;
  }

  private static String extractIdempotencyKey(ServerHttpRequest req) {
    String a = req.getHeaders().getFirst("Idempotency-Key");
    if (a != null && !a.isBlank()) {
      return a.trim();
    }
    String b = req.getHeaders().getFirst("X-Idempotency-Key");
    return b == null ? "" : b.trim();
  }

  private static String kvAccessFingerprintPart(HttpMethod method, String path, ServerHttpRequest req) {
    if (method != HttpMethod.PUT) {
      return "";
    }
    if (!P_GKV.matcher(path).matches()) {
      return "";
    }
    return KvTicketService.extractAccessToken(req);
  }

  private static Optional<String> tenantFromPath(HttpMethod method, String path) {
    if (method == HttpMethod.POST) {
      return firstGroup(path, P_COORD, P_WORKER, P_JOB, P_GCOORD, P_GWORKER, P_GJOB);
    }
    if (method == HttpMethod.PUT && P_GKV.matcher(path).matches()) {
      return firstGroup(path, P_GKV);
    }
    return Optional.empty();
  }

  @SafeVarargs
  private static Optional<String> firstGroup(String path, Pattern... patterns) {
    for (Pattern p : patterns) {
      var m = p.matcher(path);
      if (m.matches()) {
        return Optional.of(m.group(1));
      }
    }
    return Optional.empty();
  }

  private static String normalizePath(URI uri) {
    String p = uri.getPath();
    if (p == null) {
      return "";
    }
    int q = p.indexOf('?');
    if (q >= 0) {
      p = p.substring(0, q);
    }
    if (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  private static ServerHttpRequest rebuildRequest(ServerHttpRequest delegate, DataBufferFactory factory, byte[] body) {
    return new ServerHttpRequestDecorator(delegate) {
      @Override
      public Flux<DataBuffer> getBody() {
        if (body == null || body.length == 0) {
          return Flux.empty();
        }
        return Flux.just(factory.wrap(java.util.Arrays.copyOf(body, body.length)));
      }
    };
  }

  private static byte[] drainBuffer(DataBuffer buf) {
    try {
      int n = buf.readableByteCount();
      if (n == 0) {
        return new byte[0];
      }
      byte[] b = new byte[n];
      buf.read(b);
      return b;
    } finally {
      DataBufferUtils.release(buf);
    }
  }
}
