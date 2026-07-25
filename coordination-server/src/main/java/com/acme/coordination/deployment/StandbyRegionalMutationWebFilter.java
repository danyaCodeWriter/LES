package com.acme.coordination.deployment;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public class StandbyRegionalMutationWebFilter implements WebFilter {

  private final DeploymentRegistrationPolicy policy;

  public StandbyRegionalMutationWebFilter(DeploymentRegistrationPolicy policy) {
    this.policy = policy;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    var req = exchange.getRequest();
    if (!policy.shouldBlockRest(req.getPath().value(), req.getMethod())) {
      return chain.filter(exchange);
    }
    String body = policy.isSwitch()
        ? DeploymentRegistrationPolicy.switchBodyJson()
        : DeploymentRegistrationPolicy.standbyBodyJson();
    exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
    exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
    var buffer = exchange.getResponse().bufferFactory().wrap(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return exchange.getResponse().writeWith(Mono.just(buffer));
  }
}
