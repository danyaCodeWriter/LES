package com.acme.coordination.deployment;

import java.nio.charset.StandardCharsets;
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
@Order(Ordered.HIGHEST_PRECEDENCE + 40)
public class StandbyRegionalMutationWebFilter implements WebFilter {

  private final DeploymentRegistrationPolicy policy;

  public StandbyRegionalMutationWebFilter(DeploymentRegistrationPolicy policy) {
    this.policy = policy;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    String path = DeploymentRegistrationPolicy.normalizePath(exchange.getRequest().getPath().value());
    if (!policy.shouldBlockRest(path, exchange.getRequest().getMethod())) {
      return chain.filter(exchange);
    }
    String json = policy.isSwitch() ? DeploymentRegistrationPolicy.switchBodyJson() : DeploymentRegistrationPolicy.standbyBodyJson();
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
    exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
    return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
  }
}
