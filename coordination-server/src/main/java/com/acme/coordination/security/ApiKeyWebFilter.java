package com.acme.coordination.security;

import com.acme.coordination.config.CoordinationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiKeyWebFilter implements WebFilter {

  private final CoordinationProperties props;

  public ApiKeyWebFilter(CoordinationProperties props) {
    this.props = props;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    String required = props.apiKey();
    if (required == null || required.isBlank()) {
      return chain.filter(exchange);
    }
    String got = exchange.getRequest().getHeaders().getFirst("x-api-key");
    if (required.equals(got)) {
      return chain.filter(exchange);
    }
    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
    return exchange.getResponse().setComplete();
  }
}

