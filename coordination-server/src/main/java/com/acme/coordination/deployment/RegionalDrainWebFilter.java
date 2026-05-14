package com.acme.coordination.deployment;

import com.acme.coordination.region.RegionSwitchDrainPolicy;
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
@Order(Ordered.HIGHEST_PRECEDENCE + 38)
public class RegionalDrainWebFilter implements WebFilter {

  private final RegionSwitchDrainPolicy drain;

  public RegionalDrainWebFilter(RegionSwitchDrainPolicy drain) {
    this.drain = drain;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    String path = DeploymentRegistrationPolicy.normalizePath(exchange.getRequest().getPath().value());
    if (!RegionalLeaseMutationPaths.isRegionalLeaseMutationRest(path, exchange.getRequest().getMethod())) {
      return chain.filter(exchange);
    }
    if (drain.regionalWritesAllowed()) {
      return chain.filter(exchange);
    }
    String region = drain.drainingPreviousRegionId().orElse("");
    String json = "{\"error\":\"REGION_DRAIN\",\"message\":\""
        + "Wait until coordinator, workers, and job keys in the previous active region expire (TTL). "
        + "Only coordinationEpoch may remain. Then acquire/register/job APIs succeed again.\","
        + "\"drainingPreviousRegion\":\"" + escapeJson(region) + "\"}";
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponse().setStatusCode(HttpStatus.CONFLICT);
    exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
    return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
