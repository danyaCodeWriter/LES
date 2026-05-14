package com.acme.coordination.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

  @Bean
  public OpenAPI coordinationOpenApi() {
    return new OpenAPI()
        .info(new Info()
            .title("Coordination service API")
            .version("v1")
            .description(
                "REST + SSE over etcd: active-region short paths (no region in URL), cross-DC /global/... . "
                    + "See static openapi/openapi.yaml."));
  }
}
