package com.acme.coordination;

import com.acme.coordination.deployment.CoordinationDeploymentProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(CoordinationDeploymentProperties.class)
public class CoordinationServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(CoordinationServiceApplication.class, args);
  }
}

