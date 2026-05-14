package com.acme.coordination.config;

import com.acme.coordination.etcd.Keyspace;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.Client;
import java.time.Duration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@EnableConfigurationProperties(CoordinationProperties.class)
public class Config {

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Bean(destroyMethod = "close")
  public Client etcdClient(CoordinationProperties props) {
    Duration dialTimeout = props.etcd().dialTimeout() != null ? props.etcd().dialTimeout() : Duration.ofSeconds(3);
    return Client.builder()
        .endpoints(props.etcd().endpoints().toArray(new String[0]))
        .connectTimeout(dialTimeout)
        .build();
  }

  @Bean
  public Keyspace keyspace(CoordinationProperties props) {
    return new Keyspace(props.etcd().rootPrefix());
  }
}

