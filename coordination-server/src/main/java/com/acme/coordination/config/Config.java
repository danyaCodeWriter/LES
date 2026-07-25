package com.acme.coordination.config;

import com.acme.coordination.etcd.Keyspace;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@EnableConfigurationProperties(CoordinationProperties.class)
public class Config {

  private static final Logger log = LoggerFactory.getLogger(Config.class);

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Bean(destroyMethod = "close")
  public Client etcdClient(CoordinationProperties props) {
    CoordinationProperties.Etcd etcd = props.etcd();
    Duration dialTimeout =
        etcd.dialTimeout() != null ? etcd.dialTimeout() : Duration.ofSeconds(3);
    ClientBuilder builder =
        Client.builder()
            .endpoints(etcd.endpoints().toArray(new String[0]))
            .connectTimeout(dialTimeout);

    if (etcd.authEnabled()) {
      builder
          .user(ByteSequence.from(etcd.username(), StandardCharsets.UTF_8))
          .password(ByteSequence.from(etcd.password() == null ? "" : etcd.password(), StandardCharsets.UTF_8));
      log.info("etcd client: RBAC user={}", etcd.username());
    }

    CoordinationProperties.Tls tls = etcd.tls();
    if (tls != null && tls.enabled()) {
      applyTls(builder, tls);
      log.info(
          "etcd client: TLS enabled caCert={} mTLS={} authority={}",
          tls.hasCaCert() ? tls.caCertPath() : "(default JVM trust)",
          tls.hasClientCert(),
          tls.authority().isBlank() ? "(none)" : tls.authority());
    }

    return builder.build();
  }

  private static void applyTls(ClientBuilder builder, CoordinationProperties.Tls tls) {
    try {
      builder.sslContext(
          ssl -> {
            if (tls.hasCaCert()) {
              ssl.trustManager(new File(tls.caCertPath()));
            }
            if (tls.hasClientCert()) {
              ssl.keyManager(new File(tls.certPath()), new File(tls.keyPath()));
            }
          });
    } catch (SSLException e) {
      throw new IllegalStateException(
          "Failed to configure etcd TLS (check coordination.etcd.tls.* PEM paths): " + e.getMessage(),
          e);
    }
    if (!tls.authority().isBlank()) {
      builder.authority(tls.authority());
    }
  }

  @Bean
  public Keyspace keyspace(CoordinationProperties props) {
    return new Keyspace(props.etcd().rootPrefix());
  }
}
