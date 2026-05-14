package com.acme.coordination.health;

import com.acme.coordination.config.CoordinationProperties;
import io.etcd.jetcd.Client;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class EtcdHealthIndicator implements HealthIndicator {

  private final Client client;
  private final CoordinationProperties props;

  public EtcdHealthIndicator(Client client, CoordinationProperties props) {
    this.client = client;
    this.props = props;
  }

  @Override
  public Health health() {
    try {
      String endpoint = props.etcd().endpoints().getFirst();
      client.getMaintenanceClient()
          .statusMember(endpoint)
          .get(Duration.ofSeconds(2).toMillis(), TimeUnit.MILLISECONDS);
      return Health.up().build();
    } catch (Exception e) {
      return Health.down(e).build();
    }
  }
}

