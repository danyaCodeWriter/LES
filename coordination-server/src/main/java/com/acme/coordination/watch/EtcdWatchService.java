package com.acme.coordination.watch;

import static com.acme.coordination.etcd.EtcdBytes.bs;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.options.WatchOption;
import java.nio.charset.StandardCharsets;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Service
public class EtcdWatchService {

  private final Watch watch;

  public EtcdWatchService(Client client) {
    this.watch = client.getWatchClient();
  }

  public Flux<WatchEventDto> watchPrefix(String prefix) {
    return Flux.create(sink -> {
      WatchOption opt = WatchOption.newBuilder().isPrefix(true).build();
      Watcher watcher = watch.watch(bs(prefix), opt, resp -> {
        for (var ev : resp.getEvents()) {
          var kv = ev.getKeyValue();
          String key = kv.getKey().toString(StandardCharsets.UTF_8);
          String val = kv.getValue().toString(StandardCharsets.UTF_8);
          sink.next(new WatchEventDto(
              ev.getEventType().name(),
              key,
              kv.getModRevision(),
              kv.getLease(),
              val
          ));
        }
      });
      sink.onCancel(watcher::close);
      sink.onDispose(watcher::close);
    }, FluxSink.OverflowStrategy.BUFFER);
  }
}

