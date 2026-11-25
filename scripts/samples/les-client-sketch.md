# LesClient sketch (after refactor)

```bash
# etcd + server
./scripts/demo-two-dc.sh etcd-start
./gradlew :coordination-server:bootRun
```

```java
try (var les = LesClient.connect("http://localhost:8080")
        .sreToken("sre-stream-dev-token")
        .homeRegion("r1")
        // .viaRss("http://rss:8080", "payments-rg").drainTenant("payments").autoDrainOnFlip(true)
        .build()) {

  // Path A — exclusive freestanding lock
  les.tenant("payments").global()
     .lock("nightly-reconcile")
     .tryRun(() -> System.out.println("only one wins"));

  // JobQ
  try (var leader = les.tenant("payments").global().becomeLeader("queue-claimer");
       var w = les.tenant("payments").global().joinWorker("w1")) {
    var ring = les.tenant("payments").global().hashRing(java.util.List.of(w.workerId()));
    System.out.println(ring);
  }

  // Drain after RSS flip (manual)
  // les.tenant("payments").drain("r1");
}
```
