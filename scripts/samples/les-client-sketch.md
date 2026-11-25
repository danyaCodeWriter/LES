# LesClient sketch (after refactor)

```bash
# etcd + server
./scripts/demo-two-dc.sh etcd-start
./gradlew test
./gradlew :coordination-server:bootRun
# HTTP :8080  gRPC :9090  SRE token default: sre-stream-dev-token
```

```java
import com.acme.coordination.sdk.les.LesClient;
import com.acme.coordination.sdk.session.JobLease;

try (var les = LesClient.connect("http://localhost:8080")
        .sreToken("sre-stream-dev-token")
        .homeRegion("r1")
        // Optional RSS (client-only; Les server never calls RSS):
        // .viaRss("http://rss:8080", "payments-rg")
        // .drainTenant("payments")
        // .autoDrainOnFlip(true)
        .build()) {

  // Path A — freestanding lock with background renew while work runs
  les.tenant("payments").global()
     .lock("nightly-reconcile")
     .tryRun(() -> { /* long work; renew ticks in background */ });

  // Long hold (manual close)
  les.tenant("payments").global().lock("batch-x").tryHold().ifPresent(lease -> {
    try (JobLease held = lease) {
      // held.isHeld() stays true while renew succeeds
    } catch (Exception e) { throw new RuntimeException(e); }
  });

  // JobQ + hashing DX
  try (var leader = les.tenant("payments").global().becomeLeader("queue-claimer");
       var w = les.tenant("payments").global().joinWorker("w1")) {
    var pool = les.tenant("payments").global().workers(leader);
    var ring = pool.hashRing();
    System.out.println(ring.workerFor("account-42"));
  }

  // GLOBAL KV kill-switch / shard-map + watch
  var flags = les.tenant("payments").global().flags();
  flags.put("payments-halt", java.util.Map.of("on", true), java.time.Duration.ofHours(1));
  try (var watch = flags.watch("payments-halt", ev -> System.out.println(ev.eventType()))) {
    Thread.sleep(1000);
  }

  // Manual drain (after RSS flip, or ops)
  // les.tenant("payments").drain("r1");
}
```

## Drain curl

```bash
curl -sS -X POST -H "Authorization: Bearer sre-stream-dev-token" \
  http://localhost:8080/v1/tenants/payments/regions/r1:drain | jq .
```

## Freestanding job curl

```bash
curl -sS -X POST http://localhost:8080/v1/tenants/payments/jobs/task-x:acquire \
  -H 'content-type: application/json' \
  -d '{"holder":"pod-1","ttlSeconds":30,"metadata":{}}' | jq .
# no participantToken → freestanding
```
