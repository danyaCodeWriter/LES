package com.acme.coordination;

import com.acme.coordination.codec.JsonCodec;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.idempotency.IdempotencyService;
import com.acme.coordination.service.CoordinatorService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.KvTicketService;
import com.acme.coordination.service.TenantKeyQuotaService;
import com.acme.coordination.service.WorkerRegistryService;
import io.etcd.jetcd.Client;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class EtcdIntegrationTest {

  static final String REGION = "test-region";

  static GenericContainer<?> etcd;
  static Client client;

  private static TenantKeyQuotaService testQuota(Keyspace ks) {
    var props =
        new CoordinationProperties(
            new CoordinationProperties.Etcd(
                List.of("http://127.0.0.1:2379"),
                "/coord/v1/tenants",
                Duration.ofSeconds(3),
                Duration.ofSeconds(3)),
            "",
            new CoordinationProperties.ActiveRegion("E", "", "", 5000L),
            new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
            new CoordinationProperties.Sre(""),
            new CoordinationProperties.Limits(0));
    return new TenantKeyQuotaService(client, ks, props);
  }

  private static TenantKeyQuotaService testQuota(Keyspace ks, int maxKeys) {
    var props =
        new CoordinationProperties(
            new CoordinationProperties.Etcd(
                List.of("http://127.0.0.1:2379"),
                "/coord/v1/tenants",
                Duration.ofSeconds(3),
                Duration.ofSeconds(3)),
            "",
            new CoordinationProperties.ActiveRegion("E", "", "", 5000L),
            new CoordinationProperties.Idempotency(false, 86400, 128, 262_144),
            new CoordinationProperties.Sre(""),
            new CoordinationProperties.Limits(maxKeys));
    return new TenantKeyQuotaService(client, ks, props);
  }

  @BeforeAll
  static void start() {
    etcd = new GenericContainer<>("quay.io/coreos/etcd:v3.5.15")
        .withExposedPorts(2379)
        .withCommand(
            "/usr/local/bin/etcd",
            "--name", "etcd0",
            "--advertise-client-urls", "http://0.0.0.0:2379",
            "--listen-client-urls", "http://0.0.0.0:2379",
            "--initial-advertise-peer-urls", "http://0.0.0.0:2380",
            "--listen-peer-urls", "http://0.0.0.0:2380",
            "--initial-cluster", "etcd0=http://0.0.0.0:2380",
            "--initial-cluster-state", "new",
            "--initial-cluster-token", "tkn"
        )
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));
    etcd.start();
    String endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
    client = Client.builder().endpoints(endpoint).build();
  }

  @AfterAll
  static void stop() {
    if (client != null) client.close();
    if (etcd != null) etcd.stop();
  }

  @Test
  void coordinatorAcquireIsExclusivePerTenant() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    CoordinatorService svc = new CoordinatorService(client, ks, json, testQuota(ks));

    var r1 = svc.acquire("t1", REGION, "i1", "p1", 10, Map.of("a", "b")).get();
    Assertions.assertTrue(r1.acquired());
    Assertions.assertNotNull(r1.token());
    Assertions.assertFalse(r1.token().isBlank());
    Assertions.assertNotNull(r1.fenceEpoch());
    Assertions.assertTrue(r1.fenceEpoch() > 0);
    Assertions.assertTrue(svc.renewIfHolder("t1", REGION, r1.token()).get());
    Assertions.assertFalse(svc.renewIfHolder("t1", REGION, "wrong-token").get());
    var r2 = svc.acquire("t1", REGION, "i2", "p2", 10, Map.of()).get();
    Assertions.assertFalse(r2.acquired());
    Assertions.assertTrue(r2.currentHolder().isPresent());

    var r3 = svc.acquire("t2", REGION, "i2", "p2", 10, Map.of()).get();
    Assertions.assertTrue(r3.acquired());
  }

  @Test
  void listCoordinatorsGlobalPerRegion() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    CoordinatorService svc = new CoordinatorService(client, ks, json, testQuota(ks));

    Assertions.assertTrue(svc.acquire("tg", "r1", "a", "p", 10, Map.of()).get().acquired());
    Assertions.assertTrue(svc.acquire("tg", "r2", "b", "p", 10, Map.of()).get().acquired());

    Map<String, CoordinatorService.CoordinatorState> all = svc.listCoordinatorsAllRegions("tg").get();
    Assertions.assertEquals(2, all.size());
    Assertions.assertTrue(all.containsKey("r1"));
    Assertions.assertTrue(all.containsKey("r2"));
  }

  @Test
  void globalCoordinatorIsExclusivePerTenant() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    CoordinatorService svc = new CoordinatorService(client, ks, json, testQuota(ks));

    var g1 = svc.acquireGlobal("tg2", "i1", "p1", 10, Map.of()).get();
    Assertions.assertTrue(g1.acquired());
    Assertions.assertNotNull(g1.token());
    Assertions.assertTrue(svc.renewGlobalIfHolder("tg2", g1.token()).get());
    var g2 = svc.acquireGlobal("tg2", "i2", "p2", 10, Map.of()).get();
    Assertions.assertFalse(g2.acquired());

    Assertions.assertTrue(svc.acquire("tg2", REGION, "i3", "p3", 10, Map.of()).get().acquired());
  }

  @Test
  void listWorkerIdsSorted() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    WorkerRegistryService workers = new WorkerRegistryService(client, ks, json, testQuota(ks));

    Assertions.assertTrue(workers.register("tid", REGION, "w-b", "p", 10, Map.of()).get().created());
    Assertions.assertTrue(workers.register("tid", REGION, "w-a", "p", 10, Map.of()).get().created());

    var ids = workers.listWorkerIds("tid", REGION).get();
    Assertions.assertEquals(List.of("w-a", "w-b"), ids);
  }

  @Test
  void globalWorkerPoolAndRegionalAreSeparate() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    WorkerRegistryService workers = new WorkerRegistryService(client, ks, json, testQuota(ks));

    var a = workers.registerGlobal("tg3", "w1", "pod-a", 10, Map.of("dc", "eu")).get();
    Assertions.assertTrue(a.created());
    var b = workers.register("tg3", REGION, "w1", "pod-b", 10, Map.of("dc", "local")).get();
    Assertions.assertTrue(b.created());

    Assertions.assertTrue(workers.heartbeatGlobal("tg3", "w1", a.token()).get());
    Assertions.assertTrue(workers.heartbeat("tg3", REGION, "w1", b.token()).get());
  }

  @Test
  void workerRegisterHeartbeatAndJobLock() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    WorkerRegistryService workers = new WorkerRegistryService(client, ks, json, testQuota(ks));
    JobLockService jobs = new JobLockService(client, ks, json, testQuota(ks));

    var reg = workers.register("t1", REGION, "w1", "pod1", 10, Map.of("client-range", "a-m")).get();
    Assertions.assertTrue(reg.created());
    Assertions.assertNotNull(reg.token());

    var hb = workers.heartbeat("t1", REGION, "w1", reg.token()).get();
    Assertions.assertTrue(hb);

    var acq = jobs.acquire("t1", REGION, "job-1", "w1", 10, Map.of("kind", "test")).get();
    Assertions.assertTrue(acq.acquired());
    Assertions.assertNotNull(acq.token());
    Assertions.assertTrue(jobs.renewIfCredential("t1", REGION, "job-1", acq.token(), null).get());
    Assertions.assertFalse(jobs.renewIfCredential("t1", REGION, "job-1", "wrong-token", null).get());
    var acq2 = jobs.acquire("t1", REGION, "job-1", "pod2", 10, Map.of()).get();
    Assertions.assertFalse(acq2.acquired());
    Assertions.assertTrue(jobs.releaseIfCredential("t1", REGION, "job-1", acq.token(), null).get());
  }

  @Test
  void listJobsForHolderRegional() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    WorkerRegistryService workers = new WorkerRegistryService(client, ks, json, testQuota(ks));
    JobLockService jobs = new JobLockService(client, ks, json, testQuota(ks));

    var reg = workers.register("tlist", REGION, "w-hold", "pod1", 10, Map.of()).get();
    Assertions.assertTrue(reg.created());
    Assertions.assertTrue(jobs.acquire("tlist", REGION, "ja", "w-hold", 10, Map.of()).get().acquired());
    Assertions.assertTrue(jobs.acquire("tlist", REGION, "jb", "w-hold", 10, Map.of()).get().acquired());
    Assertions.assertTrue(jobs.acquire("tlist", REGION, "jc", "other", 10, Map.of()).get().acquired());

    var mine = jobs.listJobsForHolder("tlist", REGION, "w-hold").get();
    Assertions.assertEquals(2, mine.size());
    Assertions.assertTrue(mine.stream().allMatch(s -> "w-hold".equals(s.record().holder())));
  }

  @Test
  void kvTicketCreateGetDelete() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    KvTicketService kv = new KvTicketService(client, ks, json, testQuota(ks));

    var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    var put =
        kv.put("tkv", "my-key", mapper.readTree("{\"x\":1,\"y\":[2]}"), 10, null).get();
    Assertions.assertTrue(put.created());
    Assertions.assertNotNull(put.accessToken());

    var got = kv.get("tkv", "my-key", put.accessToken()).get();
    Assertions.assertEquals(1, got.value().get("x").asInt());

    Assertions.assertTrue(Boolean.TRUE.equals(kv.delete("tkv", "my-key", put.accessToken()).get().get("deleted")));

    Assertions.assertThrows(
        java.util.concurrent.ExecutionException.class,
        () -> kv.get("tkv", "my-key", put.accessToken()).get());
  }

  @Test
  void tenantKeyQuota_blocksCoordinatorAcquireWhenLimitTooLow() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    CoordinatorService svc = new CoordinatorService(client, ks, json, testQuota(ks, 1));
    Assertions.assertThrows(
        java.util.concurrent.ExecutionException.class, () -> svc.acquire("tquota", REGION, "c1", "p1", 10, Map.of()).get());
  }

  @Test
  void idempotencyPayloadRoundTrip() throws Exception {
    JsonCodec json = new JsonCodec(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());
    Keyspace ks = new Keyspace("/coord/v1/tenants");
    var cfg = new CoordinationProperties.Idempotency(true, 120, 128, 262_144);
    IdempotencyService idem = new IdempotencyService(client, ks, json, cfg);
    String fp =
        IdempotencyService.fingerprint(
            "POST",
            "/v1/tenants/t1/coordinator/acquire",
            "idem-1",
            "{\"x\":1}".getBytes(StandardCharsets.UTF_8),
            "");
    Assertions.assertTrue(idem.get("t1", fp).get().isEmpty());
    idem.store("t1", fp, 201, "{\"token\":\"abc\"}".getBytes(StandardCharsets.UTF_8)).get();
    var cached = idem.get("t1", fp).get();
    Assertions.assertTrue(cached.isPresent());
    Assertions.assertEquals(201, cached.get().httpStatus());
    Assertions.assertTrue(new String(cached.get().bodyBytes(), StandardCharsets.UTF_8).contains("abc"));
  }
}
