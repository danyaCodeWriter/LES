package com.acme.coordination.sdk;

import com.acme.coordination.ring.RingAssignment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Thin REST client for coordination-server (Jackson JSON).
 *
 * <p>Low-level: {@link #registerWorker}, {@link #acquireCoordinator}, {@link #acquireJob}, renew/heartbeat by hand.
 *
 * <p>Preferred for production loops: {@link com.acme.coordination.sdk.session.CoordinationSessions} —
 * {@code holdWorker} / {@code holdCoordinator} / {@code holdJob} with auto-renew and {@code try}-with-resources.
 *
 * <p>GET coordinator / workers / jobs (and SSE workers/jobs streams) require the active leader's renew token:
 * {@code Authorization: Bearer <coordinator acquire token>} (regional vs global scope must match the URL). GET a
 * <strong>single</strong> job by id additionally accepts the <strong>holder worker's</strong> register token (same
 * Bearer or {@code X-Worker-Token}).
 */
public final class CoordinationRestClient implements AutoCloseable {

  private final HttpClient http;
  private final ObjectMapper json;
  private final String baseUrl;

  public CoordinationRestClient(String baseUrl) {
    this(baseUrl, new ObjectMapper().findAndRegisterModules(), HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build());
  }

  public CoordinationRestClient(String baseUrl, ObjectMapper json, HttpClient http) {
    this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/$", "");
    this.json = Objects.requireNonNull(json);
    this.http = Objects.requireNonNull(http);
  }

  public JsonNode getCoordinatorActive(String tenantId, String coordinatorReadToken) throws Exception {
    return getJson("/v1/tenants/" + tenantId + "/coordinator", coordinatorReadToken);
  }

  /** JSON array of worker id strings (active-region pool), sorted. */
  public JsonNode getWorkerIdsActive(String tenantId, String coordinatorReadToken) throws Exception {
    return getJson("/v1/tenants/" + tenantId + "/workers/ids", coordinatorReadToken);
  }

  /** JSON array of worker id strings (global pool), sorted. */
  public JsonNode getWorkerIdsGlobal(String tenantId, String globalCoordinatorReadToken) throws Exception {
    return getJson("/v1/tenants/" + tenantId + "/global/workers/ids", globalCoordinatorReadToken);
  }

  public JsonNode getCoordinatorGlobalSingleton(String tenantId, String globalCoordinatorReadToken) throws Exception {
    return getJson("/v1/tenants/" + tenantId + "/global/coordinator", globalCoordinatorReadToken);
  }

  public JsonNode acquireCoordinator(String tenantId, Map<String, Object> body) throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/coordinator/acquire", body, null);
  }

  /** Same as {@link #acquireCoordinator} with optional {@code Idempotency-Key} (replay-safe after network loss). */
  public JsonNode acquireCoordinator(String tenantId, Map<String, Object> body, String idempotencyKey) throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/coordinator/acquire", body, idempotencyKey);
  }

  public JsonNode acquireCoordinatorGlobal(String tenantId, Map<String, Object> body) throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/global/coordinator/acquire", body, null);
  }

  public JsonNode acquireCoordinatorGlobal(String tenantId, Map<String, Object> body, String idempotencyKey)
      throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/global/coordinator/acquire", body, idempotencyKey);
  }

  public JsonNode renewCoordinator(String tenantId, String token) throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/coordinator/renew", Map.of("token", token));
  }

  public JsonNode renewCoordinatorGlobal(String tenantId, String token) throws Exception {
    return postJson("/v1/tenants/" + tenantId + "/global/coordinator/renew", Map.of("token", token));
  }

  /** Active-region worker pool (server resolves logical region). */
  public JsonNode registerWorker(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("workerId", workerId);
    body.put("instanceId", instanceId);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/workers/register", body, null);
  }

  public JsonNode registerWorker(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata,
      String idempotencyKey
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("workerId", workerId);
    body.put("instanceId", instanceId);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/workers/register", body, idempotencyKey);
  }

  public JsonNode workerHeartbeat(String tenantId, String workerId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/workers/" + urlPathSegment(workerId) + "/heartbeat",
        Map.of("token", token));
  }

  public JsonNode deregisterWorker(String tenantId, String workerId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/workers/" + urlPathSegment(workerId) + "/deregister",
        Map.of("token", token));
  }

  /** Global worker pool ({@code .../global/workers/...}). */
  public JsonNode registerWorkerGlobal(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("workerId", workerId);
    body.put("instanceId", instanceId);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/global/workers/register", body, null);
  }

  public JsonNode registerWorkerGlobal(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata,
      String idempotencyKey
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("workerId", workerId);
    body.put("instanceId", instanceId);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/global/workers/register", body, idempotencyKey);
  }

  public JsonNode workerHeartbeatGlobal(String tenantId, String workerId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/global/workers/" + urlPathSegment(workerId) + "/heartbeat",
        Map.of("token", token));
  }

  public JsonNode deregisterWorkerGlobal(String tenantId, String workerId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/global/workers/" + urlPathSegment(workerId) + "/deregister",
        Map.of("token", token));
  }

  public JsonNode acquireJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("holder", holder);
    body.put("participantToken", participantToken);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/jobs/" + urlPathSegment(jobId) + ":acquire", body, null);
  }

  public JsonNode acquireJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata,
      String idempotencyKey
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("holder", holder);
    body.put("participantToken", participantToken);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/jobs/" + urlPathSegment(jobId) + ":acquire", body, idempotencyKey);
  }

  public JsonNode renewJob(String tenantId, String jobId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/jobs/" + urlPathSegment(jobId) + ":renew",
        Map.of("token", token));
  }

  public JsonNode releaseJob(String tenantId, String jobId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/jobs/" + urlPathSegment(jobId) + ":release",
        Map.of("token", token));
  }

  public JsonNode getJob(String tenantId, String jobId, String coordinatorReadToken) throws Exception {
    return getJob(tenantId, jobId, coordinatorReadToken, null);
  }

  /**
   * GET job state: coordinator read token and/or holder worker register token (if both set, Bearer = coordinator,
   * {@code X-Worker-Token} = worker).
   */
  public JsonNode getJob(String tenantId, String jobId, String coordinatorReadToken, String workerRegisterToken)
      throws Exception {
    return getJsonJobRead(
        "/v1/tenants/" + tenantId + "/jobs/" + urlPathSegment(jobId), coordinatorReadToken, workerRegisterToken);
  }

  public JsonNode acquireJobGlobal(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("holder", holder);
    body.put("participantToken", participantToken);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/global/jobs/" + urlPathSegment(jobId) + ":acquire", body, null);
  }

  public JsonNode acquireJobGlobal(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata,
      String idempotencyKey
  ) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("holder", holder);
    body.put("participantToken", participantToken);
    body.put("ttlSeconds", ttlSeconds);
    body.put("metadata", metadata == null ? Map.of() : metadata);
    return postJson("/v1/tenants/" + tenantId + "/global/jobs/" + urlPathSegment(jobId) + ":acquire", body, idempotencyKey);
  }

  public JsonNode renewJobGlobal(String tenantId, String jobId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/global/jobs/" + urlPathSegment(jobId) + ":renew",
        Map.of("token", token));
  }

  public JsonNode releaseJobGlobal(String tenantId, String jobId, String token) throws Exception {
    return postJson(
        "/v1/tenants/" + tenantId + "/global/jobs/" + urlPathSegment(jobId) + ":release",
        Map.of("token", token));
  }

  public JsonNode getJobGlobal(String tenantId, String jobId, String globalCoordinatorReadToken) throws Exception {
    return getJobGlobal(tenantId, jobId, globalCoordinatorReadToken, null);
  }

  public JsonNode getJobGlobal(
      String tenantId, String jobId, String globalCoordinatorReadToken, String workerRegisterToken
  ) throws Exception {
    return getJsonJobRead(
        "/v1/tenants/" + tenantId + "/global/jobs/" + urlPathSegment(jobId),
        globalCoordinatorReadToken,
        workerRegisterToken);
  }

  /**
   * POST /v1/rings/assign (no tenant) and build a {@link RingAssignment} from {@code ranges} only (no {@code withPos}).
   */
  public RingAssignment assignRing(List<String> workerIds, Integer virtualNodes) throws Exception {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("workerIds", workerIds);
    if (virtualNodes != null) {
      body.put("virtualNodes", virtualNodes);
    }
    JsonNode root = postJson("/v1/rings/assign", body, null);
    return RingAssignments.fromAssignResponse(root);
  }

  private JsonNode getJsonJobRead(String path, String coordinatorReadToken, String workerRegisterToken)
      throws Exception {
    HttpRequest.Builder rb =
        HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(30)).GET();
    boolean coord = coordinatorReadToken != null && !coordinatorReadToken.isBlank();
    boolean worker = workerRegisterToken != null && !workerRegisterToken.isBlank();
    if (coord) {
      rb.header("Authorization", "Bearer " + coordinatorReadToken.trim());
    }
    if (worker) {
      if (!coord) {
        rb.header("Authorization", "Bearer " + workerRegisterToken.trim());
      } else {
        rb.header("X-Worker-Token", workerRegisterToken.trim());
      }
    }
    HttpRequest req = rb.build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("GET " + path + " failed: HTTP " + resp.statusCode() + " body=" + resp.body());
    }
    if (resp.body() == null || resp.body().isBlank()) {
      return json.nullNode();
    }
    return json.readTree(resp.body());
  }

  private JsonNode getJson(String path, String coordinatorReadToken) throws Exception {
    HttpRequest.Builder rb =
        HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(30)).GET();
    if (coordinatorReadToken != null && !coordinatorReadToken.isBlank()) {
      rb.header("Authorization", "Bearer " + coordinatorReadToken);
    }
    HttpRequest req = rb.build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("GET " + path + " failed: HTTP " + resp.statusCode() + " body=" + resp.body());
    }
    if (resp.body() == null || resp.body().isBlank()) {
      return json.nullNode();
    }
    return json.readTree(resp.body());
  }

  private static String urlPathSegment(String raw) {
    return URLEncoder.encode(Objects.requireNonNull(raw, "segment"), StandardCharsets.UTF_8).replace("+", "%20");
  }

  private JsonNode postJson(String path, Object body) throws Exception {
    return postJson(path, body, null);
  }

  private JsonNode postJson(String path, Object body, String idempotencyKey) throws Exception {
    String payload = json.writeValueAsString(body);
    HttpRequest.Builder rb =
        HttpRequest.newBuilder(URI.create(baseUrl + path))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      rb.header("Idempotency-Key", idempotencyKey.trim());
    }
    HttpRequest req = rb.build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("POST " + path + " failed: HTTP " + resp.statusCode() + " body=" + resp.body());
    }
    return json.readTree(resp.body());
  }

  /**
   * Sessions with auto-renew / heartbeat (regional or global). Caller must {@code close()} the returned
   * {@link com.acme.coordination.sdk.session.CoordinationSessions} (shuts down the renewer thread).
   */
  public com.acme.coordination.sdk.session.CoordinationSessions sessions() {
    return new com.acme.coordination.sdk.session.CoordinationSessions(this);
  }

  @Override
  public void close() {
    // HttpClient is shared; nothing to close.
  }
}
