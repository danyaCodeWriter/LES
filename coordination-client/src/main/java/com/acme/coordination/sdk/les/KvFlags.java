package com.acme.coordination.sdk.les;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Thin GLOBAL KV helpers for kill-switch / shard-map style keys. */
public final class KvFlags {

  private final LesClient client;
  private final String tenantId;
  private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
  private final Map<String, String> accessTokens = new ConcurrentHashMap<>();
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  KvFlags(LesClient client, String tenantId) {
    this.client = client;
    this.tenantId = tenantId;
  }

  public void put(String keyId, Map<String, ?> value, Duration ttl) throws Exception {
    ObjectNode body = json.createObjectNode();
    body.set("value", json.valueToTree(value));
    if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
      body.put("ttlSeconds", Math.max(10, (int) ttl.toSeconds()));
    }
    String path = "/v1/tenants/" + tenantId + "/global/kv/" + enc(keyId);
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(client.restBase() + path))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
            .build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("KV put failed HTTP " + resp.statusCode() + " " + resp.body());
    }
    JsonNode n = json.readTree(resp.body());
    if (n.hasNonNull("accessToken")) {
      accessTokens.put(keyId, n.get("accessToken").asText());
    }
  }

  public boolean isSet(String keyId) {
    try {
      return get(keyId).isPresent();
    } catch (Exception e) {
      return false;
    }
  }

  public Optional<JsonNode> get(String keyId) throws Exception {
    String token = accessTokens.get(keyId);
    String path = "/v1/tenants/" + tenantId + "/global/kv/" + enc(keyId);
    HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(client.restBase() + path)).GET();
    if (token != null) {
      rb.header("Authorization", "Bearer " + token);
    }
    HttpResponse<String> resp = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() == 404) {
      return Optional.empty();
    }
    if (resp.statusCode() / 100 != 2) {
      return Optional.empty();
    }
    return Optional.of(json.readTree(resp.body()).path("value"));
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
