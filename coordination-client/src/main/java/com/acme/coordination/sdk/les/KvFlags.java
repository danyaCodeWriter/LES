package com.acme.coordination.sdk.les;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GLOBAL KV helpers for kill-switch / shard-map keys, including SSE watch over {@code stream/keys}
 * filtered to the kv prefix.
 */
public final class KvFlags {

  private static final Logger log = LoggerFactory.getLogger(KvFlags.class);

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
            .timeout(Duration.ofSeconds(30))
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

  /** Put JSON document (e.g. shard-map) without TTL. */
  public void putJson(String keyId, JsonNode value) throws Exception {
    ObjectNode body = json.createObjectNode();
    body.set("value", value);
    String path = "/v1/tenants/" + tenantId + "/global/kv/" + enc(keyId);
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(client.restBase() + path))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
            .build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("KV putJson failed HTTP " + resp.statusCode() + " " + resp.body());
    }
    JsonNode n = json.readTree(resp.body());
    if (n.hasNonNull("accessToken")) {
      accessTokens.put(keyId, n.get("accessToken").asText());
    }
  }

  public void rememberAccessToken(String keyId, String token) {
    if (token != null && !token.isBlank()) {
      accessTokens.put(keyId, token);
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
    HttpRequest.Builder rb =
        HttpRequest.newBuilder(URI.create(client.restBase() + path)).timeout(Duration.ofSeconds(30)).GET();
    if (token != null) {
      rb.header("Authorization", "Bearer " + token);
    }
    HttpResponse<String> resp = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() == 404 || resp.statusCode() == 401 || resp.statusCode() == 403) {
      return Optional.empty();
    }
    if (resp.statusCode() / 100 != 2) {
      return Optional.empty();
    }
    return Optional.of(json.readTree(resp.body()).path("value"));
  }

  /**
   * Watch GLOBAL KV changes via tenant {@code stream/keys} (SRE or global coordinator token).
   * Invokes {@code onKey} when the SSE payload key path contains {@code /global/kv/{keyId}}.
   */
  public AutoCloseable watch(String keyId, Consumer<KvWatchEvent> onKey) {
    String sre = client.options().sreToken();
    if (sre == null || sre.isBlank()) {
      throw new IllegalStateException("sreToken required for kv().watch — or pass a global coord token via watchWithToken");
    }
    return watchWithToken(keyId, sre, onKey);
  }

  public AutoCloseable watchWithToken(String keyId, String bearerToken, Consumer<KvWatchEvent> onKey) {
    String needle = "/global/kv/" + keyId;
    Thread t =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  HttpRequest req =
                      HttpRequest.newBuilder(
                              URI.create(client.restBase() + "/v1/tenants/" + tenantId + "/stream/keys"))
                          .timeout(Duration.ofMinutes(30))
                          .header("Accept", "text/event-stream")
                          .header("Authorization", "Bearer " + bearerToken)
                          .GET()
                          .build();
                  HttpResponse<java.io.InputStream> resp =
                      http.send(req, HttpResponse.BodyHandlers.ofInputStream());
                  if (resp.statusCode() / 100 != 2) {
                    log.warn("KV watch HTTP {} — retry", resp.statusCode());
                    Thread.sleep(2000);
                    continue;
                  }
                  try (BufferedReader br =
                      new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                    String line;
                    StringBuilder data = new StringBuilder();
                    while (!Thread.currentThread().isInterrupted() && (line = br.readLine()) != null) {
                      if (line.startsWith("data:")) {
                        if (!data.isEmpty()) {
                          data.append('\n');
                        }
                        data.append(line.substring(5).trim());
                      } else if (line.isEmpty() && !data.isEmpty()) {
                        dispatch(needle, keyId, data.toString(), onKey);
                        data.setLength(0);
                      }
                    }
                  }
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  return;
                } catch (Exception e) {
                  log.warn("KV watch reconnect: {}", e.toString());
                  try {
                    Thread.sleep(2000);
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              }
            },
            "les-kv-watch-" + keyId);
    t.setDaemon(true);
    t.start();
    return () -> t.interrupt();
  }

  private void dispatch(String needle, String keyId, String dataJson, Consumer<KvWatchEvent> onKey) {
    try {
      JsonNode n = json.readTree(dataJson);
      String key = n.path("key").asText("");
      if (!key.contains(needle)) {
        return;
      }
      String type = n.path("eventType").asText(n.path("type").asText("UPDATE"));
      JsonNode valueJson = null;
      String raw = n.path("valueJson").asText(null);
      if (raw != null && !raw.isBlank()) {
        try {
          JsonNode ticket = json.readTree(raw);
          valueJson = ticket.has("value") ? ticket.get("value") : ticket;
        } catch (Exception ignored) {
          valueJson = null;
        }
      }
      onKey.accept(new KvWatchEvent(keyId, type, valueJson, key));
    } catch (Exception e) {
      log.debug("skip kv watch frame: {}", e.toString());
    }
  }

  public record KvWatchEvent(String keyId, String eventType, JsonNode value, String etcdKey) {}

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
