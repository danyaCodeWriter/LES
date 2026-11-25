package com.acme.coordination.sdk.les;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-only RSS SSE consumer ({@code GET /v1/recovery-groups/{rg}/active-region}). Never used by
 * Les server.
 */
public final class RssBridge implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RssBridge.class);

  private final HttpClient http;
  private final String rssBaseUrl;
  private final String recoveryGroup;
  private final String homeRegion;
  private final Consumer<ActiveRegionChanged> onChanged;
  private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
  private final AtomicReference<String> activeRegion = new AtomicReference<>("");
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread thread;

  public record ActiveRegionChanged(String previousActive, String newActive, String homeRegion) {}

  RssBridge(
      HttpClient http,
      String rssBaseUrl,
      String recoveryGroup,
      String homeRegion,
      Consumer<ActiveRegionChanged> onChanged) {
    this.http = http;
    this.rssBaseUrl = rssBaseUrl.replaceAll("/$", "");
    this.recoveryGroup = recoveryGroup;
    this.homeRegion = homeRegion;
    this.onChanged = onChanged == null ? ev -> {} : onChanged;
  }

  void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    thread = new Thread(this::loop, "les-rss-" + recoveryGroup);
    thread.setDaemon(true);
    thread.start();
  }

  public boolean amIActive() {
    String a = activeRegion.get();
    return a != null && !a.isBlank() && a.equals(homeRegion);
  }

  public String activeRegion() {
    return activeRegion.get();
  }

  private void loop() {
    while (running.get()) {
      try {
        String url =
            rssBaseUrl
                + "/v1/recovery-groups/"
                + URLEncoder.encode(recoveryGroup, StandardCharsets.UTF_8)
                + "/active-region?region="
                + URLEncoder.encode(homeRegion, StandardCharsets.UTF_8);
        HttpRequest req =
            HttpRequest.newBuilder(URI.create(url))
                .header("Accept", "text/event-stream")
                .GET()
                .build();
        HttpResponse<java.io.InputStream> resp =
            http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() / 100 != 2) {
          log.warn("RSS HTTP {} — retry", resp.statusCode());
          Thread.sleep(2000);
          continue;
        }
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
          String line;
          String event = null;
          StringBuilder data = new StringBuilder();
          while (running.get() && (line = br.readLine()) != null) {
            if (line.startsWith("event:")) {
              event = line.substring(6).trim();
            } else if (line.startsWith("data:")) {
              if (!data.isEmpty()) {
                data.append('\n');
              }
              data.append(line.substring(5).trim());
            } else if (line.isEmpty()) {
              if ("state".equals(event) && !data.isEmpty()) {
                handleState(data.toString());
              }
              event = null;
              data.setLength(0);
            }
          }
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        log.warn("RSS stream error: {}", e.toString());
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void handleState(String dataJson) {
    try {
      JsonNode n = json.readTree(dataJson);
      String next = n.path("activeRegion").asText("");
      String prev = activeRegion.getAndSet(next);
      if (prev != null && !prev.isBlank() && !Objects.equals(prev, next)) {
        onChanged.accept(new ActiveRegionChanged(prev, next, homeRegion));
      }
    } catch (Exception e) {
      log.warn("bad RSS state payload: {}", e.toString());
    }
  }

  @Override
  public void close() {
    running.set(false);
    if (thread != null) {
      thread.interrupt();
    }
  }
}
