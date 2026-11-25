package com.acme.coordination.sdk.les;

import com.acme.coordination.sdk.CoordinationRestClient;
import com.acme.coordination.sdk.session.ApiScope;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Lightweight SSE watcher for workers/jobs streams (OpsWatch). */
public final class OpsWatch implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(OpsWatch.class);

  private final CoordinationRestClient rest;
  private final String tenantId;
  private final String token;
  private final ApiScope scope;
  private final HttpClient http = HttpClient.newHttpClient();
  private Thread thread;
  private volatile boolean running;

  OpsWatch(CoordinationRestClient rest, String tenantId, String token, ApiScope scope) {
    this.rest = rest;
    this.tenantId = tenantId;
    this.token = token;
    this.scope = scope;
  }

  public OpsWatch onWorkers(Consumer<String> rawSseLine) {
    startStream(scope == ApiScope.GLOBAL ? "global/stream/workers" : "stream/workers", rawSseLine);
    return this;
  }

  public OpsWatch onJobs(Consumer<String> rawSseLine) {
    startStream(scope == ApiScope.GLOBAL ? "global/stream/jobs" : "stream/jobs", rawSseLine);
    return this;
  }

  /** Tenant-wide keys (SRE or global coordinator token). */
  public OpsWatch onKeys(Consumer<String> rawSseLine) {
    startStream("stream/keys", rawSseLine);
    return this;
  }

  private void startStream(String suffix, Consumer<String> consumer) {
    running = true;
    thread =
        new Thread(
            () -> {
              try {
                String path = "/v1/tenants/" + tenantId + "/" + suffix;
                HttpRequest req =
                    HttpRequest.newBuilder(URI.create(rest.baseUrl() + path))
                        .header("Accept", "text/event-stream")
                        .header("Authorization", "Bearer " + token)
                        .GET()
                        .build();
                HttpResponse<java.io.InputStream> resp =
                    http.send(req, HttpResponse.BodyHandlers.ofInputStream());
                try (var br =
                    new java.io.BufferedReader(
                        new java.io.InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                  String line;
                  while (running && (line = br.readLine()) != null) {
                    consumer.accept(line);
                  }
                }
              } catch (Exception e) {
                if (running) {
                  log.warn("OpsWatch stream ended: {}", e.toString());
                }
              }
            },
            "les-ops-watch");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void close() {
    running = false;
    if (thread != null) {
      thread.interrupt();
    }
  }
}
