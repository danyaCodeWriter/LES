package com.acme.coordination.sdk.les;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Platform monitoring SSE using SRE API key ({@code stream/keys}). */
public final class MonitoringSse implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MonitoringSse.class);

  private final String baseUrl;
  private final String apiKey;
  private final String tenantId;
  private volatile boolean running;
  private Thread thread;

  public static MonitoringSse open(String lesBaseUrl, String sreApiKey, String tenantId) {
    return new MonitoringSse(lesBaseUrl.replaceAll("/$", ""), sreApiKey, tenantId);
  }

  private MonitoringSse(String baseUrl, String apiKey, String tenantId) {
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
    this.tenantId = tenantId;
  }

  public MonitoringSse onEtcdActivity(Consumer<String> rawLine) {
    running = true;
    thread =
        new Thread(
            () -> {
              try {
                HttpRequest req =
                    HttpRequest.newBuilder(
                            URI.create(baseUrl + "/v1/tenants/" + tenantId + "/stream/keys"))
                        .header("Accept", "text/event-stream")
                        .header("Authorization", "Bearer " + apiKey)
                        .GET()
                        .build();
                HttpResponse<java.io.InputStream> resp =
                    HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofInputStream());
                try (var br =
                    new java.io.BufferedReader(
                        new java.io.InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                  String line;
                  while (running && (line = br.readLine()) != null) {
                    rawLine.accept(line);
                  }
                }
              } catch (Exception e) {
                if (running) {
                  log.warn("MonitoringSse ended: {}", e.toString());
                }
              }
            },
            "les-monitoring-sse");
    thread.setDaemon(true);
    thread.start();
    return this;
  }

  @Override
  public void close() {
    running = false;
    if (thread != null) {
      thread.interrupt();
    }
  }
}
