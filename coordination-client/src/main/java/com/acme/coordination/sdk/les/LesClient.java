package com.acme.coordination.sdk.les;

import com.acme.coordination.sdk.CoordinationRestClient;
import com.acme.coordination.sdk.session.CoordinationSessions;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Product-facing Les entrypoint. Hides renew/idempotency details behind sessions and helpers.
 *
 * <p>RSS is <strong>optional and client-only</strong> ({@link Builder#viaRss}); the Les server never
 * calls RSS. When viaRss is enabled, active-region flips trigger {@link LesTenant#drain(String)}.
 */
public final class LesClient implements AutoCloseable {

  private final CoordinationRestClient rest;
  private final CoordinationSessions sessions;
  private final LesOptions options;
  private final RssBridge rss;

  private LesClient(CoordinationRestClient rest, LesOptions options, RssBridge rss) {
    this.rest = rest;
    this.sessions = rest.sessions();
    this.options = options;
    this.rss = rss;
  }

  public static Builder connect(String lesBaseUrl) {
    return new Builder(lesBaseUrl);
  }

  public CoordinationRestClient rest() {
    return rest;
  }

  /** Base URL without trailing slash (for KV/SSE helpers). */
  public String restBase() {
    return rest.baseUrl();
  }

  public CoordinationSessions sessions() {
    return sessions;
  }

  public LesOptions options() {
    return options;
  }

  public LesTenant tenant(String tenantId) {
    return new LesTenant(this, tenantId);
  }

  /** Optional RSS bridge; null when viaRss was not configured. */
  public RssBridge rssOrNull() {
    return rss;
  }

  @Override
  public void close() {
    if (rss != null) {
      rss.close();
    }
    sessions.close();
    rest.close();
  }

  public static final class Builder {
    private final String lesBaseUrl;
    private String sreToken = "";
    private String homeRegion = "r1";
    private boolean viaRss;
    private String rssBaseUrl;
    private String recoveryGroup;
    private String drainTenantId;
    private boolean autoDrainOnFlip = true;
    private Consumer<RssBridge.ActiveRegionChanged> onActiveChanged;

    Builder(String lesBaseUrl) {
      this.lesBaseUrl = Objects.requireNonNull(lesBaseUrl);
    }

    /** SRE / drain / monitoring token ({@code coordination.sre.stream-listen-token}). */
    public Builder sreToken(String sreToken) {
      this.sreToken = sreToken == null ? "" : sreToken;
      return this;
    }

    public Builder homeRegion(String homeRegion) {
      this.homeRegion = homeRegion == null || homeRegion.isBlank() ? "r1" : homeRegion.trim();
      return this;
    }

    /**
     * Enable RSS client integration (default off). Pass recovery group; SDK streams active region
     * and can drain previous regional namespace on flip.
     */
    public Builder viaRss(String rssBaseUrl, String recoveryGroup) {
      this.viaRss = true;
      this.rssBaseUrl = Objects.requireNonNull(rssBaseUrl);
      this.recoveryGroup = Objects.requireNonNull(recoveryGroup);
      return this;
    }

    public Builder onActiveRegionChanged(Consumer<RssBridge.ActiveRegionChanged> hook) {
      this.onActiveChanged = hook;
      return this;
    }

    /** Tenant used for auto-drain on RSS flip (requires {@link #sreToken}). Default on when set. */
    public Builder drainTenant(String tenantId) {
      this.drainTenantId = tenantId;
      return this;
    }

    public Builder autoDrainOnFlip(boolean enabled) {
      this.autoDrainOnFlip = enabled;
      return this;
    }

    public LesClient build() {
      CoordinationRestClient rest = new CoordinationRestClient(lesBaseUrl);
      LesOptions opts = new LesOptions(sreToken, homeRegion, Duration.ofSeconds(30));
      LesClient[] holder = new LesClient[1];
      String drainTenant = drainTenantId;
      boolean doDrain = autoDrainOnFlip;
      Consumer<RssBridge.ActiveRegionChanged> hook =
          ev -> {
            if (onActiveChanged != null) {
              onActiveChanged.accept(ev);
            }
            if (doDrain
                && holder[0] != null
                && drainTenant != null
                && !drainTenant.isBlank()
                && opts.sreToken() != null
                && !opts.sreToken().isBlank()
                && ev.previousActive() != null
                && !ev.previousActive().isBlank()) {
              try {
                holder[0].tenant(drainTenant).drain(ev.previousActive());
              } catch (Exception e) {
                // logged by caller metrics; avoid killing RSS thread
              }
            }
          };
      RssBridge bridge = null;
      if (viaRss) {
        bridge =
            new RssBridge(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                rssBaseUrl,
                recoveryGroup,
                homeRegion,
                hook);
        bridge.start();
      }
      holder[0] = new LesClient(rest, opts, bridge);
      return holder[0];
    }
  }
}
