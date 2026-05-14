package com.acme.coordination.deployment;

import java.util.regex.Pattern;
import org.springframework.http.HttpMethod;

/** REST paths that acquire/renew/release lease-backed regional state (not global, not read-only). */
public final class RegionalLeaseMutationPaths {

  private static final Pattern REST = Pattern.compile(
      "^/v1/tenants/[^/]+/(coordinator/(acquire|renew)|workers/register|workers/[^/]+/(heartbeat|deregister)|jobs/[^:]+:(acquire|renew|release))$");

  private RegionalLeaseMutationPaths() {}

  public static boolean isRegionalLeaseMutationRest(String normalizedPath, HttpMethod method) {
    if (method == null
        || method == HttpMethod.GET
        || method == HttpMethod.HEAD
        || method == HttpMethod.OPTIONS) {
      return false;
    }
    if (!normalizedPath.startsWith("/v1/tenants/")) {
      return false;
    }
    if (DeploymentRegistrationPolicy.isGlobalTenantPath(normalizedPath)) {
      return false;
    }
    return REST.matcher(normalizedPath).matches();
  }
}
