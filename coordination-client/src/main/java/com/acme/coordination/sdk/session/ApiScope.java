package com.acme.coordination.sdk.session;

/** Contour for leases: regional short-path vs global (cross-DC). */
public enum ApiScope {
  /** {@code /v1/tenants/{t}/coordinator|workers|jobs} — only while local LES {@code status=active}. */
  REGIONAL,
  /** {@code /v1/tenants/{t}/global/...} — allowed on stand_in; survives regional switchover. */
  GLOBAL
}
