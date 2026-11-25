package com.acme.coordination.sdk.session;

/** Contour for leases: regional short-path vs global (cross-DC). */
public enum ApiScope {
  /**
   * {@code /v1/tenants/{t}/coordinator|workers|jobs} — etcd segment {@code COORDINATION_REGION}.
   * Active DC is a client/RSS concern, not a Les server gate.
   */
  REGIONAL,
  /** {@code /v1/tenants/{t}/global/...} — one owner across all DCs (Active-Active). */
  GLOBAL
}
