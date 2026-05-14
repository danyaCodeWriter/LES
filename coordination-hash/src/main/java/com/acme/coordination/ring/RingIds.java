package com.acme.coordination.ring;

import java.math.BigInteger;
import java.util.UUID;

/**
 * Stable string form of business ids for Murmur3-32 ring hashing. Must match what you use everywhere in the fleet.
 */
public final class RingIds {
  private RingIds() {}

  /**
   * Supported types: {@link UUID} (canonical {@link UUID#toString()}), integral boxed types ({@link Long}, {@link
   * Integer}, {@link Short}, {@link Byte}), {@link BigInteger} ({@link BigInteger#toString()}), {@link CharSequence}
   * (as-is). Other {@link Number} subtypes are rejected.
   */
  public static String toKeyString(Object id) {
    if (id == null) {
      throw new IllegalArgumentException("id is null");
    }
    if (id instanceof UUID u) {
      return u.toString();
    }
    if (id instanceof Long l) {
      return Long.toString(l);
    }
    if (id instanceof Integer i) {
      return Integer.toString(i);
    }
    if (id instanceof Short s) {
      return Short.toString(s);
    }
    if (id instanceof Byte b) {
      return Byte.toString(b);
    }
    if (id instanceof BigInteger bi) {
      return bi.toString();
    }
    if (id instanceof Number) {
      throw new IllegalArgumentException("Unsupported Number subtype for ring key: " + id.getClass().getName());
    }
    if (id instanceof CharSequence cs) {
      return cs.toString();
    }
    throw new IllegalArgumentException(
        "Unsupported id type "
            + id.getClass().getName()
            + "; use UUID, integral Number, BigInteger, or CharSequence (or call RingAssignment.workerForKeyString).");
  }
}
